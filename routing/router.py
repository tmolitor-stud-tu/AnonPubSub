import uuid
from datetime import datetime
from operator import itemgetter
from sortedcontainers import SortedList
from queue import Queue, Empty, Full
from threading import Thread, Event, RLock, Condition
import logging
logger = logging.getLogger(__name__)

# own classes
import filters
from networking import Message


class Router(object):
    stopped = Event()
    settings = {
        "OUTGOING_TIME": 1.0,       # time to wait until a message is really sent out
        "TIMING_FACTOR": 2.0,       # factor to scale timing values by
    }
    
    # *** public interface ***
    def __init__(self, node_id, queue):
        self.queue = queue
        self.node_id = node_id
        self.connections = {}
        self.subscriptions = {}
        self.last_outgoing_time = {}
        self.timers = SortedList(key=itemgetter('timeout'))
        self.timers_condition = Condition()
        self.timers_thread = Thread(name="local::"+self.node_id+"::_timers", target=self._timers)
        self.timers_thread.start()
        self.routing_thread = Thread(name="local::"+self.node_id+"::_routing", target=self._routing)
        self.routing_thread.start()
    
    def stop(self):
        Router.stopped.set()    # this will stop the _routing thread which terminates all connections (no lock on self.connections needed)
        self.queue.put({})      # empty put to wake up _routing thread after Router.stopped is set to True
        with self.timers_condition:
            self.timers_condition.notify()  # stop wait in _timers thread
        self.routing_thread.join()
        self.timers_thread.join()
        Router.stopped.clear()  # all threads terminated, clear flag again to allow for new initialisations
    
    def subscribe(self, channel, callback):
        logger.info("Subscribing for data on channel '%s'..." % str(channel))
        self.queue.put({
            "_command": "subscribe",
            "channel": channel,
            "callback": callback
        })
    
    def unsubscribe(self, channel):
        logger.info("Unsubscribing channel '%s'..." % str(channel))
        self.queue.put({
            "_command": "unsubscribe",
            "channel": channel,
        })
    
    def publish(self, channel, data):
        logger.debug("Publishing data on channel '%s'..." % str(channel))
        self.queue.put({
            "_command": "publish",
            "channel": channel,
            "data": data
        })
    
    def unpublish(self, channel):
        logger.debug("Unpublishing channel '%s'..." % str(channel))
        self.queue.put({
            "_command": "unpublish",
            "channel": channel
        })
    
    # this dumps internal data structures of the child class to logger if implemented
    def dump(self, callback=None):
        self.queue.put({
            "_command": "dump",
            "callback": callback
        })
    
    # *** internal api methods for child classes ***
    # _send_msg() and _send_covert_msg() are used by child classes for rate limited sending of messages (for routing demonstration purposes)
    def _send_msg(self, msg, con):
        if not filters.msg_outgoing(msg, con):              # call filters framework
            self.__outgoing("data", msg, con)
    
    def _send_covert_msg(self, msg, con):
        if not filters.covert_msg_outgoing(msg, con):       # call filters framework
            self.__outgoing("covert_data", msg, con)
    
    def _add_timer(self, timeout, command):
        timeout *= Router.settings["TIMING_FACTOR"]         # delay timer for demonstrating purposes
        timer_id = str(uuid.uuid4())
        with self.timers_condition:
            self.timers.add({"timeout": datetime.now().timestamp() + timeout, "id": timer_id, "command": command})
            self.timers_condition.notify()                  # notify timers thread of changes
        return timer_id
    
    def _abort_timer(self, timer_id):
        with self.timers_condition:
            # filter out this timer id
            self.timers = SortedList(iterable=[entry for entry in self.timers if entry["id"] != timer_id], key=itemgetter('timeout'))
            self.timers_condition.notify()      # notify timers thread of changes
    
    def _call_command(self, command):
        # determine the method name we have to call on self to process the command
        func = "_%s_command" % str(command["_command"])
        if hasattr(self, func):
            return getattr(self, func)(command)
        logger.error("Unknown routing command '%s' (%s), ignoring command: %s" % (str(command["_command"]), func, str(command)))
        return None
    
    # *** routing methods that should be overwritten by child classes ***
    def _route_covert_data(self, msg, incoming_connection=None):
        pass
    
    def _route_data(self, msg, incoming_connection=None):
        pass
    
    # *** command methods that can be overwritten or used as is by child classes ***
    def _add_connection_command(self, command):
        con = command["connection"]
        peer = con.get_peer_id()
        self.connections[peer] = con
    
    def _remove_connection_command(self, command):
        con = command["connection"]
        peer = con.get_peer_id()
        if peer in self.connections:
            del self.connections[peer]
        if peer in self.last_outgoing_time:
            del self.last_outgoing_time[peer]
    
    def _covert_message_received_command(self, command):
        # covert messages and uncovert messages are handled the same (only one part of the called method name differs)
        return self.__route("covert_data", command["message"], command["connection"])
    
    def _message_received_command(self, command):
        # covert messages and uncovert messages are handled the same (only one part of the called method name differs)
        return self.__route("data", command["message"], command["connection"])
    
    def _subscribe_command(self, command):
        self.subscriptions[command["channel"]] = command["callback"]
    
    def _unsubscribe_command(self, command):
        if command["channel"] in self.subscriptions:
            del self.subscriptions[command["channel"]]
    
    def _publish_command(self, command):
        pass    # this command has no common implementation that could be used by child classes
    
    def _unpublish_command(self, command):
        pass    # this command has no common implementation that could be used by child classes
    
    def _dump_command(self, command):
        # this command has no common implementation that could be used by child classes but is not mandatory to implement
        logger.error("This router does not support dumping of its internal state!")
    
    # *** internal methods, DON'T touch from child classes ***
    def __route(self, message_category, message, connection):
        # ignore messages from unknown namespaces
        func = "_route_%s" % message_category        # main class namespace
        if message.get_type().startswith(self.__class__.__name__) and hasattr(self, func):
            getattr(self, func)(message, connection)
        else:
            # iterate over all mixins and try to find mixin responsible for routing this message
            for mixin in set(base.__name__ for base in self.__class__.__bases__ if base.__name__.endswith("Mixin")):
                func = "_%s__route_%s" % (mixin, message_category)   # mixin namespace
                if message.get_type().startswith(mixin) and hasattr(self, func):
                    getattr(self, func)(message, connection)
    
    def __outgoing(self, msg_type, msg, con):
        # no locking required in this method since we are only called from _routing thread
        
        # use "Router__real_send" as reference to our __real_send_command() because of name mangling semantics of double underscore
        # (those methods cannot accidentally be overwritten from child)
        # clone message object to decouple it from changes the caller does after "sending it out"
        command = {"_command": "Router__real_send", "type": msg_type, "message": Message(msg), "connection": con}
        
        if not Router.settings["OUTGOING_TIME"]:        # no delay wanted at all --> call __real_send directly
            self._call_command(command)
            return
        
        # calculate next send time and update timestamps
        peer = con.get_peer_id()
        if peer not in self.last_outgoing_time:
            self.last_outgoing_time[peer] = {"data": datetime.now().timestamp(), "covert_data": datetime.now().timestamp()}
        
        # *** this will send out only one message per OUTGOING_TIME
        # always wait a minimum of OUTGOING_TIME seconds for every message
        #timestamp = max(
            #self.last_outgoing_time[peer][msg_type] + Router.settings["OUTGOING_TIME"],
            #datetime.now().timestamp() + Router.settings["OUTGOING_TIME"]
        #)
        
        # *** this will delay every message for OUTGOING_TIME seconds (but send out more than one message per OUTGOING_TIME if requested)
        # delay every message by OUTGOING_TIME seconds
        timestamp = datetime.now().timestamp() + Router.settings["OUTGOING_TIME"]
        
        self.last_outgoing_time[peer][msg_type] = timestamp     # update last outgoing timestamp
        
        # add send timer
        # (don't use _add_timer() directly because that method multiplies the timeout with TIMING_FACTOR, see the comments there)
        with self.timers_condition:
            self.timers.add({"timeout": timestamp, "id": str(uuid.uuid4()), "command": command})
            self.timers_condition.notify()      # notify timers thread of changes
    
    def __real_send_command(self, command):
        try:
            if command["type"] == "data":
                command["connection"].send_msg(command["message"])
            elif command["type"] == "covert_data":
                command["connection"].send_covert_msg(command["message"])
            else:
                raise ValueError("Message type '%s' unknown!" % command["type"])
        except Exception as e:
            logger.warning("Failed to really send %s message to peer %s: %s" % (str(command["type"]), str(command["connection"].get_peer_id()), str(e)))
    
    # *** internal threads ***
    def _timers(self):
        logger.debug("timers thread started...");
        while not Router.stopped.isSet():
            # wait for nex timer or for 60 seconds if no timer is currently present
            with self.timers_condition:
                if len(self.timers):
                    logger.debug("next timer event in %s seconds" % str(self.timers[0]["timeout"] - datetime.now().timestamp()))
                    self.timers_condition.wait(self.timers[0]["timeout"] - datetime.now().timestamp())
                else:
                    #logger.debug("next timer event in %s seconds" % str(60))
                    self.timers_condition.wait(60)   # this gets aborted as soon as a timer is added to the list
            # stop here if the router is stopped
            if Router.stopped.isSet():
                break
            # execute all now expired timers
            expired = []
            now = datetime.now().timestamp()
            with self.timers_condition:
                for timer in self.timers:
                    if timer["timeout"] <= now:
                        logger.debug("timer %s adding command %s to queue" % (str(timer["id"]), str(timer["command"])))
                        self.queue.put(timer["command"])
                        expired.append(timer["id"])
                    else:
                        break       # we are sorted, so stop here
                # clean up fired timers afterwards (that can be less than are expired by NOW!)
                self.timers = SortedList(iterable=[entry for entry in self.timers if entry["id"] not in expired], key=itemgetter('timeout'))
        logger.debug("timers thread stopped...")
    
    def _routing(self):
        logger.debug("routing thread started...");
        while not Router.stopped.isSet():
            try:
                command = self.queue.get(True, 60)      # 60 seconds timeout
                if Router.stopped.isSet():              # don't process anything here if we are stopped
                    break
            except Empty as err:
                logger.debug("routing queue empty")
                continue
            logger.debug("got routing command: %s" % command["_command"])
            self._call_command(command)
            self.queue.task_done()
        logger.debug("routing thread got stop signal, terminating all connections...")
        for peer_id, con in self.connections.items():
            con.terminate()
        logger.debug("routing thread stopped...")
