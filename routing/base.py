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


OUTGOING_TIME = 1.0
TIMING_FACTOR = 2.0

class Router(object):
    stopped = Event()
    
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
    
    def publish(self, channel, data):
        logger.info("Publishing data on channel '%s'..." % str(channel))
        self.queue.put({
            "command": "publish",
            "channel": channel,
            "data": data
        })
    
    def subscribe(self, channel, callback):
        logger.info("Subscribing for data on channel '%s'..." % str(channel))
        self.queue.put({
            "command": "subscribe",
            "channel": channel,
            "callback": callback
        })
    
    def unsubscribe(self, channel):
        # TODO: das hier fehlt noch!
        pass
    
    # *** internal api methods for child classes ***
    # _send_msg() and _send_covert_msg() are used by child classes for rate limited sending of messages (for routing demonstrating purposes)
    def _send_msg(self, msg, con):
        filters.msg_outgoing(msg, con)      # call filters framework
        self.__outgoing("normal", msg, con)
    
    def _send_covert_msg(self, msg, con):
        filters.covert_msg_outgoing(msg, con)       # call filters framework
        self.__outgoing("covert", msg, con)
    
    def _add_timer(self, timeout, command):
        timeout *= TIMING_FACTOR    # delay timer for demonstrating purposes
        timer_id = str(uuid.uuid4())
        with self.timers_condition:
            self.timers.add({"timeout": datetime.now().timestamp() + timeout, "id": timer_id, "command": command})
            self.timers_condition.notify()      # notify timers thread of changes
        return timer_id
    
    def _abort_timer(self, timer_id):
        with self.timers_condition:
            # filter out this timer id
            self.timers = SortedList(iterable=[entry for entry in self.timers if entry["id"] != timer_id], key=itemgetter('timeout'))
            self.timers_condition.notify()      # notify timers thread of changes
    
    def _call_command(self, command):
        func = "_%s_command" % command["command"]       # this is the method name we have to call on self to process the command
        if hasattr(self, func):
            return getattr(self, func)(command)
        logger.error("Unknown routing command '%s' (%s), ignoring command!" % (command["command"], func))
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
        if command["message"].get_type().startswith(self.__class__.__name__):   #ignore messages from other routers
            self._route_covert_data(command["message"], command["connection"])
    
    def _message_received_command(self, command):
        if command["message"].get_type().startswith(self.__class__.__name__):   #ignore messages from other routers
            self._route_data(command["message"], command["connection"])
    
    def _subscribe_command(self, command):
        if command["channel"] in self.subscriptions:
            return
        self.subscriptions[command["channel"]] = command["callback"]
    
    def _publish_command(self, command):
        pass    # this command has no common implementation that could be used by child classes
    
    # *** internal methods, DON'T touch from child classes ***
    def __outgoing(self, msg_type, msg, con):
        # no locking required in this method since we are only called from _routing thread
        
        # use "Router__real_send" as reference to our __real_send_command() because of name mangling semantics of double underscore
        # (those methods cannot accidentally be overwritten from child)
        command = {"command": "Router__real_send", "type": msg_type, "message": msg, "connection": con}
        
        # calculate next send time and update timestamps
        peer = con.get_peer_id()
        if peer not in self.last_outgoing_time:
            self.last_outgoing_time[peer] = {"normal": datetime.now().timestamp(), "covert": datetime.now().timestamp()}
        # always wait a minimum of OUTGOING_TIME seconds for every message
        timestamp = max(self.last_outgoing_time[peer][msg_type] + OUTGOING_TIME, datetime.now().timestamp() + OUTGOING_TIME)
        self.last_outgoing_time[peer][msg_type] = timestamp     # update last outgoing timestamp
        
        # add send timer (don't use _add_timer() directly because this method multiplies the timeout with TIMING_FACTOR, see the comments there)
        with self.timers_condition:
            self.timers.add({"timeout": timestamp, "id": str(uuid.uuid4()), "command": command})
            self.timers_condition.notify()      # notify timers thread of changes
    
    def __real_send_command(self, command):
        try:
            if command["type"] == "normal":
                command["connection"].send_msg(command["message"])
            elif command["type"] == "covert":
                command["connection"].send_covert_msg(command["message"])
            else:
                raise ValueError("Message type '%s' unknown!" % command["type"])
        except Exception as e:
            logger.warning("Failed to really send covert message to peer %s: %s" % (str(command["connection"].get_peer_id()), str(e)))
    
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
                command = self.queue.get(1)     #1 second timeout
                if Router.stopped.isSet():      # don't process anything here if we are stopped
                    break
            except Empty as err:
                logger.debug("routing queue empty")
                continue
            logger.debug("got routing command: %s" % command["command"])
            self._call_command(command)
            self.queue.task_done()
        logger.debug("routing thread got stop signal, terminating all connections...")
        for peer_id, con in self.connections.items():
            con.terminate()
        logger.debug("routing thread stopped...")
