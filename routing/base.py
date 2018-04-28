import uuid
from datetime import datetime
from operator import itemgetter
from sortedcontainers import SortedList
from queue import Queue, Empty, Full
from threading import Thread, Event, RLock, Condition
import logging
logger = logging.getLogger(__name__)

from networking import Connection, Message


class Router(object):
    stopped = Event()
    
    # public interface
    def __init__(self, node_id, queue):
        self.queue = queue
        self.node_id = node_id
        self.connections = {}
        self.subscriptions = {}
        self.timers = SortedList(key=itemgetter('timeout'))
        self.timers_condition = Condition()
        self.routing_thread = Thread(name="local::"+self.node_id+"::_routing", target=self._routing)
        self.routing_thread.start()
        self.timers_thread = Thread(name="local::"+self.node_id+"::_timers", target=self._timers)
        self.timers_thread.start()
    
    def stop(self):
        for peer_id, con in self.connections.items():
            con.terminate()
        Router.stopped.set()
        with self.timers_condition:
            self.timers_condition.notify()  # stop wait in timers thread
        self.queue.put({})      #empty put to wake up routing thread after Router.stopped is set to True
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
    
    # internal methods
    def _route_data(self, msg, incoming_connection=None):
        pass
    
    def _route_covert_data(self, msg, incoming_connection=None):
        pass
    
    def _add_connection_command(self, command):
        con = command["connection"]
        peer = con.get_peer_id()
        self.connections[peer] = con
    
    def _remove_connection_command(self, command):
        con = command["connection"]
        peer = con.get_peer_id()
        del self.connections[peer]
    
    def _message_received_command(self, command):
        if command["message"].get_type() == "%s_data" % self.__class__.__name__:      #ignore messages from other routers
            self._route_data(command["message"], command["connection"])
    
    def _covert_message_received_command(self, command):
        if command["message"].get_type() == "%s_data" % self.__class__.__name__:      #ignore messages from other routers
            self._route_covert_data(command["message"], command["connection"])
    
    def _subscribe_command(self, command):
        if command["channel"] in self.subscriptions:
            return
        self.subscriptions[command["channel"]] = command["callback"]
    
    def _publish_command(self, command):
        pass    # this command has no common implementation that could be used by child classes
    
    def _add_timer(self, timeout, command):
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
    
    def _timers(self):
        logger.debug("timer thread started...");
        while not Router.stopped.isSet():
            # wait for nex timer or for 8 seconds if no timer is currently present
            with self.timers_condition:
                if len(self.timers):
                    logger.debug("next timer event in %s seconds" % str(self.timers[0]["timeout"] - datetime.now().timestamp()))
                    self.timers_condition.wait(self.timers[0]["timeout"] - datetime.now().timestamp())
                else:
                    logger.debug("next timer event in %s seconds" % str(60))
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
        logger.debug("timer thread stopped...")
    
    def _routing(self):
        logger.debug("routing thread started...");
        while not Router.stopped.isSet():
            try:
                command = self.queue.get(1)     #1 second timeout
                if Router.stopped.isSet():      # don't process anything here if we are stopped
                    break
            except Empty as err:
                logger.debug("routing queue empty");
                continue
            logger.debug("got routing command: %s" % command["command"])
            func = "_"+command["command"]+"_command"        # this is the method name we have to call on self to process the command
            if hasattr(self, func):
                getattr(self, func)(command)
            else:
                logger.error("Unknown routing command '%s', ignoring command!" % command["command"])
            self.queue.task_done()
        logger.debug("routing thread stopped...")
    