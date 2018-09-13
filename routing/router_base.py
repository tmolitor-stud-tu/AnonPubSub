import uuid
from datetime import datetime
from operator import itemgetter
from sortedcontainers import SortedList
from queue import Queue, Empty, Full
from threading import Thread, Event, RLock, Condition
import logging
logger = logging.getLogger(__name__)

# own classes
from utils import catch_exceptions
from networking import Message


class RouterBase(object):
    stopped = Event()
    
    # *** public interface ***
    def __init__(self, node_id, queue):
        self.queue = queue
        self.node_id = node_id
        self.connections = {}
        self.timers = SortedList(key=itemgetter('timeout'))
        self.timers_condition = Condition()
        self.timers_thread = Thread(name="local::"+self.node_id+"::_timers", target=self._timers, daemon=True)
        self.timers_thread.start()
        self.routing_thread = Thread(name="local::"+self.node_id+"::_routing", target=self._routing, daemon=True)
        self.routing_thread.start()
    
    def stop(self):
        RouterBase.stopped.set()    # this will stop the _routing thread which terminates all connections (no lock on self.connections needed)
        self.queue.put({})          # empty put to wake up _routing thread after RouterBase.stopped is set to True
        with self.timers_condition:
            self.timers_condition.notify()  # stop wait in _timers thread
        self.routing_thread.join(30.0)
        self.timers_thread.join(30.0)
        RouterBase.stopped.clear()  # all threads terminated, clear flag again to allow for new initialisations
    
    # this dumps internal data structures
    def dump(self, callback=None):
        self.queue.put({
            "_command": "dump",
            "callback": callback
        })
    
    # *** internal api methods for child classes ***    
    def _add_timer(self, timeout, command):
        return self.__real_add_timer(timeout, command)
    
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
        if peer in list(self.connections.keys()):
            del self.connections[peer]
    
    # *** special command methods that should NOT be overwritten by child classes ***
    def _covert_message_received_command(self, command):
        # covert messages and uncovert messages are handled the same (only one part of the called method name differs)
        if not filters.covert_msg_incoming(command["message"], command["connection"]):      # call filters framework
            return self.__route("covert_data", command["message"], command["connection"])
    
    def _message_received_command(self, command):
        # covert messages and uncovert messages are handled the same (only one part of the called method name differs)
        if not filters.msg_incoming(command["message"], command["connection"]):             # call filters framework
            return self.__route("data", command["message"], command["connection"])
    
    # *** internal methods, DON'T touch from child classes ***
    def __real_add_timer(self, timeout, command):
        timer_id = str(uuid.uuid4())
        with self.timers_condition:
            self.timers.add({"timeout": datetime.now().timestamp() + timeout, "id": timer_id, "command": command})
            self.timers_condition.notify()                  # notify timers thread of changes
        return timer_id
    
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
    
    # *** internal threads ***
    @catch_exceptions(logger=logger)
    def _timers(self):
        logger.debug("timers thread started...");
        while not RouterBase.stopped.isSet():
            # wait for next timer or for 60 seconds if no timer is currently present
            with self.timers_condition:
                if len(self.timers):
                    logger.debug("next timer event in %s seconds" % str(self.timers[0]["timeout"] - datetime.now().timestamp()))
                    self.timers_condition.wait(self.timers[0]["timeout"] - datetime.now().timestamp())
                else:
                    #logger.debug("next timer event in %s seconds" % str(60))
                    self.timers_condition.wait(60)   # this gets aborted as soon as a timer is added to the list
            # stop here if the router is stopped
            if RouterBase.stopped.isSet():
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
    
    @catch_exceptions(logger=logger)
    def _routing(self):
        logger.debug("routing thread started...");
        while not RouterBase.stopped.isSet():
            try:
                command = self.queue.get(True, 60)      # 60 seconds timeout
                if RouterBase.stopped.isSet():              # don't process anything here if we are stopped
                    break
            except Empty as err:
                logger.debug("routing queue empty")
                continue
            logger.debug("got routing command: %s" % command["_command"])
            self._call_command(command)
            self.queue.task_done()
        logger.debug("routing thread stopped...")
