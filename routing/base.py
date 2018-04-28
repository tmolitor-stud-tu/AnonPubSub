from queue import Queue, Empty, Full
from threading import Thread
from threading import Event
import logging
logger = logging.getLogger(__name__)

from networking import Connection, Message


class Router(object):
    stopped = Event()
    
    def __init__(self, node_id, queue):
        self.queue = queue
        self.node_id = node_id
        self.connections = {}
        self.subscriptions = {}
        self.publish_ttl = 0
        self.routing_thread = Thread(name="local::"+self.node_id+"::_routing", target=self._routing)
        self.routing_thread.start()
    
    def stop(self):
        for peer_id, con in self.connections.items():
            con.terminate()
        Router.stopped.set()
        self.queue.put({})      #empty put to wake up routing thread after Router.stopped is set to True
        self.routing_thread.join()
    
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
    
    def _routing(self):
        logger.debug("routing thread started...");
        while not Router.stopped.isSet():
            try:
                command = self.queue.get(1)      #1 second timeout
                if Router.stopped.isSet():
                    break
            except Empty as err:
                logger.debug("routing queue empty");
                continue
            if Router.stopped.isSet():  # don't process anything here if we are stopped
                break
            logger.debug("got routing command: %s" % command["command"])
            func = "_"+command["command"]+"_command"
            if hasattr(self, func):
                getattr(self, func)(command)
            else:
                logger.error("Unknown routing command '%s', ignoring command!" % command["command"])
            self.queue.task_done()
        logger.debug("routing thread stopped...")
    