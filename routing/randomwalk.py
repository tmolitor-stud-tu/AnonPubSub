from queue import Queue
import numpy
import logging
logger = logging.getLogger(__name__)

from networking import Connection, Message
from .base import Router


INITIAL_TTL = 60
INITIAL_WALKERS = 5

class Randomwalk(Router):
    
    def __init__(self, node_id, queue):
        super(Randomwalk, self).__init__(node_id, queue)
        self.subscriptions = {}
        logger.info("Flooding Router initialized...")
    
    def publish(self, channel, data):
        logger.info("Publishing data on channel '%s'..." % str(channel))
        self._route_data(Message("data", {"channel": channel, "data": data, "ttl": INITIAL_TTL, "nodes": []}))
    
    def subscribe(self, channel, callback):
        if channel in self.subscriptions:
            return
        logger.info("Subscribing for data on channel '%s'..." % str(channel))
        self.subscriptions[channel] = callback
    
    def _route_data(self, msg, incoming_connection=None):
        if msg["ttl"] <= 0:
            logger.warning("Ignoring data because of expired ttl!")
            return
        
        if msg["channel"] in self.subscriptions:
            self.subscriptions[msg["channel"]](msg["data"])        #inform own subscriber of new data
        
        msg["ttl"] -= 1
        msg["nodes"].append(self.node_id)
        connections = [key for key in self.connections if key not in msg["nodes"]]    #this does avoid loops
        if not len(connections):
            logger.warning("No additional peers found, cannot route data further!")
            return
        #use at most INITIAL_WALKERS randomly selected peers to send our message to
        connections = list(numpy.random.choice(connections, p=weights, size=min(len(connections), INITIAL_WALKERS)))
        for node_id in connections:
            self.connections[node_id].send_msg(msg)

    def _process_command(self, command):
        if command["command"] == "add_connection":
            con = command["connection"]
            peer = con.get_peer_id()
            self.connections[peer] = con
        elif command["command"] == "remove_connection":
            con = command["connection"]
            peer = con.get_peer_id()
            del self.connections[peer]
        elif command["command"] == "message_received":
            self._route_data(command["message"], command["connection"])
        else:
            logger.error("Unknown routing command '%s', ignoring command!" % command["command"])
