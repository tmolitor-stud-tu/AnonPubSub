from queue import Queue
import numpy
import logging
logger = logging.getLogger(__name__)

from networking import Connection, Message
from .base import Router


INITIAL_TTL = 60
GOSSIPING_PEERS = 2

class Gossiping(Router):
    
    def __init__(self, node_id, queue):
        super(Gossiping, self).__init__(node_id, queue)
        self.publish_ttl = INITIAL_TTL
        logger.info("%s Router initialized..." % self.__class__.__name__)
    
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
        #use at most GOSSIPING_PEERS randomly selected peers to send our message to
        connections = list(numpy.random.choice(
            connections,
            size=min(len(connections), GOSSIPING_PEERS)
        ))
        for node_id in connections:
            self.connections[node_id].send_msg(msg)