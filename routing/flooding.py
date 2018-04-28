from queue import Queue
import logging
logger = logging.getLogger(__name__)

from networking import Connection, Message
from .base import Router


INITIAL_TTL = 60

class Flooding(Router):
    
    def __init__(self, node_id, queue):
        super(Flooding, self).__init__(node_id, queue)
        logger.info("%s Router initialized..." % self.__class__.__name__)
    
    def _route_data(self, msg, incoming_connection=None):
        if msg["ttl"] <= 0:
            logger.warning("Ignoring data because of expired ttl!")
            return
        
        if msg["channel"] in self.subscriptions:
            self.subscriptions[msg["channel"]](msg["data"])        #inform own subscriber of new data
        
        msg["ttl"] -= 1
        msg["nodes"].append(self.node_id)
        connections = {key: value for key, value in self.connections.items() if key not in msg["nodes"]}    #this does avoid loops
        if not len(connections):
            logger.warning("No additional peers found, cannot route data further!")
            return
        for node_id in connections:
            connections[node_id].send_msg(msg)
    
    def _publish_command(self, command):
        msg = Message("%s_data" % self.__class__.__name__, {
            "channel": command["channel"],
            "data": command["data"],
            "ttl": INITIAL_TTL,
            "nodes": []
        })
        self._route_data(msg)
