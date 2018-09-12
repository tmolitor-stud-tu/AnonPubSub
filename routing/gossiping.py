import uuid
import numpy
import logging
logger = logging.getLogger(__name__)

# own classes
from networking import Message
from .router import Router
from .cover_traffic_mixin import CoverTrafficMixin


class Gossiping(Router, CoverTrafficMixin):
    settings = {
        "INITIAL_TTL": 60,
        "GOSSIPING_PEERS": 2,
    }
    
    def __init__(self, node_id, queue):
        super(Gossiping, self).__init__(node_id, queue)
        logger.info("%s router initialized..." % self.__class__.__name__)
    
    def _route_data(self, msg, incoming_connection=None):
        if msg["ttl"] <= 0:
            logger.warning("Ignoring data because of expired ttl!")
            return
        
        if self._forward_data(msg):     # inform own subscribers of new data and determine if data is fresh
            logger.info("Data ID already seen, ignoring it and not routing further...")
            return
        
        msg["ttl"] -= 1
        msg["nodes"].append(self.node_id)
        connections = [key for key in self.connections if key not in msg["nodes"]]    # this does avoid loops
        if not len(connections):
            logger.warning("No additional peers found, cannot route data further!")
            return
        # use at most GOSSIPING_PEERS randomly selected peers to send our message to
        connections = list(numpy.random.choice(
            connections,
            size=min(len(connections), Gossiping.settings["GOSSIPING_PEERS"])
        ))
        for node_id in connections:
            self._send_msg(msg, self.connections[node_id])
    
    def _publish_command(self, command):
        # call parent class for common tasks (update self.publishing)
        super(Gossiping, self)._publish_command(command)
        msg = Message("%s_data" % self.__class__.__name__, {
            "channel": command["channel"],
            "data": command["data"],
            "id": str(uuid.uuid4()),
            "ttl": Gossiping.settings["INITIAL_TTL"],
            "nodes": []
        })
        self._route_data(msg)
