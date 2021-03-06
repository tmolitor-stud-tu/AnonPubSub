import uuid
import numpy
import logging
logger = logging.getLogger(__name__)

# own classes
from networking import Message
from .router import Router
from .cover_traffic_mixin import CoverTrafficMixin


class Randomwalk(Router, CoverTrafficMixin):
    settings = {
        "INITIAL_TTL": 60,
        "INITIAL_WALKERS": 5,
    }
    
    def __init__(self, node_id, queue):
        super(Randomwalk, self).__init__(node_id, queue)
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
        # use at most INITIAL_WALKERS randomly selected peers to send our message to if we are the origin
        # or only 1 peer to pass the message to if we are not the origin
        connections = list(numpy.random.choice(
            connections,
            size=min(len(connections), Randomwalk.settings["INITIAL_WALKERS"] if not incoming_connection else 1)
        ))
        for node_id in connections:
            self._send_msg(msg, self.connections[node_id])
    
    def _publish_command(self, command):
        # call parent class for common tasks (update self.publishing)
        super(Randomwalk, self)._publish_command(command)
        msg = Message("%s_data" % self.__class__.__name__, {
            "channel": command["channel"],
            "data": command["data"],
            "id": str(uuid.uuid4()),
            "ttl": Randomwalk.settings["INITIAL_TTL"],
            "nodes": []
        })
        self._route_data(msg)
