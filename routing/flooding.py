import os
import base64
import logging
logger = logging.getLogger(__name__)

#crypto imports
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes

# own classes
from networking import Message
from .base import Router


MAX_HASHES = 1000
REFLOOD_TIME = 5.0
SUBSCRIBE_DELAY = 2.0

class Flooding(Router):
    
    def __init__(self, node_id, queue):
        super(Flooding, self).__init__(node_id, queue)
        self.publishing = set()
        self.advertisement_routing_table = {}
        self.data_routing_table = {}
        logger.info("%s router initialized..." % self.__class__.__name__)
    
    def _init_channel(self, channel):
        if not channel in self.advertisement_routing_table:
            self.advertisement_routing_table[channel] = {}
        if not channel in self.data_routing_table:
            self.data_routing_table[channel] = {}
    
    def _hash(self, nonce):
        digest = hashes.Hash(hashes.BLAKE2s(32), backend=default_backend())
        digest.update(nonce)
        return digest.finalize()
    
    # this returns -1, 0 or 1 like compare functions used in sorting do
    def _find_shorter_hashchain(self, nonce1, nonce2):
        if nonce1 == nonce2:
            return 0       # both paths are of equal length
        
        update1 = nonce1
        update2 = nonce2
        for i in range(1, MAX_HASHES + 1):
            update1 = self._hash(update1)
            update2 = self._hash(update2)
            if update1 == nonce2:
                logger.info("Path length of nonce1 is %d shorter than nonce2..." % i)
                return 1
            if update2 == nonce1:
                logger.info("Path length of nonce2 is %d shorter than nonce1..." % i)
                return -1
        
        logger.info("Could not determine which nonce represents the shorter path, maybe another publisher was found...")
        return None
    
    def _route_covert_data(self, msg, incoming_connection=None):
        self._init_channel(msg["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        
        if msg.get_type().endswith("_advertise"):
            nonce = base64.b64decode(bytes(msg["nonce"], "ascii"))  # decode nonce
            
            # search nonce in routing table to determine if a new shortest path was found or a loop was detected
            # and drop advertisement if a loop was detected
            found = False
            for routing_nonce in list(self.advertisement_routing_table[msg["channel"]].keys()):
                result = self._find_shorter_hashchain(routing_nonce, nonce)
                # routing table entry is shorter than the newly received one:
                if result == 1:
                    logger.warning("Routing loop with longer path length detected, dropping advertisement message for channel '%s' coming from %s!" % (str(msg["channel"]), str(incoming_peer)))
                    return
                # routing table entry is queal to the newly received one:
                elif result == 0:
                    logger.warning("Routing loop with equal path lenth detected, dropping advertisement message for channel '%s' coming from %s!" % (str(msg["channel"]), str(incoming_peer)))
                    return
                elif result == -1:
                    logger.info("New shortest advertisement path for channel '%s' found: %s..." % (str(msg["channel"]), str(incoming_peer)))
                    del self.advertisement_routing_table[msg["channel"]][routing_nonce]         # delete old advertisement routing entry
                    self.advertisement_routing_table[msg["channel"]][nonce] = incoming_peer     # add new advertisement routing entry
            # could not determine if this nonce was already seen --> create a new advertisement routing table entry for this new publisher:
            if not found:
                logger.info("First advertisement for channel '%s' received, shortest path is now: %s..." % (str(msg["channel"]), str(incoming_peer)))
                self.advertisement_routing_table[msg["channel"]][nonce] = incoming_peer
            
            # send back subscribe requests if we are a subscriber of this channel
            if msg["channel"] in self.subscriptions:
                self._add_timer(SUBSCRIBE_DELAY, {
                    "command": "Flooding_send_subscribe",
                    "channel": msg["channel"],
                    "publisher_nonce": nonce,                           # this is needed to select the right path to publisher
                })
            
            # calculate new nonce and update msg with this new nonce
            nonce = self._hash(nonce)
            msg["nonce"] = str(base64.b64encode(nonce), "ascii")
            
            # send out advertisement to all peers (excluding the incoming one)
            connections = {key: value for key, value in self.connections.items() if key != incoming_peer}
            if not len(connections):
                logger.warning("No additional peers found, cannot route advertisement further!")
                return
            for node_id in connections:
                logger.info("Routing advertisement for channel '%s' to peer %s..." % (str(msg["channel"]), str(node_id)))
                self._send_covert_msg(msg, connections[node_id])
        
        elif msg.get_type().endswith("_subscribe"):
            # decode nonces
            nonce = base64.b64decode(bytes(msg["nonce"], "ascii"))
            publisher_nonce = base64.b64decode(bytes(msg["publisher_nonce"], "ascii"))
            
            # activate shortest data route if we are a forwarder or publisher
            if incoming_peer:
                found = False
                for routing_nonce in list(self.data_routing_table[msg["channel"]].keys()):
                    result = self._find_shorter_hashchain(routing_nonce, nonce)
                    # the following cases are commented out because they should never occur here
                    ## routing table entry is shorter than the newly received one:
                    #if result == 1:
                        #logger.warning("Routing loop with longer path length detected, dropping subscribe message for channel '%s' coming from %s!" % (str(msg["channel"]), str(incoming_peer)))
                        #return
                    ## routing table entry is queal to the newly received one:
                    #elif result == 0:
                        #logger.warning("Routing loop with equal path lenth detected, dropping subsribe message for channel '%s' coming from %s!" % (str(msg["channel"]), str(incoming_peer)))
                        #return
                    if result == -1:
                        logger.info("New shortest data path for channel '%s' found: %s..." % (str(msg["channel"]), str(incoming_peer)))
                        # delete old data routing entry
                        del self.data_routing_table[msg["channel"]][routing_nonce]
                        # add new data routing entry
                        if not self.data_routing_table[msg["channel"]][nonce]:
                            self.data_routing_table[msg["channel"]][nonce] = {}
                        self.data_routing_table[msg["channel"]][nonce][publisher_nonce] = incoming_peer
                # could not determine if this nonce was already seen --> create a new data routing table entry for this new subscriber:
                if not found:
                    logger.info("First subsribe request for channel '%s', shortest data path is now: %s..." % (str(msg["channel"]), str(incoming_peer)))
                    if not self.data_routing_table[msg["channel"]][nonce]:
                        self.data_routing_table[msg["channel"]][nonce] = {}
                    self.data_routing_table[msg["channel"]][nonce][publisher_nonce] = incoming_peer
            
            # calculate new nonce and update msg with this new nonce
            nonce = self._hash(nonce)
            msg["nonce"] = str(base64.b64encode(nonce), "ascii")
            
            # send out subscribe message using the recorded shortest path
            found = False
            for routing_nonce in list(self.advertisement_routing_table[msg["channel"]].keys()):
                result = self._find_shorter_hashchain(routing_nonce, publisher_nonce)
                if result != None:      # publisher nonce equals routing_nonce
                    peer = self.advertisement_routing_table[msg["channel"]][routing_nonce]
                    if peer in self.connections:
                        self._send_covert_msg(msg, self.connections[peer])
                        found = True
            if not found:
                logger.warning("Publisher nonce not found in advertisement routing table, cannot not route subscribe request further!")
        
        elif msg.get_type().endswith("_unsubscribe"):
            publisher_nonce = base64.b64decode(bytes(msg["publisher_nonce"], "ascii"))      # decode nonce
            
            # clean up data routing table
            for channel in self.data_routing_table:
                for nonce in list(self.data_routing_table[channel].keys()):
                    if self.data_routing_table[channel][nonce]["peer"] == incoming_peer:
                        del self.data_routing_table[channel][nonce]
    
    def _route_data(self, msg, incoming_connection=None):
        self._init_channel(msg["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        
        # inform own subscriber of new data
        if msg["channel"] in self.subscriptions:
            self.subscriptions[msg["channel"]](msg["data"])
        
        if not len(self.data_routing_table[msg["channel"]]):
            logger.warning("No additional peers found, cannot route data further!")
            return
        
        # send out data to all peers in data routing table (excluding the incoming one)
        connections = set()
        for nonce, entry in self.data_routing_table[msg["channel"]].items():
            if entry["peer"] in self.connections and entry["peer"] != incoming_peer:
                connections.add(entry["peer"])
        for peer in connections:
            logger.info("Routing data to peer '%s'..." % peer)
            self._send_msg(msg, self.connections[peer])
    
    def _remove_connection_command(self, command):
        # call parent class for common tasks
        super(Flooding, self)._remove_connection_command(command)
        peer = command["connection"].get_peer_id()
        
        # send out unsubscribe messages and clean up data routing table afterwards
        for channel in self.data_routing_table:
            for nonce in list(self.data_routing_table[channel].keys()):
                for publisher_nonce in self.data_routing_table[channel][nonce]:
                    if self.data_routing_table[channel][nonce][publisher_nonce] == peer:
                        self._route_covert_data(Message("%s_unsubscribe" % self.__class__.__name__, {
                            "channel": channel,
                            "nonce": nonce,                         # this is needed to clean up the right routing table entry
                            "publisher_nonce": publisher_nonce      # this is needed to select the right path to publisher
                        }))
                        del self.data_routing_table[channel][nonce][publisher_nonce]
                    
                    
            # TODO: send out unsubscribe for this nonce (or node)??
        
        # clean up advertisement routing table, too
        for channel in self.advertisement_routing_table:
            for nonce in list(self.advertisement_routing_table[channel].keys()):
                if self.advertisement_routing_table[channel][nonce] == peer:
                    del self.advertisement_routing_table[channel][nonce]
                    # TODO: send out unadvertise for this nonce (or node??)
    
    def _publish_command(self, command):
        if command["channel"] not in self.publishing:   # create overlay if needed
            self.publishing.add(command["channel"])
            self._call_command({
                "command": "Flooding_advertise",
                "channel": command["channel"]
            })
        else:    
            msg = Message("%s_data" % self.__class__.__name__, {
                "channel": command["channel"],  # this would be the encrypted topic if a TTP was used
                "data": command["data"],
            })
            self._route_data(msg)
    
    def _Flooding_advertise_command(self, command):
        logger.info("(Re)advertising channel '%s'..." % str(command["channel"]))
        nonce = str(base64.b64encode(os.urandom(32)), "ascii")
        msg = Message("%s_advertise" % self.__class__.__name__, {
            "channel": command["channel"],      # this would be the encrypted topic if a TTP was used
            "publisher": self.node_id,
            "nonce": nonce
        })
        if not len(self.connections):
            logger.warning("No peers found, cannot advertise channel '%s'!" % str(command["channel"]))
        else:
            for node_id in self.connections:
                logger.info("Sending out covert msg for channel '%s' to peer %s..." % (str(msg["channel"]), str(node_id)))
                self._send_covert_msg(msg, self.connections[node_id])
        
        # flood advertisement again in REFLOOD_TIME seconds
        self._add_timer(REFLOOD_TIME, {
            "command": "Flooding_advertise",
            "channel": command["channel"]
        })
    
    def _Flooding_send_subscribe_command(self, command):
        self._route_covert_data(Message("%s_subscribe" % self.__class__.__name__, {
            "channel": command["channel"],
            "publisher_nonce": command["publisher_nonce"],      # this is needed to select the right path to publisher
            "nonce": self._hash(bytes(self.node_id, "ascii"))   # this is needed to break old longer paths
        }))
