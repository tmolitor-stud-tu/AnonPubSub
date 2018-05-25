import os
import base64
import functools
from sortedcontainers import SortedList
import logging
logger = logging.getLogger(__name__)

#crypto imports
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes

# own classes
from networking import Message
from .base import Router


class Flooding(Router):
    settings = {
        "ANONYMOUS_IDS": True,
        "MAX_HASHES": 1000,         # this limits the maximum diameter of the underlay to this value
        "SUBSCRIBE_DELAY": 2.0
    }
    
    def __init__(self, node_id, queue):
        super(Flooding, self).__init__(node_id, queue)
        self.publishing = set()
        self.master = {}
        self.subscriber_ids = {}
        self.subscription_timers = {}
        self.advertisement_routing_table = {}
        self.data_routing_table = {}
        logger.info("%s router initialized..." % self.__class__.__name__)
    
    def _init_channel(self, channel):
        if not channel in self.advertisement_routing_table:
            self.advertisement_routing_table[channel] = {}
        if not channel in self.data_routing_table:
            self.data_routing_table[channel] = {}
        if not channel in self.master:
            self.master[channel] = False
    
    @functools.lru_cache(maxsize=4000)  # ~4 * settings["MAX_HASHES"]
    def _hash(self, nonce):
        digest = hashes.Hash(hashes.BLAKE2s(32), backend=default_backend())
        digest.update(nonce)
        return digest.finalize()
    
    # this returns -1, 0 or 1 like compare functions used in sorting do
    @functools.lru_cache(maxsize=64)
    def _compare_nonces(self, nonce1, nonce2):
        if nonce1 == nonce2:
            return 0       # both paths are of equal length
        update1 = nonce1
        update2 = nonce2
        for i in range(1, Flooding.settings["MAX_HASHES"] + 1):
            update1 = self._hash(update1)
            update2 = self._hash(update2)
            if update1 == nonce2:
                return 1
            if update2 == nonce1:
                return -1
        return None
    
    def _add_nonce(self, channel, nonce, nonce_value):
        # search our routing table to find nonce liste to append this list to or create a new list if none is found
        found = None
        for routing_nonce in self.advertisement_routing_table[channel]:
            if self._compare_nonces(nonce, routing_nonce) != None:
                found = routing_nonce
                break
        if found:
            self.advertisement_routing_table[channel][found][nonce] = nonce_value
            if self.advertisement_routing_table[channel][found].peekitem(0) == nonce:
                return ("shorter", found)     # hash chain of nonce was already in routing table but we found a shorter path
            else:
                return ("longer", found)     # hash chain of nonce was already in routing table and it remains the shortest path
        self.advertisement_routing_table[channel][nonce] = SortedDict(key=functools.cmp_to_key(self._compare_nonces), {
            nonce: nonce_value
        })
        return ("new", nonce)        # hash chain of nonce was NOT already in routing table
    
    def _route_covert_data(self, msg, incoming_connection=None):
        if msg.get_type().endswith("_init"):
            return self._route_init(msg, incoming_connection)
        elif msg.get_type().endswith("_advertise"):
            return self._route_advertise(msg, incoming_connection)
    
    def _route_advertise(advertisement, incoming_connection):
        logger.info("Routing advertisement: %s coming from %s..." % (str(advertisement), str(incoming_connection)))
        self._init_channel(advertisement["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        nonce = base64.b64decode(bytes(advertisement["nonce"], "ascii"))    # decode nonce
        nonce = self._hash(nonce)       # the path to us is one step further than to our neighbor
        
        # send subscribe request if needed (make sure we wait the full SUBSCRIBE_DELAY seconds until we subscribe)
        if command["channel"] in self.subscription_timers:
            self._abort_timer(self.subscription_timers[command["channel"]])
        self.subscription_timers[command["channel"]] = self._add_timer(Flooding.settings["SUBSCRIBE_DELAY"], {
            "command": "Flooding_create_overlay",
            "channel": advertisement["channel"]
        })
        
        if not incoming_connection:     # the advertisement is originating here and that makes it always "new"
            ordering = "new"
        else:
            (ordering, entry) = self._add_nonce(advertisement["channel"], nonce, incoming_peer)
        if ordering in ("new", "shorter"):      # if this is a new or shorter path we have to flood the advertisement further
            # encode nonce again and update message with this new nonce
            advertisement["nonce"] = str(base64.b64encode(nonce), "ascii")
            
            # route advertisement further to all peers but the incoming one
            connections = {key: value for key, value in self.connections.items() if key != incoming_peer} 
            for con in connections:
                logger.info("Routing advertisement for channel '%s' to %s..." % (str(advertisement["channel"]), str(con)))
                self._route_covert_data(advertisement, con)
        
    def _route_init(init, incoming_connection):
        logger.info("Routing init: %s coming from %s..." % (str(init), str(incoming_connection)))
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        
        # add new peer to connections list
        if incoming_connection:
            self.connections[incoming_peer] = incoming_connection
        
        # update own routing table to include remote entries
        for channel, _nonce in init["advertisements"].items():
            self._init_channel(channel)
            nonce = base64.b64decode(bytes(_nonce, "ascii"))        # decode nonce
            nonce = self._hash(nonce)       # the path to us is one step further than to our neighbor
            (ordering, entry) = self._add_nonce(channel, nonce, incoming_peer)
            if ordering == "new":      # flood shortest path further if it is new
                # fake an advertisement message incoming from connection "con"
                self._route_covert_data(Message("%s_advertise" % self.__class__.__name__, {
                    "channel": channel,
                    "nonce": _nonce
                }), con)
    
    def _route_data(self, msg, incoming_connection=None):
        if incoming_connection:     # don't log locally published data (makes the log more clear)
            logger.info("Routing data: %s coming from %s..." % (str(msg), str(incoming_connection)))
        self._init_channel(msg["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        
        # if this is a publication to master publisher --> route it along the shortest path(s) taken from advertisement_routing_table
        # to all master publishers and skip normal data routing and subscription test
        if msg.get_type().endswith("_publish"):
            if self.master[msg["channel"]]:     # we are the master --> publish data on behalf of slave publisher
                # send data to all subscribers (do not pass incoming_connection to _route_data() because we are publishing on behalf of a slave)
                self._route_data(Message("%s_data" % self.__class__.__name__, {
                    "channel": msg["channel"],
                    "data": msg["data"],
                }))
            # route data further (needed if we are in the path to (other) masters)
            connections = set()
            for _, noncelist in self.advertisement_routing_table[msg["channel"]].items():
                if len(noncelist):
                    connections.append(noncelist.peekitem(0))   # shortest path to this particular master publisher
            # transform peer set to real connection list
            connections = [con for node_id, con in self.connections.items() if node_id in connections and node_id != incoming_peer]
            for con in connections:
                logger.info("Routing publish data to %s..." % con)
                self._send_msg(msg, con)
        
        # TODO: normal data routing comes here (*OLD* code below)
        
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
    
    def _add_connection_command(self, command):
        # no need to call parent class here, doing everything on our own
        con = command["connection"]
        peer = con.get_peer_id()
        # anonymize routing tables (we only need a mapping of shortest nonces to channels) and encode nonce for transmission
        advertisements = {channel: str(base64.b64encode(list(noncelist)[0]), "ascii") for channel, entries in self.advertisement_routing_table.items() for _, noncelist in entries.items() if len(noncelist)}
        # exchange routing tables before adding connection to our connections list (which is done upon receiving the routing table of our peer)
        logger.info("Sending init message to newly connected peer at %s..." % str(con))
        self._send_covert_msg(Message("%s_init" % self.__class__.__name__, {
            "advertisements": advertisements,
        }), con)
    
    def _remove_connection_command(self, command):
        # no need to call parent class here, doing everything on our own
        con = command["connection"]
        peer = con.get_peer_id()
        if peer in self.connections:
            del self.connections[peer]
        # TODO: react properly here
        # TODO: remove peer from advertisement_routing_table and data_routing_table
        # TODO: send unsubscribe and/or unpublish messages to tear down paths
    
    def _publish_command(self, command):
        self._init_channel(command["channel"])
        if command["channel"] not in self.publishing:
            self.publishing.add(command["channel"])
            # we are the first publisher --> flood underlay with advertisements (we are the master now)
            if not len(self.advertisement_routing_table[command["channel"]]):
                self.master[command["channel"]] = True
                self._route_covert_data(Message("%s_advertise" % self.__class__.__name__, {
                    "channel": command["channel"],
                    "nonce": os.urandom(32)
                }))
        # start sending out data
        if self.master[command["channel"]]:
            # send data to all subscribers
            self._route_data(Message("%s_data" % self.__class__.__name__, {
                "channel": command["channel"],  # this would be the encrypted topic if a TTP was used
                "data": command["data"],
            }))
        else:
            # send data to master publisher
            self._route_data(Message("%s_publish" % self.__class__.__name__, {
                "channel": command["channel"],  # this would be the encrypted topic if a TTP was used
                "data": command["data"],
            }))
    
    def _subscribe_command(self, command):
        self._init_channel(command["channel"])
        if command["channel"] not in self.subscriptions:
            # make sure we wait the full SUBSCRIBE_DELAY seconds until we subscribe
            if command["channel"] in self.subscription_timers:
                self._abort_timer(self.subscription_timers[command["channel"]])
            self.subscription_timers[command["channel"]] = self._add_timer(Flooding.settings["SUBSCRIBE_DELAY"], {
                "command": "Flooding_create_overlay",
                "channel": command["channel"]
            })
        
        # call parent class for common tasks
        super(Flooding, self)._unsubscribe_command(command)
    
    def _unsubscribe_command(self, command):
        if command["channel"] in self.subscriptions:
            del self.subscriptions[command["channel"]]
    
    def _Flooding_create_overlay_command(self, command):
        del self.subscription_timers[command["channel"]]
        
        # create new subscriber id for this channel if needed
        if command["channel"] not in self.subscriber_ids:
            self.subscriber_ids[command["channel"]] = str(uuid.uuid4()) if Flooding.settings["ANONYMOUS_IDS"] else self.node_id
        
        # send out subscribe message
        self._route_covert_data(Message("%s_subscribe" % self.__class__.__name__, {
            "channel": command["channel"],  # this would be the encrypted topic if a TTP was used
            "subscriber": self.subscriber_ids[command["channel"]],
        }))
    
