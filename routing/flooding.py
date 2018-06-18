import os
import uuid
import base64
import functools
from random import SystemRandom
from sortedcontainers import SortedList, SortedDict
import logging
logger = logging.getLogger(__name__)

#crypto imports
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes

# own classes
from networking import Message
from .base import Router


class StrToClass(object):
    def __init__(self, s):
        self.s = s
    def __repr__(self):
        return self.s

class Flooding(Router):
    settings = {
        "ANONYMOUS_IDS": True,
        "MAX_HASHES": 1000,         # this limits the maximum diameter of the underlay to this value
        "SUBSCRIBE_DELAY": 2.0,
        "RANDOM_MASTER_PUBLISH": False,
        "ACTIVE_PATH_PROBES": True,
    }
    
    def __init__(self, node_id, queue):
        super(Flooding, self).__init__(node_id, queue)
        self.publishing = set()
        self.master = {}
        self.subscriber_ids = {}
        self.subscription_timers = {}
        self.advertisement_routing_table = {}
        self.outstanding_reflood = {}
        self.outstanding_path_probes = {}
        self.data_routing_table = {}
        logger.info("%s router initialized..." % self.__class__.__name__)
    
    def _init_channel(self, channel):
        if not channel in self.advertisement_routing_table:
            self.advertisement_routing_table[channel] = {}
        if not channel in self.data_routing_table:
            self.data_routing_table[channel] = {}
        if not channel in self.master:
            self.master[channel] = False
        if not channel in self.outstanding_reflood:
            self.outstanding_reflood[channel] = {}
        if not channel in self.outstanding_path_probes:
            self.outstanding_path_probes[channel] = {}
    
    @functools.lru_cache(maxsize=4096)  # ~4 * settings["MAX_HASHES"] (power of 2 is faster)
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
                return -1
            if update2 == nonce1:
                return 1
        return None
    
    def _find_nonce(self, nonce, iterable):
        for entry in iterable:
            if self._compare_nonces(nonce, entry) != None:
                return entry
        return None
    
    def _add_nonce(self, channel, nonce, peer_id):
        # search our routing table to find nonce liste to append this list to or create a new list if none is found
        chain = self._find_nonce(nonce, self.advertisement_routing_table[channel])
        if chain:
            if nonce not in self.advertisement_routing_table[channel][chain]:
                self.advertisement_routing_table[channel][chain][nonce] = set()
            self.advertisement_routing_table[channel][chain][nonce].add(peer_id)
            if self.advertisement_routing_table[channel][chain].peekitem(0)[0] == nonce:
                logger.debug("%s: Added %s nonce %s to chain %s" % (
                    channel, "shorter", str(base64.b64encode(nonce), "ascii"), str(base64.b64encode(chain), "ascii")))
                return ("shorter", chain)   # hash chain of nonce was already in routing table but we now found a shorter path
            else:
                logger.debug("%s: Added %s nonce %s to chain %s" % (
                    channel, "longer", str(base64.b64encode(nonce), "ascii"), str(base64.b64encode(chain), "ascii")))
                return ("longer", chain)    # hash chain of nonce was already in routing table and it remains the shortest path
        else:
            chain = nonce
            self.advertisement_routing_table[channel][chain] = SortedDict(functools.cmp_to_key(self._compare_nonces), {nonce: set([peer_id])})
            logger.debug("%s: Added %s nonce %s to chain %s" % (
                channel, "new", str(base64.b64encode(nonce), "ascii"), str(base64.b64encode(chain), "ascii")))
            return ("new", chain)        # hash chain of nonce was NOT already in routing table
    
    def _route_covert_data(self, msg, incoming_connection=None):
        if msg.get_type().endswith("_init"):
            return self._route_init(msg, incoming_connection)
        elif msg.get_type().endswith("_advertise"):
            return self._route_advertise(msg, incoming_connection)
        elif msg.get_type().endswith("_probePath"):
            return self._route_probePath(msg, incoming_connection)
        elif msg.get_type().endswith("_unadvertise"):
            return self._route_unadvertise(msg, incoming_connection)
    
    def _route_advertise(self, advertisement, incoming_connection):
        logger.info("Routing advertisement: %s coming from %s..." % (str(advertisement), str(incoming_connection)))
        self._init_channel(advertisement["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        nonce = base64.b64decode(bytes(advertisement["nonce"], "ascii"))    # decode nonce
        
        # prevent advertisement loop if this hashchain was already received from the same incoming_peer
        chain = self._find_nonce(nonce, self.advertisement_routing_table[advertisement["channel"]])
        # hashchain known and already seen from this incoming peer --> abort flooding
        if chain and incoming_peer in set(
        peer_id for peer_set in self.advertisement_routing_table[advertisement["channel"]][chain].values() for peer_id in peer_set
        ):
            logger.debug("This hashchain was already received from this peer, aborting advertise (re)flooding")
            return
        
        # send subscribe request if needed (make sure we wait the full SUBSCRIBE_DELAY seconds until we subscribe)
        #TODO: richtig machen (bei allen bekannten bzw. neuen mastern subscriben)
        if advertisement["channel"] in self.subscription_timers:
            self._abort_timer(self.subscription_timers[advertisement["channel"]])
        self.subscription_timers[advertisement["channel"]] = self._add_timer(Flooding.settings["SUBSCRIBE_DELAY"], {
            "command": "Flooding_create_overlay",
            "channel": advertisement["channel"]
        })
        
        # now add nonce to routing table
        (ordering, entry) = self._add_nonce(advertisement["channel"], nonce, incoming_peer)
        
        # abort REflooding if we already know this hashchain
        if advertisement["reflood"] and ordering != "new":
            logger.debug("We already know this hashchain, arborting advertise REflooding...")
            return
        
        # abort flooding if we already know a shorter path
        if ordering == "longer":
            logger.debug("This path was longer, aborting advertise (re)flooding...")
            return
        
        # encode nonce again and update message with this new nonce
        nonce = self._hash(nonce)       # the path to our neighbor is one step further
        advertisement["nonce"] = str(base64.b64encode(nonce), "ascii")
        
        # route advertisement further to all peers but the incoming one
        for con in set(value for key, value in self.connections.items() if key != incoming_peer):
            logger.info("Routing advertisement for channel '%s' to %s..." % (str(advertisement["channel"]), str(con)))
            self._send_covert_msg(advertisement, con)
    
    def _route_probePath(self, probePath, incoming_connection):
        logger.info("Routing probePath: %s coming from %s..." % (str(probePath), str(incoming_connection)))
        self._init_channel(probePath["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        chain = base64.b64decode(bytes(probePath["chain"], "ascii"))    # decode chain nonce
        
        # return incoming barrier to sender
        if not probePath["returning"]:
            nonce = None
            # search for hashchain
            chain = self._find_nonce(chain, self.advertisement_routing_table[probePath["channel"]])
            if chain and len(self.advertisement_routing_table[probePath["channel"]][chain]):
                # shortest known path
                nonce = str(base64.b64encode(self.advertisement_routing_table[probePath["channel"]][chain].peekitem(0)[0]), "ascii")
            logger.debug("Answering probePath with '%s'..." % nonce if Flooding.settings["ACTIVE_PATH_PROBES"])
            self._send_covert_msg(Message("%s_probePath" % self.__class__.__name__, {
                "channel": probePath["channel"],
                "chain": probePath["chain"],
                "returning": True,
                "nonce": nonce if Flooding.settings["ACTIVE_PATH_PROBES"],
            }), incoming_connection)
            return
        
        # handle returning barrier
        
        # add nonce to routing table if given (ACTIVE_PATH_PROBES at remote peer is true and remote peer did know an alternative path)
        if probePath["nonce"]:
            nonce = base64.b64decode(bytes(probePath["nonce"], "ascii"))    # decode nonce
            (ordering, entry) = self._add_nonce(probePath["channel"], nonce, incoming_peer)
        # remove incoming_peer from outstanding_path_probes (if it is still outstanding, ignore returning probePath message otherwise)
        chain = self._find_nonce(chain, self.outstanding_path_probes[probePath["channel"]])
        if chain and incoming_peer in self.outstanding_path_probes[probePath["channel"]][chain]:
            self.outstanding_path_probes[probePath["channel"]][chain].discard(incoming_peer)
            # all barriers returned: resume unadvertisement handling here
            if not len(self.outstanding_path_probes[probePath["channel"]][chain]):
                del self.outstanding_path_probes[probePath["channel"]][chain]   # cleanup
                
                alternatives = SortedList(key=functools.cmp_to_key(self._compare_nonces))
                for nonce, peer_set in self.advertisement_routing_table[probePath["channel"]][chain].items():
                    peer_set.discard(incoming_peer)
                    if len(peer_set):
                        alternatives.update(peer_set)
                
                
                # REflood advertisement of alternative path
                for peer_id in set(peer_id for peer_id in self.outstanding_reflood[probePath["channel"]][chain] if peer_id in self.connections):
                    self._send_covert_msg(Message("%s_advertise" % self.__class__.__name__, {
                        "channel": probePath["channel"],
                        "nonce": str(base64.b64encode(self._hash(alternatives[0])), "ascii"),    # shortest known path
                        "reflood": True
                    }), self.connections[peer_id])
                del self.outstanding_reflood[probePath["channel"]][chain]
    
    def _route_unadvertise(self, unadvertisement, incoming_connection):
        logger.info("Routing unadvertisement: %s coming from %s..." % (str(unadvertisement), str(incoming_connection)))
        self._init_channel(unadvertisement["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        chain = base64.b64decode(bytes(unadvertisement["chain"], "ascii"))    # decode chain nonce
        
        # search for hashchain and abort if it can not be found
        chain = self._find_nonce(chain, self.advertisement_routing_table[unadvertisement["channel"]])
        if not chain:
            logger.debug("Could not find hashchain in routing table, aborting unadvertise flooding...")
            return
            
        # remove ALL nonces of this hashchain pointing to the incoming peer
        alternatives = SortedList(key=functools.cmp_to_key(self._compare_nonces))
        was_known = False
        for nonce, peer_set in dict(self.advertisement_routing_table[unadvertisement["channel"]][chain].items()).items():
            if incoming_peer in peer_set:
                was_known = True
            peer_set.discard(incoming_peer)
            if len(peer_set):
                alternatives.update(peer_set)
            else:
                del self.advertisement_routing_table[unadvertisement["channel"]][chain][nonce]
                if not len(self.advertisement_routing_table[unadvertisement["channel"]][chain]):
                    del self.advertisement_routing_table[unadvertisement["channel"]][chain]
        
        # don't route unadvertise further if we didn't know the unadvertised path
        if not was_known:
            logger.debug("We did not know a path to this incoming peer, aborting unadvertise flooding...")
            return
        
        # remove peers we don't have connections to from our alternatives (we want to be on the safe side)
        alternatives.intersection_update(set(self.connections.keys()))
        
        # send path probes (barriers) to all alternative paths and abort unadvertisement flooding (restart handling when all path probes returned)
        if len(alternatives):
            logger.debug("We know some alternative paths, synchronizing with those peers using path probes...")
            # add incoming_peer to outstanding_reflood set
            if chain not in self.outstanding_reflood[unadvertisement["channel"]]:
                self.outstanding_reflood[unadvertisement["channel"]][chain] = set()
            self.outstanding_reflood[unadvertisement["channel"]][chain].add(incoming_peer)
            # send out path probes to all alternatives (incoming_peer will never be listed in those alternatives)
            if chain not in self.outstanding_path_probes[unadvertisement["channel"]]:
                self.outstanding_path_probes[unadvertisement["channel"]][chain] = set()
            for peer_id in alternatives:
                self._send_covert_msg(Message("%s_probePath" % self.__class__.__name__, {
                    "channel": unadvertisement["channel"],
                    "chain": unadvertisement["chain"],
                    "returning": False,
                }), self.connections[peer_id])
                self.outstanding_path_probes[unadvertisement["channel"]][chain].add(peer_id)
            # handling of this unadvertisements will continue if all barriers have returned
            return
        
        # route unadvertise to all peers but incoming one
        for con in set(value for key, value in self.connections.items() if key != incoming_peer):
            logger.info("Routing unadvertisement for channel '%s' to %s..." % (str(unadvertisement["channel"]), str(con)))
            self._send_covert_msg(unadvertisement, con)
    
    def _route_init(self, init, incoming_connection):
        logger.info("Routing init: %s coming from %s..." % (str(init), str(incoming_connection)))
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        
        # finally add our new peer to connections list
        if incoming_connection:
            self.connections[incoming_peer] = incoming_connection
        
        # update own routing table to include remote entries
        for channel, _nonce in init["advertisements"].items():
            self._init_channel(channel)
            nonce = base64.b64decode(bytes(_nonce, "ascii"))        # decode nonce
            (ordering, entry) = self._add_nonce(channel, nonce, incoming_peer)
            if ordering == "new":      # flood shortest path further if this hashchain is new
                # fake an advertisement message incoming from connection "incoming_connection"
                self._route_covert_data(Message("%s_advertise" % self.__class__.__name__, {
                    "channel": channel,
                    "nonce": _nonce,
                    "reflood": True
                }), incoming_connection)
    
    def _route_data(self, msg, incoming_connection=None):
        if incoming_connection:     # don't log locally published data (makes the log more clear)
            logger.info("Routing data: %s coming from %s..." % (str(msg), str(incoming_connection)))
        self._init_channel(msg["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        
        # if this is a publication to master publisher --> route it along the shortest path taken from advertisement_routing_table
        # to the master publisher indicated by nonce and skip normal data routing and subscription tests
        if msg.get_type().endswith("_publish"):
            if self.master[msg["channel"]]:     # we are the master --> publish data on behalf of slave publisher
                # send data to all subscribers (do not pass incoming_connection to _route_data() because we are publishing on behalf of a slave)
                self._route_data(Message("%s_data" % self.__class__.__name__, {
                    "channel": msg["channel"],
                    "data": msg["data"],
                }))
                # the publish message ends here
                return
            
            # route data further (we are not a master): select next hop based on shortest path for this hashchain
            nonce = base64.b64decode(bytes(msg["nonce"], "ascii"))    # decode nonce
            chain = self._find_nonce(nonce, self.advertisement_routing_table[msg["channel"]])
            if chain and len(node_id = self.advertisement_routing_table[msg["channel"]][chain]):
                node_id = self.advertisement_routing_table[msg["channel"]][chain].peekitem(0)[0]
            if chain and node_id != incoming_peer and node_id in self.connections:
                logger.info("Routing publish data to %s..." % self.connections[node_id])
                self._send_msg(msg, self.connections[node_id])
            else:
                logger.warning("Nonce not found while trying to route publish message to master publisher!")
            return
        
        # TODO: normal data routing comes here (*OLD* code below)
        return
        
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
        # anonymize routing tables (we only need a mapping of shortest nonces to channels), and hash + encode nonce for transmission
        advertisements = {channel: str(base64.b64encode(self._hash(list(noncelist)[0])), "ascii")
                          for channel, entries in self.advertisement_routing_table.items() for _, noncelist in entries.items() if len(noncelist)}
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
        
        # extract all nonces of this peer for which we have to unadvertise paths
        unadvertise = {}
        for channel in self.advertisement_routing_table:
            for chain in self.advertisement_routing_table[channel]:
                for nonce in self.advertisement_routing_table[channel][chain]:
                    for node_id in self.advertisement_routing_table[channel][chain][nonce]:
                        if node_id == peer:
                            # anonymize nonce before adding to list (peers need to know only the corresponding hashchain, not the explicit nonce)
                            if Flooding.settings["ANONYMOUS_IDS"]:
                                for x in range(SystemRandom().randint(0, 32)):
                                    nonce = self._hash(nonce)
                            unadvertise[peer] = nonce
        
        for nonce in unadvertise.values():
            # unadvertise path to this peer
            self._route_covert_data(Message("%s_unadvertise" % self.__class__.__name__, {
                "channel": channel,
                "chain": str(base64.b64encode(nonce), "ascii"),
            }), con)
            # and afterwards: force return all probePath barriers of this peer
            self._route_covert_data(Message("%s_probePath" % self.__class__.__name__, {
                "channel": channel,
                "chain": str(base64.b64encode(nonce), "ascii"),
                "returning": True,
                "nonce": None,
            }), con)
        
    
    def _publish_command(self, command):
        self._init_channel(command["channel"])
        if command["channel"] not in self.publishing:
            self.publishing.add(command["channel"])
            # we are the first publisher --> flood underlay with advertisements (we are one of the masters now)
            if not len(self.advertisement_routing_table[command["channel"]]):
                self.master[command["channel"]] = True
                self._route_covert_data(Message("%s_advertise" % self.__class__.__name__, {
                    "channel": command["channel"],
                    "nonce": str(base64.b64encode(os.urandom(32)), "ascii"),
                    "reflood": False
                }))
        # start sending out data
        if self.master[command["channel"]] or not len(self.advertisement_routing_table[command["channel"]]):
            # send data to all subscribers
            self._route_data(Message("%s_data" % self.__class__.__name__, {
                "channel": command["channel"],
                "data": command["data"],
            }))
        else:
            master = self.advertisement_routing_table[command["channel"]].keys()[0]
            if Flooding.settings["RANDOM_MASTER_PUBLISH"]:
                master = SystemRandom.choice(self.advertisement_routing_table[command["channel"]].keys())
            # send data to randomly choosen master publisher (choosing a new one for every message reduces message loss if one master fails)
            self._route_data(Message("%s_publish" % self.__class__.__name__, {
                "channel": command["channel"],
                "data": command["data"],
                "nonce": master
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
    
    def _dump_command(self, command):
        advertisement_routing_table = {}
        for channel in self.advertisement_routing_table:
            advertisement_routing_table[channel] = {}
            for chain in self.advertisement_routing_table[channel]:
                dict_contents = ["'%s': %s" % (str(base64.b64encode(nonce), "ascii"), str(self.advertisement_routing_table[channel][chain][nonce]))
                                 for nonce, peer_set in self.advertisement_routing_table[channel][chain].items()]
                advertisement_routing_table[channel][str(base64.b64encode(chain), "ascii")] = StrToClass("SortedDict(%s)" % ", ".join(dict_contents))
        state = {
            "publishing": list(self.publishing),
            "master": self.master,
            "subscriber_ids": self.subscriber_ids,
            "subscription_timers": self.subscription_timers,
            "advertisement_routing_table": advertisement_routing_table,
            "data_routing_table": self.data_routing_table
        }
        logger.info("INTERNAL STATE:\n%s" % str(state))
        if command["callback"]:
            command["callback"](state)
    
    def _Flooding_create_overlay_command(self, command):
        del self.subscription_timers[command["channel"]]
        return
        
        # create new subscriber id for this channel if needed
        if command["channel"] not in self.subscriber_ids:
            self.subscriber_ids[command["channel"]] = str(uuid.uuid4()) if Flooding.settings["ANONYMOUS_IDS"] else self.node_id
        
        # send out subscribe message
        self._route_covert_data(Message("%s_subscribe" % self.__class__.__name__, {
            "channel": command["channel"],  # this would be the encrypted topic if a TTP was used
            "subscriber": self.subscriber_ids[command["channel"]],
        }))
    
