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
from utils import init_mixins, final
from .router import Router
from .active_paths_mixin import ActivePathsMixin
from .probabilistic_forwarding_mixin import ProbabilisticForwardingMixin
from .cover_traffic_mixin import CoverTrafficMixin


# this is used to pretty print sortedDict contents
class StrToClass(object):
    def __init__(self, s):
        self.s = s
    def __repr__(self):
        return self.s

@final
@init_mixins
class Simple(Router, ProbabilisticForwardingMixin, CoverTrafficMixin):
    settings = {
        "ANONYMOUS_IDS": True,
        "MAX_HASHES": 1000,         # this limits the maximum diameter of the underlay to this value
        "REFLOOD_DELAY": 4.0,
        "PROBABILISTIC_FORWARDING_FRACTION": 0,      # probability of neighbor selection to select for probabilistic forwarding
        "AGGRESSIVE_REFLOODING": False,
    }
    
    def __init__(self, node_id, queue):
        # init parent class and configure mixins
        super(Simple, self).__init__(node_id, queue)
        self._ProbabilisticForwardingMixin__configure(Simple.settings["PROBABILISTIC_FORWARDING_FRACTION"])
        
        # init own data structures
        self.reflood_timers = {}
        self.advertisement_routing_table = {}
        
        logger.info("%s router initialized..." % self.__class__.__name__)
    
    def _init_channel(self, channel):
        if not channel in self.advertisement_routing_table:
            self.advertisement_routing_table[channel] = {}
        if not channel in self.reflood_timers:
            self.reflood_timers[channel] = {}
    
    def stop(self):
        logger.warning("Stopping router!")
        super(Simple, self).stop()
        logger.warning("Router successfully stopped!");
    
    @functools.lru_cache(maxsize=4096)  # ~4 * settings["MAX_HASHES"] (power of 2 is faster)
    def _hash(self, nonce):
        digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
        digest.update(nonce)
        return digest.finalize()
    
    # this returns -1, 0 or 1 like compare functions used in sorting do
    @functools.lru_cache(maxsize=64)
    def _compare_nonces(self, nonce1, nonce2):
        if nonce1 == nonce2:
            return 0       # both paths are of equal length
        update1 = nonce1
        update2 = nonce2
        for i in range(1, Simple.settings["MAX_HASHES"] + 1):
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
            else:       # this nonce was already received (via another peer) --> this path can be equal to the already known shortest one or longer
                self.advertisement_routing_table[channel][chain][nonce].add(peer_id)
                if self.advertisement_routing_table[channel][chain].peekitem(0)[0] == nonce:
                    logger.debug("%s: Added %s nonce %s to chain %s" % (
                        channel, "equal", str(base64.b64encode(nonce), "ascii"), str(base64.b64encode(chain), "ascii")))
                    return ("equal", chain)   # hash chain of nonce was already in routing table and we now found an equal length path
            logger.debug("%s: Added %s nonce %s to chain %s" % (
                channel, "longer", str(base64.b64encode(nonce), "ascii"), str(base64.b64encode(chain), "ascii")))
            return ("longer", chain)    # hash chain of nonce was already in routing table and it remains the shortest path
        else:
            chain = nonce
            self.advertisement_routing_table[channel][chain] = SortedDict(functools.cmp_to_key(self._compare_nonces), {nonce: set([peer_id])})
            logger.debug("%s: Added %s nonce %s to chain %s" % (
                channel, "new", str(base64.b64encode(nonce), "ascii"), str(base64.b64encode(chain), "ascii")))
            return ("new", chain)        # hash chain of nonce was NOT already in routing table
    
    def __dump_state(self):
        # pretty printing for reflood_timers
        reflood_timers = {}
        for channel in self.reflood_timers:
            reflood_timers[channel] = {}
            for chain, timer_id in self.reflood_timers[channel].items():
                reflood_timers[channel][str(base64.b64encode(chain), "ascii")] = timer_id
        # pretty printing for advertisement_routing_table
        advertisement_routing_table = {}
        for channel in self.advertisement_routing_table:
            advertisement_routing_table[channel] = {}
            for chain in self.advertisement_routing_table[channel]:
                dict_contents = ["'%s': %s" % (str(base64.b64encode(nonce), "ascii"), str(self.advertisement_routing_table[channel][chain][nonce]))
                                 for nonce, peer_set in self.advertisement_routing_table[channel][chain].items()]
                advertisement_routing_table[channel][str(base64.b64encode(chain), "ascii")] = StrToClass("SortedDict(%s)" % ", ".join(dict_contents))
        
        return {
            "reflood_timers": reflood_timers,
            "advertisement_routing_table": advertisement_routing_table,
        }
    
    def _route_covert_data(self, msg, incoming_connection=None):
        # manage hashchain routing tables
        if msg.get_type().endswith("_init"):
            return self._route_init(msg, incoming_connection)
        elif msg.get_type().endswith("_advertise"):
            return self._route_advertise(msg, incoming_connection)
        elif msg.get_type().endswith("_unadvertise"):
            return self._route_unadvertise(msg, incoming_connection)
    
    def _route_advertise(self, advertisement, incoming_connection):
        logger.info("Routing advertisement: %s coming from %s..." % (str(advertisement), str(incoming_connection)))
        self._init_channel(advertisement["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        nonce = base64.b64decode(bytes(advertisement["nonce"], "ascii"))    # decode nonce
        chain = self._find_nonce(nonce, self.advertisement_routing_table[advertisement["channel"]])
        
        # prevent advertisement loop if this hashchain was already received from the same incoming_peer
        # hashchain known and already seen from this incoming peer --> abort flooding
        if chain and incoming_peer in set(
        peer_id for peer_set in self.advertisement_routing_table[advertisement["channel"]][chain].values() for peer_id in peer_set
        ):
            logger.debug("This hashchain was already received from exactly this peer, aborting advertise (re)flooding")
            return
        
        # now add nonce to routing table
        (ordering, chain) = self._add_nonce(advertisement["channel"], nonce, incoming_peer)
        
        # abort REflooding if we already know this hashchain
        if advertisement["reflood"] and ordering != "new":
            logger.debug("We already know this hashchain, arborting advertise REflooding...")
            return
        
        # abort flooding if we already know a shorter path
        if ordering == "longer" or ordering == "equal":
            logger.debug("This path was longer or equal, aborting advertise (re)flooding...")
            return
        
        # encode nonce again and update message with this new nonce
        nonce = self._hash(nonce)       # the path to our neighbor is one step further
        advertisement["nonce"] = str(base64.b64encode(nonce), "ascii")
        
        # route advertisement further to all peers but the incoming one
        for con in set(value for key, value in self.connections.items() if key != incoming_peer):
            logger.info("Routing advertisement for channel '%s' to %s..." % (str(advertisement["channel"]), str(con)))
            self._send_covert_msg(advertisement, con)
    
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
        logger.debug("Found chain '%s' for unadvertisement of channel '%s'..." % (str(base64.b64encode(chain), "ascii"), unadvertisement["channel"]))
        
        # remove ALL nonces of this hashchain pointing to the incoming peer and build an alternative peer list
        alternatives = set()
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
            logger.debug("We did not know a path to this incoming peer, aborting unadvertisement flooding...")
            return
        
        if None in alternatives:
            logger.debug("We are the subscriber for this hashchain, aborting unadvertisement flooding...")
            return
        
        # remove peers we don't have connections to from our alternatives list (we want to be on the safe side)
        alternatives.intersection_update(set(self.connections.keys()))
        
        # query reflooding in REFLOOD_DELAY seconds if we know alternatives and flood unadvertisements only to those peers we know alternative
        # paths from (so that they can remove us as alternative). if those peers don't know the unadvertised path the flooding will end there
        # only do this if the force flag is not set (force means this subscriber just unsubscribed)
        if len(alternatives) and not unadvertisement["force"]:
            logger.info("We know some alternative paths, only flooding unadvertisement to those peers...")
            for con in [con for peer_id, con in self.connections.items() if peer_id in alternatives]:
                logger.info("Routing unadvertisement for channel '%s' to %s..." % (str(unadvertisement["channel"]), str(con)))
                self._send_covert_msg(unadvertisement, con)
            # start reflood timer
            if chain in self.reflood_timers[unadvertisement["channel"]]:
                self._abort_timer(self.reflood_timers[unadvertisement["channel"]][chain])
            self.reflood_timers[unadvertisement["channel"]][chain] = self._add_timer(Simple.settings["REFLOOD_DELAY"], {
                "_command": "Simple__reflood",
                "channel": unadvertisement["channel"],
                "chain": chain,
            })
            return
        
        # route unadvertise to all peers but incoming one
        for con in set(value for key, value in self.connections.items() if key != incoming_peer):
            logger.info("Routing unadvertisement for channel '%s' to %s..." % (str(unadvertisement["channel"]), str(con)))
            self._send_covert_msg(unadvertisement, con)
    
    def _route_init(self, init, incoming_connection):
        logger.info("Routing init: %s coming from %s..." % (str(init), str(incoming_connection)))
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        
        # update own routing table to include remote entries
        for channel, noncelist in init["advertisements"].items():
            self._init_channel(channel)
            for nonce in noncelist:
                # fake an advertisement message incoming from connection "incoming_connection"
                self._route_covert_data(Message("%s_advertise" % self.__class__.__name__, {
                    "channel": channel,
                    "nonce": nonce,
                    "reflood": False if Simple.settings["AGGRESSIVE_REFLOODING"] else True,
                }), incoming_connection)
    
    def _route_data(self, msg, incoming_connection=None):
        logger.info("Routing data: %s coming from %s..." % (str(msg), str(incoming_connection)))
        self._init_channel(msg["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        
        if self._forward_data(msg):     # inform own subscribers of new data and determine if data is fresh
            logger.info("Data ID already seen, ignoring it and not routing further...")
            return
        
        # route it along the shortest path taken from advertisement_routing_table to the subscribers indicated by their nonces
        next_hops = {}      # map next hop node-ids to their chains
        for nonce in msg["nonces"]:
            nonce = base64.b64decode(bytes(nonce, "ascii"))    # decode nonce
            chain = self._find_nonce(nonce, self.advertisement_routing_table[msg["channel"]])
            if chain and len(self.advertisement_routing_table[msg["channel"]][chain]):
                # use first set item (one of possibly multiple next shortest hops)
                node_id = next(iter(self.advertisement_routing_table[msg["channel"]][chain].peekitem(0)[1]))
                if node_id == None:     # we are one of the subscribers the message was for --> no further forwarding for this chain
                    continue
                if node_id not in next_hops:
                    next_hops[node_id] = []
                next_hops[node_id].append(str(base64.b64encode(chain), "ascii"))
        
        # add probabilistic forwarding peers to also send message to
        for con in self._ProbabilisticForwardingMixin__get_additional_peers(msg["channel"], incoming_peer):
            node_id = con.get_peer_id()
            if node_id not in next_hops:
                next_hops[node_id] = []
        
        # route message further if needed
        for node_id, chains in next_hops.items():
            msg["nonces"]=chains    # update chain list the message is for
            logger.info("Routing data to %s..." % self.connections[node_id])
            self._send_msg(msg, self.connections[node_id])
        return
    
    def _add_connection_command(self, command):
        # call parent class for common tasks
        super(Simple, self)._add_connection_command(command)
        peer = command["connection"].get_peer_id()
        
        # anonymize routing tables (we only need a mapping of shortest nonces to channels), and hash + encode nonce for transmission
        advertisements = {channel:
                              [str(base64.b64encode(self._hash(list(noncelist)[0])), "ascii") for _, noncelist in entries.items() if len(noncelist)]
                          for channel, entries in self.advertisement_routing_table.items() if len(entries)}
        
        # exchange routing tables
        logger.info("Sending init message to newly connected peer at %s..." % str(command["connection"]))
        self._send_covert_msg(Message("%s_init" % self.__class__.__name__, {
            "advertisements": advertisements,
        }), command["connection"])
    
    def _remove_connection_command(self, command):
        # call parent class for common tasks
        super(Simple, self)._remove_connection_command(command)
        peer = command["connection"].get_peer_id()
        
        # extract all nonces of this peer for which we have to unadvertise paths
        for channel in self.advertisement_routing_table:
            unadvertise = set()
            for chain in self.advertisement_routing_table[channel]:
                for nonce in self.advertisement_routing_table[channel][chain]:
                    for node_id in self.advertisement_routing_table[channel][chain][nonce]:
                        if node_id == peer:
                            # anonymize nonce before adding to list (peers need to know only the corresponding hashchain, not the explicit nonce)
                            if Simple.settings["ANONYMOUS_IDS"]:
                                for x in range(SystemRandom().randint(0, 32)):
                                    nonce = self._hash(nonce)
                            unadvertise.add(nonce)        
            # unadvertise paths coming from this peer
            for nonce in unadvertise:
                self._route_covert_data(Message("%s_unadvertise" % self.__class__.__name__, {
                    "channel": channel,
                    "chain": str(base64.b64encode(nonce), "ascii"),
                    "force": False,
                }), command["connection"])
    
    def _publish_command(self, command):
        # call parent class for common tasks
        super(Simple, self)._publish_command(command)
        self._init_channel(command["channel"])
        
        nonces = [str(base64.b64encode(nonce), "ascii") for nonce in list(self.advertisement_routing_table[command["channel"]].keys())]
        # send data to randomly choosen master publisher (choosing a new one for every message reduces message loss if one master fails)
        self._route_data(Message("%s_data" % self.__class__.__name__, {
            "channel": command["channel"],
            "data": command["data"],
            "id": str(uuid.uuid4()),
            "nonces": nonces,
        }))
    
    def _subscribe_command(self, command):
        # initialize channel
        self._init_channel(command["channel"])
        
        # ignore already subscribed channels (only callback update)
        if command["channel"] in self.subscriptions:
            # call parent class for common tasks (update self.subscriptions)
            return super(Simple, self)._subscribe_command(command)
        
        self._route_covert_data(Message("%s_advertise" % self.__class__.__name__, {
            "channel": command["channel"],
            "nonce": str(base64.b64encode(os.urandom(32)), "ascii"),
            "reflood": False
        }))
        
        # call parent class for common tasks (update self.subscriptions)
        super(Simple, self)._subscribe_command(command)
        
        # create probabilistic forwarding tree
        self._ProbabilisticForwardingMixin__add_subtree(command["channel"])
        
    def _unsubscribe_command(self, command):
        # only do unsubscribe if needed
        if command["channel"] in self.subscriptions:
            # call parent class for common tasks
            super(Simple, self)._unsubscribe_command(command)
            self._init_channel(command["channel"])
            
            # unadvertise our nonces (should be only one in practice)
            chains = set()
            for chain in self.advertisement_routing_table[command["channel"]]:
                for entry in self.advertisement_routing_table[command["channel"]][chain].values():
                    if None in entry:
                        chains.add(str(base64.b64encode(chain), "ascii"))   # add chain to list
            for chain in chains:    # should be only one item in practice
                # send out unadvertise messages
                self._route_covert_data(Message("%s_unadvertise" % self.__class__.__name__, {
                    "channel": command["channel"],
                    "chain": chain,
                    "force": True,
                }))
    
    # *** the following commands are internal to Simple ***
    def __reflood_command(self, command):
        channel = command["channel"]
        chain = command["chain"]
        if chain in self.reflood_timers[channel]:
            del self.reflood_timers[channel][chain]
        
        if chain not in self.advertisement_routing_table[channel]:
            logger.info("Not REflooding for chain '%s' on channel '%s', no alternatives found..." % (str(base64.b64encode(chain), "ascii"), channel))
            return
        
        # calculate alternative paths
        alternatives = SortedList(key=functools.cmp_to_key(self._compare_nonces))
        for nonce in self.advertisement_routing_table[channel][chain]:
            alternatives.add(nonce)
        
        if not len(alternatives):
            logger.info("Not REflooding for chain '%s' on channel '%s', no alternatives found..." % (str(base64.b64encode(chain), "ascii"), channel))
            return
        
        nonce = alternatives[0]     # shortest known path
        
        # reflood advertisement message to all peers but the ones it advertises the path from
        for con in [con for peer_id, con in self.connections.items() if peer_id not in self.advertisement_routing_table[channel][chain][nonce]]:
            logger.info("Sending REflood advertisement for channel '%s' to peer at %s..." % (channel, str(con)))
            self._send_covert_msg(Message("%s_advertise" % self.__class__.__name__, {
                "channel": channel,
                "nonce": str(base64.b64encode(self._hash(nonce)), "ascii"),
                "reflood": True,        # this has to remain hardcoded at true
            }), con)
    