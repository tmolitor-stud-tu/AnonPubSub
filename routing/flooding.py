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
from .base import Router
from .active_paths_mixin import ActivePathsMixin
from .probabilistic_forwarding_mixin import ProbabilisticForwardingMixin


class StrToClass(object):
    def __init__(self, s):
        self.s = s
    def __repr__(self):
        return self.s

@final
@init_mixins
class Flooding(Router, ActivePathsMixin, ProbabilisticForwardingMixin):
    settings = {
        "ANONYMOUS_IDS": True,
        "MAX_HASHES": 1000,         # this limits the maximum diameter of the underlay to this value
        "REFLOOD_DELAY": 2.0,
        "RANDOM_MASTER_PUBLISH": False,
        "SUBSCRIBE_DELAY": 2.0,
        "AGGRESSIVE_TEARDOWN": False,
        "AGGRESSIVE_RESUBSCRIBE": False,
        "MIN_BECOME_MASTER_DELAY": 1.0,
        "MAX_BECOME_MASTER_DELAY": 8.0,
        "PROBABILISTIC_FORWARDING_FRACTION": 0.25,
    }
    
    def __init__(self, node_id, queue):
        # init parent class and configure mixins
        super(Flooding, self).__init__(node_id, queue)
        self._ActivePathsMixin__configure(Flooding.settings["AGGRESSIVE_TEARDOWN"])
        self._ProbabilisticForwardingMixin__configure(Flooding.settings["PROBABILISTIC_FORWARDING_FRACTION"])
        
        # init own data structures
        self.edge_version = 0
        self.publishing = set()
        self.master = {}
        self.become_master_timers = {}
        self.subscriber_ids = {}
        self.subscription_timers = {}
        self.reflood_timers = {}
        self.advertisement_routing_table = {}
        
        logger.info("%s router initialized..." % self.__class__.__name__)
    
    def _init_channel(self, channel):
        self._ActivePathsMixin__init_channel(channel)
        if not channel in self.advertisement_routing_table:
            self.advertisement_routing_table[channel] = {}
        if not channel in self.master:
            self.master[channel] = False
        if not channel in self.reflood_timers:
            self.reflood_timers[channel] = {}
        if not channel in self.subscription_timers:
            self.subscription_timers[channel] = {}
    
    def stop(self):
        logger.warning("Stopping router!")
        super(Flooding, self).stop()
        logger.warning("Router successfully stopped!");
    
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
    
    def _canonize_active_path_identifier(self, channel, identifier):
        identifier = base64.b64decode(bytes(identifier, "ascii"))
        publishers = set(base64.b64decode(bytes(entry, "ascii")) for entry in self._ActivePathsMixin__get_known_publishers(channel))
        retval = self._find_nonce(identifier, publishers)
        if not retval:
            retval = identifier
        return str(base64.b64encode(retval), "ascii")
    
    def _route_covert_data(self, msg, incoming_connection=None):
        # manage hashchain routing tables
        if msg.get_type().endswith("_init"):
            return self._route_init(msg, incoming_connection)
        elif msg.get_type().endswith("_advertise"):
            return self._route_advertise(msg, incoming_connection)
        elif msg.get_type().endswith("_unadvertise"):
            return self._route_unadvertise(msg, incoming_connection)
        # manage active paths
        elif msg.get_type().endswith("_error"):
            return self._route_error(msg, incoming_connection)
        elif msg.get_type().endswith("_unsubscribe"):
            return self._route_unsubscribe(msg, incoming_connection)
        elif msg.get_type().endswith("_teardown"):
            return self._route_teardown(msg, incoming_connection)
        elif msg.get_type().endswith("_subscribe"):
            return self._route_subscribe(msg, incoming_connection)
    
    def _route_subscribe(self, subscription, incoming_connection):
        logger.info("Routing subscription: %s coming from %s..." % (str(subscription), str(incoming_connection)))
        self._init_channel(subscription["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        chain = base64.b64decode(bytes(subscription["chain"], "ascii"))    # decode nonce
        
        # look up local hashchain identifier
        chain = self._find_nonce(chain, self.advertisement_routing_table[subscription["channel"]])
        
        # extract shortest paths if possible and randomly pick a next hop
        node_id = None
        if chain and len(self.advertisement_routing_table[subscription["channel"]][chain]):
            peer_set = self.advertisement_routing_table[subscription["channel"]][chain].peekitem(0)[1]
            
            # we are not the master (self.master is not set or shortest path doesn't point to us) --> calculate next hop
            if not self.master[subscription["channel"]] or None not in peer_set:
                if len(peer_set):
                    # filter peer set (only route to peers we are still connected to and don't route message back to the peer we received it from)
                    peer_set = set(node_id for node_id in peer_set if node_id != incoming_peer and node_id in self.connections)
                    # randomly pick one item of our filtered peer_set and return None if it is empty
                    node_id = next(iter(peer_set), None)
        
        self._ActivePathsMixin__activate_edge(
            subscription["channel"],
            subscription["subscriber"],
            # this is the publisher we are activating the edge for
            self._canonize_active_path_identifier(subscription["channel"], subscription["chain"]),
            subscription["version"],
            self.connections[node_id] if node_id else None,         # use None if the active path is starting here (we are the master publisher)
            incoming_connection)                                    # incoming_connection is where the active edge should point to
        
        if node_id:
            logger.info("Routing subscription to %s..." % self.connections[node_id])
            self._send_covert_msg(subscription, self.connections[node_id])
        elif not self.master[subscription["channel"]] or None not in peer_set:
            logger.warning("Could not find peer to route subscription to and we are not the master this message is for!")
        else:
            logger.info("We are the master the subscription was sent to, so we are not going to route it further...")
    
    def _route_error(self, error, incoming_connection):
        self._init_channel(error["channel"])
        
        # map publisher nonce to the identifier used by the ActivePathsMixin
        error["publisher"] = self._canonize_active_path_identifier(error["channel"], error["publisher"])
        
        def recreate_overlay(error):
            logger.warning("Recreating broken overlay for channel '%s' in %.3f seconds..." % (error["channel"], Flooding.settings["SUBSCRIBE_DELAY"]))
            chain = base64.b64decode(bytes(error["publisher"], "ascii"))
            chain = self._find_nonce(chain, self.advertisement_routing_table[error["channel"]])
            
            # send subscribe request if needed (make sure we wait the full SUBSCRIBE_DELAY seconds until we subscribe)
            if chain in self.subscription_timers[error["channel"]]:
                self._abort_timer(self.subscription_timers[error["channel"]][chain])
                del self.subscription_timers[error["channel"]][chain]
            self.subscription_timers[error["channel"]][chain] = self._add_timer(Flooding.settings["SUBSCRIBE_DELAY"], {
                "_command": "Flooding__create_overlay",
                "channel": error["channel"],
                "chain": chain
            })
        
        return self._ActivePathsMixin__route_error(error, incoming_connection, recreate_overlay)
    
    def _route_unsubscribe(self, unsubscribe, incoming_connection):
        self._init_channel(unsubscribe["channel"])
        return self._ActivePathsMixin__route_unsubscribe(unsubscribe, incoming_connection)
    
    def _route_teardown(self, teardown, incoming_connection):
        self._init_channel(teardown["channel"])
        
        # map publisher nonce to the identifier used by the ActivePathsMixin
        teardown["publisher"] = self._canonize_active_path_identifier(teardown["channel"], teardown["publisher"])

        return self._ActivePathsMixin__route_teardown(teardown, incoming_connection)
    
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
        
        def start_subscription_process():
            # send subscribe request if needed (make sure we wait the full SUBSCRIBE_DELAY seconds until we subscribe)
            if advertisement["channel"] in self.subscriptions:
                if chain in self.subscription_timers[advertisement["channel"]]:
                    self._abort_timer(self.subscription_timers[advertisement["channel"]][chain])
                    del self.subscription_timers[advertisement["channel"]][chain]
                logger.info("Trying to create overlay for channel '%s' in %.3f seconds..." % (str(advertisement["channel"]), Flooding.settings["SUBSCRIBE_DELAY"]))
                self.subscription_timers[advertisement["channel"]][chain] = self._add_timer(Flooding.settings["SUBSCRIBE_DELAY"], {
                    "_command": "Flooding__create_overlay",
                    "channel": advertisement["channel"],
                    "chain": chain
                })
        if Flooding.settings["AGGRESSIVE_RESUBSCRIBE"]:
            start_subscription_process()
        
        # abort REflooding if we already know this hashchain
        if advertisement["reflood"] and ordering != "new":
            logger.debug("We already know this hashchain, arborting advertise REflooding...")
            return
        
        # abort flooding if we already know a shorter path
        if ordering == "longer":
            logger.debug("This path was longer, aborting advertise (re)flooding...")
            return
        
        if not Flooding.settings["AGGRESSIVE_RESUBSCRIBE"]:
            start_subscription_process()
        
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
        logger.info("Found chain '%s' for unadvertisement of channel '%s'..." % (str(base64.b64encode(chain), "ascii"), unadvertisement["channel"]))
        
        # remove ALL nonces of this hashchain pointing to the incoming peer
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
            logger.debug("We are the master for this hashchain, aborting unadvertisement flooding...")
            return
        
        # remove peers we don't have connections to from our alternatives (we want to be on the safe side)
        alternatives.intersection_update(set(self.connections.keys()))
        
        # query reflooding in REFLOOD_DELAY seconds if we know alternatives and flood unadvertisements only to those peers we know alternative
        # paths from (so that they can remove us as alternative). if those peers don't know the unadvertised path the flooding will end there
        if len(alternatives):
            logger.info("We know some alternative paths, only flooding unadvertisement to those peers...")
            for con in [con for peer_id, con in self.connections.items() if peer_id in alternatives]:
                logger.info("Routing unadvertisement for channel '%s' to %s..." % (str(unadvertisement["channel"]), str(con)))
                self._send_covert_msg(unadvertisement, con)
            # start reflood timer
            if chain in self.reflood_timers[unadvertisement["channel"]]:
                self._abort_timer(self.reflood_timers[unadvertisement["channel"]][chain])
            self.reflood_timers[unadvertisement["channel"]][chain] = self._add_timer(Flooding.settings["REFLOOD_DELAY"], {
                "_command": "Flooding__reflood",
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
        
        # finally add our new peer to connections list
        if incoming_connection:
            self.connections[incoming_peer] = incoming_connection
        
        # update own routing table to include remote entries
        for channel, nonce in init["advertisements"].items():
            self._init_channel(channel)
            # fake an advertisement message incoming from connection "incoming_connection"
            self._route_covert_data(Message("%s_advertise" % self.__class__.__name__, {
                "channel": channel,
                "nonce": nonce,
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
            nonce = base64.b64decode(bytes(msg["nonce"], "ascii"))    # decode nonce
            chain = self._find_nonce(nonce, self.advertisement_routing_table[msg["channel"]])
            
            # extract shortest paths if possible
            peer_set = set()
            if chain and len(self.advertisement_routing_table[msg["channel"]][chain]):
                peer_set = self.advertisement_routing_table[msg["channel"]][chain].peekitem(0)[1]
            
            # we are the master (self.master is set and shortest path points to us) --> publish data on behalf of slave publisher
            if self.master[msg["channel"]] and None in peer_set:
                # send data to all subscribers (do not pass incoming_connection to _route_data() because we are publishing on behalf of a slave)
                self._route_data(Message("%s_data" % self.__class__.__name__, {
                    "channel": msg["channel"],
                    "data": msg["data"],
                }))
                # the publish message ends here
                return
            
            # route data further (we are not the master of this hashchain): select next hop based on shortest paths for this hashchain
            node_id = None
            if len(peer_set):
                # filter peer set (only route to peers we are still connected to and don't route message back to the peer we received it from)
                peer_set = set(node_id for node_id in peer_set if node_id != incoming_peer and node_id in self.connections)
                # randomly pick one item of our filtered peer_set and return None if it is empty
                node_id = next(iter(peer_set), None)
            if node_id:
                logger.info("Routing publish data to %s..." % self.connections[node_id])
                self._send_msg(msg, self.connections[node_id])
            else:
                logger.warning("Could not find peer to route message to while trying to route publish message to master publisher!")
            return
        
        # route normal data messages along active paths to subscribers

        if msg["channel"] in self.subscriptions:
            self.subscriptions[msg["channel"]](msg["data"])        # inform own subscriber of new data
        
        # calculate list of next nodes to route a (data) messages to according to the active edges (and don't add our incoming peer here)
        connections = self._ActivePathsMixin__get_next_hops(msg["channel"], incoming_peer)
        connections.update(self._ProbabilisticForwardingMixin__get_additional_peers(msg["channel"], incoming_peer))
        
        # sanity check
        if not len(connections):
            logger.debug("No peers with active edges found, cannot route data further!")
            return
        
        # route message to these nodes
        for con in connections:
            logger.info("Routing data to %s..." % str(con))
            self._send_msg(msg, con)
    
    def _add_connection_command(self, command):
        # no need to call parent class here, doing everything on our own (don't add new peer to self.connections here)
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
        # call parent class for common tasks
        super(Flooding, self)._remove_connection_command(command)
        peer = command["connection"].get_peer_id()
        
        # clean up all active paths using this peer (has to come first because of local hashchain lookup done in error routing)
        self._ActivePathsMixin__cleanup(command["connection"])
        
        # extract all nonces of this peer for which we have to unadvertise paths
        unadvertise = set()
        for channel in self.advertisement_routing_table:
            for chain in self.advertisement_routing_table[channel]:
                for nonce in self.advertisement_routing_table[channel][chain]:
                    for node_id in self.advertisement_routing_table[channel][chain][nonce]:
                        if node_id == peer:
                            # anonymize nonce before adding to list (peers need to know only the corresponding hashchain, not the explicit nonce)
                            if Flooding.settings["ANONYMOUS_IDS"]:
                                for x in range(SystemRandom().randint(0, 32)):
                                    nonce = self._hash(nonce)
                            unadvertise.add(nonce)
        
        # unadvertise paths coming from this peer
        for nonce in unadvertise:
            self._route_covert_data(Message("%s_unadvertise" % self.__class__.__name__, {
                "channel": channel,
                "chain": str(base64.b64encode(nonce), "ascii"),
            }), command["connection"])
    
    def _publish_command(self, command):
        self._init_channel(command["channel"])
        if command["channel"] not in self.publishing:
            self.publishing.add(command["channel"])
            if not len(self.advertisement_routing_table[command["channel"]]):
                if command["channel"] not in self.become_master_timers:     # only start timer once
                    delay = SystemRandom().uniform(Flooding.settings["MIN_BECOME_MASTER_DELAY"], Flooding.settings["MAX_BECOME_MASTER_DELAY"])
                    logger.info("Trying to become master publisher in %.3f seconds..." % delay)
                    self.become_master_timers[command["channel"]] = self._add_timer(delay, {
                        "_command": "Flooding__become_master",
                        "channel": command["channel"],
                    })
        
        # start sending out data
        if self.master[command["channel"]]:
            # send data to all subscribers
            self._route_data(Message("%s_data" % self.__class__.__name__, {
                "channel": command["channel"],
                "data": command["data"],
            }))
        elif not len(self.advertisement_routing_table[command["channel"]]):
            if command["channel"] not in self.become_master_timers:     # only start timer once
                delay = SystemRandom().uniform(Flooding.settings["MIN_BECOME_MASTER_DELAY"], Flooding.settings["MAX_BECOME_MASTER_DELAY"])
                logger.info("Trying to become master publisher in %.3f seconds..." % delay)
                self.become_master_timers[command["channel"]] = self._add_timer(delay, {
                    "_command": "Flooding___become_master",
                    "channel": command["channel"],
                })
        else:
            master = list(self.advertisement_routing_table[command["channel"]].keys())[0]
            if Flooding.settings["RANDOM_MASTER_PUBLISH"]:
                master = SystemRandom.choice(list(self.advertisement_routing_table[command["channel"]].keys()))
            # send data to randomly choosen master publisher (choosing a new one for every message reduces message loss if one master fails)
            self._route_data(Message("%s_publish" % self.__class__.__name__, {
                "channel": command["channel"],
                "data": command["data"],
                "nonce": str(base64.b64encode(master), "ascii"),
            }))
    
    def _subscribe_command(self, command):
        # initialize channel
        self._init_channel(command["channel"])
        
        # ignore already subscribed channels (only update the callback)
        if command["channel"] in self.subscriptions:
            # call parent class for common tasks (update self.subscriptions)
            return super(Flooding, self)._subscribe_command(command)
        
        # create new subscriber id for this channel if needed
        if command["channel"] not in self.subscriber_ids:
            self.subscriber_ids[command["channel"]] = str(uuid.uuid4()) if Flooding.settings["ANONYMOUS_IDS"] else self.node_id
        
        # subscribe to all masters for this channel
        for chain in self.advertisement_routing_table[command["channel"]]:
            # make sure we wait the full SUBSCRIBE_DELAY seconds until we subscribe to this master
            if chain in self.subscription_timers[command["channel"]]:
                self._abort_timer(self.subscription_timers[command["channel"]][chain])
                del self.subscription_timers[command["channel"]][chain]
            logger.info("Trying to create overlay for channel '%s' in %.3f seconds..." % (str(command["channel"]), Flooding.settings["SUBSCRIBE_DELAY"]))
            self.subscription_timers[command["channel"]][chain] = self._add_timer(Flooding.settings["SUBSCRIBE_DELAY"], {
                "_command": "Flooding__create_overlay",
                "channel": command["channel"],
                "chain": chain
            })
        
        # call parent class for common tasks (update self.subscriptions)
        super(Flooding, self)._subscribe_command(command)
    
    def _unsubscribe_command(self, command):
        # only do unsubscribe if needed
        if command["channel"] in self.subscriptions:
            # call parent class for common tasks
            super(Flooding, self)._unsubscribe_command(command)
            self._init_channel(command["channel"])
            
            # send out unsubscribe message to all masters we are subscribed to
            self._route_covert_data(Message("%s_unsubscribe" % self.__class__.__name__, {
                "channel": command["channel"],
                "subscriber": self.subscriber_ids[command["channel"]]
            }))
            
            # remove old subscriber id if not needed anymore
            if command["channel"] in self.subscriber_ids:
                del self.subscriber_ids[command["channel"]]
    
    def _dump_command(self, command):
        # pretty printing for advertisement_routing_table
        advertisement_routing_table = {}
        for channel in self.advertisement_routing_table:
            advertisement_routing_table[channel] = {}
            for chain in self.advertisement_routing_table[channel]:
                dict_contents = ["'%s': %s" % (str(base64.b64encode(nonce), "ascii"), str(self.advertisement_routing_table[channel][chain][nonce]))
                                 for nonce, peer_set in self.advertisement_routing_table[channel][chain].items()]
                advertisement_routing_table[channel][str(base64.b64encode(chain), "ascii")] = StrToClass("SortedDict(%s)" % ", ".join(dict_contents))
        
        state = {
            "publishing": self.publishing,
            "master": self.master,
            "subscriber_ids": self.subscriber_ids,
            "subscription_timers": self.subscription_timers,
            "advertisement_routing_table": advertisement_routing_table,
            "ActivePathsMixin": self._ActivePathsMixin__dump_state(),
        }
        logger.info("INTERNAL STATE:\n%s" % str(state))
        if command and "callback" in command and command["callback"]:
            command["callback"](state)
    
    # *** the following commands are internal to Flooding ***
    def __reflood_command(self, command):
        channel = command["channel"]
        chain = command["chain"]
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
                "reflood": True
            }), con)
    
    def __create_overlay_command(self, command):
        del self.subscription_timers[command["channel"]]
        
        # only create overlay if we (still) know a master publisher for this channel
        chain = self._find_nonce(command["chain"], self.advertisement_routing_table[command["channel"]])
        if not chain:
            logger.warning("NOT creating overlay for channel '%s', no known master publisher for this channel..." % str(command["channel"]))
            return
        
        if self._ActivePathsMixin__active_edges_present(command["channel"], self.subscriber_ids[command["channel"]]):
            logger.info("NOT creating overlay for channel '%s', overlay already created..." % str(command["channel"]))
            return
        
        logger.info("Creating overlay for channel '%s'..." % str(command["channel"]))
        
        # calculate edge version
        self.edge_version += 1
        
        # anonymize chain nonce before sending out subscribe message (peers need to know only the corresponding hashchain, not the explicit nonce)
        chain = command["chain"]
        if Flooding.settings["ANONYMOUS_IDS"]:
            for x in range(SystemRandom().randint(0, 32)):
                chain = self._hash(chain)
        
        # send out subscribe message
        self._route_covert_data(Message("%s_subscribe" % self.__class__.__name__, {
            "channel": command["channel"],  # this would be the encrypted topic if a TTP was used
            "subscriber": self.subscriber_ids[command["channel"]],
            "chain": str(base64.b64encode(chain), "ascii"),
            "version": self.edge_version
        }))
    
    def __become_master_command(self, command):
        del self.become_master_timers[command["channel"]]   # mark timer as fired so that it can be started again later
        
        # we are the first publisher --> flood underlay with advertisements (we are one of the masters now)
        if not len(self.advertisement_routing_table[command["channel"]]):
            logger.info("Now becoming new master publisher...")
            self.master[command["channel"]] = True
            self._route_covert_data(Message("%s_advertise" % self.__class__.__name__, {
                "channel": command["channel"],
                "nonce": str(base64.b64encode(os.urandom(32)), "ascii"),
                "reflood": False
            }))
        else:
            logger.info("Can not become master publisher, some other master was found meanwhile...")
