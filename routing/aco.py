import uuid
import math
import random
import numpy
from operator import itemgetter
import logging
logger = logging.getLogger(__name__)

# own classes
from networking import Message
from utils import init_mixins, final
from .router import Router
from .active_paths_mixin import ActivePathsMixin
from .probabilistic_forwarding_mixin import ProbabilisticForwardingMixin
from .cover_traffic_mixin import CoverTrafficMixin


@final
@init_mixins
class ACO(Router, ActivePathsMixin, ProbabilisticForwardingMixin):
    settings = {
        "ANONYMOUS_IDS": True,
        "ANT_COUNT": 5,
        "ACTIVATING_ANTS": 2,
        "EVAPORATION_TIME": 5,
        "EVAPORATION_FACTOR": 0.75,
        "DEFAULT_ROUNDS": 15,
        "ACTIVATION_ROUNDS": 5,
        "ANT_ROUND_TIME": 5,
        # maintain overlay every DEFAULT_ROUNDS times or zero, if no maintenance should be done
        #"ANT_MAINTENANCE_TIME": ACO.settings["DEFAULT_ROUNDS"] * ACO.settings["ANT_ROUND_TIME"],
        "ANT_MAINTENANCE_TIME": 0,
        "AGGRESSIVE_TEARDOWN": False,       # completely tear down old active paths (or path fragments) if a new active path is found and activated
        "PROBABILISTIC_FORWARDING_FRACTION": 0.25,      # fraction of neighbors to select for probabilistic forwarding
    }
    
    def __init__(self, node_id, queue):
        # init parent class and configure mixins
        super(ACO, self).__init__(node_id, queue)
        self._ActivePathsMixin__configure(ACO.settings["AGGRESSIVE_TEARDOWN"])
        self._ProbabilisticForwardingMixin__configure(ACO.settings["PROBABILISTIC_FORWARDING_FRACTION"])
        
        # init own data structures
        self.subscriber_ids = {}
        self.publisher_ids = {}
        self.pheromones = {}
        self.ant_versions = 0
        self.publishers_seen = {}
        self.overlay_creation_timers = {}
        self.overlay_maintenance_timers = {}
        self._add_timer(ACO.settings["EVAPORATION_TIME"], {"_command": "ACO__evaporation"})
        
        logger.info("%s router initialized..." % self.__class__.__name__)
    
    def _init_channel(self, channel):
        self._ActivePathsMixin__init_channel(channel)
        if channel not in self.pheromones:
            self.pheromones[channel] = {}
        if channel not in self.publishers_seen:
            self.publishers_seen[channel] = set()
    
    def stop(self):
        logger.warning("Stopping router!")
        super(ACO, self).stop()
        logger.warning("Router successfully stopped!");
    
    def _pheromone_choice(self, channel, population, strictness):
        if not len(population):     # no connections to choose
            return None
        
        # calculate raw value of pheromones (taking strictness value into account)
        weights = []
        for node_id in population:
            if node_id in self.pheromones[channel]:
                weights.append(self.pheromones[channel][node_id] * strictness)
            else:
                weights.append(0.0)
        
        # normalize weights
        #logger.debug("population: %s" % str(population))
        #logger.debug("weights: %s" % str(weights))
        weights_sum = math.fsum(weights)
        if weights_sum > 0.0:
            weights = [(float(i)/weights_sum)/2.0 for i in weights]   # normalize to 0.5
        else:
            weights = [0.5/len(weights) for _ in weights]           # dummy values (normalized to 0.5)
        #logger.debug("weights normalized to 0.5: %s" % str(weights))
        weights = [float(i)+(0.5/len(weights)) for i in weights]    # add 0.5/len(weights) (weights should now sum up to 1.0)
        #logger.debug("weights ready: %s" % str(weights))
        
        return numpy.random.choice(list(population.values()), p=weights, size=1)[0]
    
    def __dump_state(self):
        return {
            "subscriber_ids": self.subscriber_ids,
            "publisher_ids": self.publisher_ids,
            "pheromones": self.pheromones,
            "ant_versions": self.ant_versions,
            "publishers_seen": self.publishers_seen,
        }
    
    def _route_covert_data(self, msg, incoming_connection=None):
        if msg.get_type().endswith("_publish"):
            return self._route_publish(msg, incoming_connection)
        elif msg.get_type().endswith("_error"):
            return self._route_error(msg, incoming_connection)
        elif msg.get_type().endswith("_unsubscribe"):
            return self._route_unsubscribe(msg, incoming_connection)
        elif msg.get_type().endswith("_teardown"):
            return self._route_teardown(msg, incoming_connection)
        elif msg.get_type().endswith("_unpublish"):
            return self._route_unpublish(msg, incoming_connection)
        elif msg.get_type().endswith("_ant"):
            return self._route_ant(msg, incoming_connection)
    
    def _route_error(self, error, incoming_connection):
        def recreate_overlay(error):
            logger.warning("Recreating broken overlay for channel '%s' in %s seconds..." % (error["channel"], str(ACO.settings["ANT_ROUND_TIME"])))
            # abort all overlay creation or maintenance timers and restart overlay creation from scratch in ANT_ROUND_TIME seconds
            if error["channel"] in self.overlay_creation_timers:
                self._abort_timer(self.overlay_creation_timers[error["channel"]])
            if error["channel"] in self.overlay_maintenance_timers:
                self._abort_timer(self.overlay_maintenance_timers[error["channel"]])
            self.overlay_creation_timers[error["channel"]] = self._add_timer(ACO.settings["ANT_ROUND_TIME"], {
                "_command": "ACO__create_overlay",
                "channel": error["channel"],
                "round_count": 1,   # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
                "retry": 0
            })
        self._init_channel(error["channel"])
        subscriber_id = self.subscriber_ids[error["channel"]] if error["channel"] in self.subscriber_ids else None
        return self._ActivePathsMixin__route_error(error, subscriber_id, incoming_connection, recreate_overlay)
    
    def _route_unsubscribe(self, unsubscribe, incoming_connection):
        self._init_channel(unsubscribe["channel"])
        return self._ActivePathsMixin__route_unsubscribe(unsubscribe, incoming_connection)
    
    def _route_teardown(self, teardown, incoming_connection):
        self._init_channel(teardown["channel"])
        return self._ActivePathsMixin__route_teardown(teardown, incoming_connection)
    
    def _route_unpublish(self, unpublish, incoming_connection):
        self._init_channel(unpublish["channel"])
        return self._ActivePathsMixin__route_unpublish(unpublish, unpublish["channel"] in self.publishing, incoming_connection)
    
    def _route_publish(self, publish, incoming_connection):
        logger.info("Routing publish: %s coming from %s..." % (str(publish), str(incoming_connection)))
        self._init_channel(publish["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        # don't do anything if we have already seen this publish message
        if publish["publisher"] not in self.publishers_seen[publish["channel"]]:
            logger.info("New publisher %s for subscribed channel '%s' seen..." % (str(publish["publisher"]), str(publish["channel"])))
            self.publishers_seen[publish["channel"]].add(publish["publisher"])
            
            if publish["channel"] in self.subscriptions:
                # send out new ants if we didn't see this publisher before
                seen_before = publish["publisher"] in self.publishers_seen[publish["channel"]]
                if not seen_before:
                    logger.info("Recreating overlay for channel '%s' in %s seconds..." % (publish["channel"], str(ACO.settings["ANT_ROUND_TIME"])))
                    # abort all overlay creation or maintenance timers and restart overlay creation from scratch in ANT_ROUND_TIME seconds
                    if publish["channel"] in self.overlay_creation_timers:
                        self._abort_timer(self.overlay_creation_timers[publish["channel"]])
                    if publish["channel"] in self.overlay_maintenance_timers:
                        self._abort_timer(self.overlay_maintenance_timers[publish["channel"]])
                    self.overlay_creation_timers[publish["channel"]] = self._add_timer(ACO.settings["ANT_ROUND_TIME"], {
                        "_command": "ACO__create_overlay",
                        "channel": publish["channel"],
                        "round_count": 1,   # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
                        "retry": 0
                    })
            
            # flood publish message further (but not on incoming connection)
            for con in [con for node_id, con in self.connections.items() if node_id != incoming_peer]:
                logger.info("Routing publish to %s..." % str(con))
                self._send_covert_msg(publish, con)
    
    def _route_ant(self, ant, incoming_connection):
        logger.debug("Routing ant: %s coming from %s..." % (str(ant), str(incoming_connection)))
        self._init_channel(ant["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        # route searching ants
        if not ant["returning"]:
            searching_ant = Message(ant)  # clone ant for further searching to not influence ant returning
            searching_ant["ttl"] -= 1
            if searching_ant["ttl"] < 0:                 #ttl expired --> kill ant
                logger.debug("TTL expired, killing ant %s!" % str(searching_ant))
                return
            if self.node_id in searching_ant["path"]:     #loop detected --> kill ant
                logger.debug("Loop detected, killing ant %s!" % str(searching_ant))
                return
            searching_ant["path"].append(self.node_id)
            connections = {key: value for key, value in self.connections.items() if key != incoming_peer}
            con = self._pheromone_choice(searching_ant["channel"], connections, searching_ant["strictness"])
            if con:
                logger.debug("Sending out %s ant: %s to %s..." % ("activating" if ant["activating"] else "searching", str(searching_ant), str(con)))
                self._send_covert_msg(searching_ant, con)
            else:
                logger.debug("Cannot route %s ant, killing ant %s!" % ("activating" if ant["activating"] else "searching", str(searching_ant)))
            
            # let another ant return if we are a publisher for this channel (this duplicates the ant into one searching further and one returning)
            if ant["channel"] in self.publishing:
                ant["returning"] = True
                ant["ttl"] = float("inf")
                if ant["activating"]:       # activating ants carry the publisher id for which the path is activated
                    ant["publisher"] = self.publisher_ids[ant["channel"]]
                return self._route_covert_data(ant)    # route returning ant (originating here, thats why no incoming_connection is given)
        
        # route returning ants
        else:
            # update pheromones on edge to incoming node, serialize pheromones write access through command queue
            if incoming_connection:   # ant didn't start its way here --> put pheromones on incoming edge
                self._call_command({
                    "_command": "ACO__update_pheromones",
                    "channel": ant["channel"],
                    "node": incoming_peer,
                    "pheromones": ant["pheromones"]
                })
            
            # add publisher to the list of seen ones (this makes sure we don't start new ant rounds if we receive a publish message
            # from this publisher in the future but rather ignore that message)
            if ant["activating"] and ant["publisher"] not in self.publishers_seen[ant["channel"]]:
                self.publishers_seen[ant["channel"]].add(ant["publisher"])
            
            # get id of next node to route ant to or None, if the path ends here (or if we are not connected to this next node anymore)
            next_node = None
            returned = False
            if len(ant["path"]):
                next_node = ant["path"][-1]     # ants return on reverse path
                del ant["path"][-1]
                if next_node not in self.connections:
                    next_node = None
            else:
                returned = True
            
            # add new active edge of version ant["version"] if this ant is an activating one
            if ant["activating"]:
                self._ActivePathsMixin__activate_edge(ant["channel"], ant["subscriber"], ant["publisher"], ant["version"], incoming_connection, self.connections[next_node] if next_node else None)
            
            # some sanity checks
            if returned:
                logger.debug("Ant returned successfully, killing ant %s!" % str(ant))
                return                      # ant returned successfully --> kill it
            elif next_node is None:
                logger.warning("Node %s on reverse ant path NOT in current connection list, killing ant %s!" % (next_node, str(ant)))
                return
            
            logger.debug("Sending out returning ant: %s to %s..." % (str(ant), str(self.connections[next_node])))
            self._send_covert_msg(ant, self.connections[next_node])
    
    def _route_data(self, msg, incoming_connection=None):
        if incoming_connection:     # don't log locally published data (makes the log more clear)
            logger.info("Routing data: %s coming from %s..." % (str(msg), str(incoming_connection)))
        self._init_channel(msg["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        if self._forward_data(msg):     # inform own subscribers of new data and determine if data is fresh
            logger.info("Data ID already seen, ignoring it and not routing further...")
            return
        
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
    
    def _remove_connection_command(self, command):
        # call parent class for common tasks
        super(ACO, self)._remove_connection_command(command)
        peer = command["connection"].get_peer_id()
        
        # clean up pheromones afterwards
        for channel in self.pheromones:
            if peer in self.pheromones[channel]:
                del self.pheromones[channel][peer]
        
        # clean up all active paths using this peer
        self._ActivePathsMixin__cleanup(command["connection"])
    
    def _subscribe_command(self, command):
        # initialize channel
        self._init_channel(command["channel"])
        
        # ignore already subscribed channels (only update the callback)
        if command["channel"] in self.subscriptions:
            # call parent class for common tasks (update self.subscriptions)
            return super(ACO, self)._subscribe_command(command)
        
        # create new subscriber id for this channel if needed
        if command["channel"] not in self.subscriber_ids:
            self.subscriber_ids[command["channel"]] = str(uuid.uuid4()) if ACO.settings["ANONYMOUS_IDS"] else self.node_id
        
        logger.info("Creating overlay for channel '%s'..." % command["channel"])
        self._call_command({
            "_command": "ACO__create_overlay",
            "channel": command["channel"],
            "round_count": 1,   # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
            "retry": 0
        })
        
        # call parent class for common tasks (update self.subscriptions)
        super(ACO, self)._subscribe_command(command)
    
    def _unsubscribe_command(self, command):
        # only do unsubscribe if needed
        if command["channel"] in self.subscriptions:
            # call parent class for common tasks
            super(ACO, self)._unsubscribe_command(command)
            self._init_channel(command["channel"])
            
            self._route_covert_data(Message("%s_unsubscribe" % self.__class__.__name__, {
                "channel": command["channel"],
                "subscriber": self.subscriber_ids[command["channel"]]
            }))
            
            # remove old subscriber id if not needed anymore
            if command["channel"] in self.subscriber_ids:
                del self.subscriber_ids[command["channel"]]
    
    def _publish_command(self, command):
        # no need to call parent class here, doing everything on our own
        self._init_channel(command["channel"])
        
        # init publishing identity and flood publishing advertisement to inform potential subscribers
        if command["channel"] not in self.publishing:
            self.publisher_ids[command["channel"]] = str(uuid.uuid4()) if ACO.settings["ANONYMOUS_IDS"] else self.node_id
            self.publishing.add(command["channel"])
            self._route_covert_data(Message("%s_publish" % self.__class__.__name__, {
                "channel": command["channel"],
                "publisher": self.publisher_ids[command["channel"]]
            }))
        
        msg = Message("%s_data" % self.__class__.__name__, {
            "channel": command["channel"],
            "data": command["data"],
            "id": str(uuid.uuid4()),
        })
        self._route_data(msg)
    
    def _unpublish_command(self, command):
        # no need to call parent class here, doing everything on our own
        self._init_channel(command["channel"])
        
        # remove publisher identity and send out unsubscribe message to tear down all active paths to subscribers
        if command["channel"] in self.publishing:
            self._ActivePathsMixin__unpublish(command["channel"], self.publisher_ids[command["channel"]])
            del self.publisher_ids[command["channel"]]
            self.publishing.discard(command["channel"])
        
    # *** the following commands are internal to ACO ***
    def __update_pheromones_command(self, command):
        if command["channel"] not in self.pheromones:
            logger.error("Unknown channel '%s' while updating pheromones, skipping update!" % command["channel"])
        else:
            if command["node"] not in self.pheromones[command["channel"]]:
                self.pheromones[command["channel"]][command["node"]] = 0.0
            self.pheromones[command["channel"]][command["node"]] += command["pheromones"]
            logger.debug("Pheromones updated: %s" % str(self.pheromones))
    
    def __evaporation_command(self, command):
        for channel in self.pheromones:
            for node_id in self.pheromones[channel]:
                self.pheromones[channel][node_id] *= ACO.settings["EVAPORATION_FACTOR"]
        logger.debug("Pheromones evaporated: %s" % str(self.pheromones))
        self._add_timer(ACO.settings["EVAPORATION_TIME"], {"_command": "ACO__evaporation"})   # call this command again in EVAPORATION_TIME seconds
    
    def __create_overlay_command(self, command):
        # we are (re)creating the overlay from scratch, abort maintenance timer
        if command["channel"] in self.overlay_maintenance_timers:
            self._abort_timer(self.overlay_maintenance_timers[command["channel"]])
        
        active_edges_present = self._ActivePathsMixin__active_edges_present(command["channel"], self.subscriber_ids[command["channel"]])
        
        # loop as long as we didn't reach DEFAULT_ROUNDS (use <= to wait for last activating ants if DEFAULT_ROUNDS % ACTIVATION_ROUNDS == 0)
        if command["round_count"] <= ACO.settings["DEFAULT_ROUNDS"]:
            self._send_out_ants(command["channel"], command["round_count"])
            
            # call us again in ANT_ROUND_TIME seconds
            self.overlay_creation_timers[command["channel"]] = self._add_timer(ACO.settings["ANT_ROUND_TIME"], {
                "_command": "ACO__create_overlay",
                "channel": command["channel"],
                "round_count": command["round_count"] + 1,
                "retry": command["retry"]
            })
        else:
            if command["channel"] in self.overlay_creation_timers:
                del self.overlay_creation_timers[command["channel"]]
            if active_edges_present:
                if ACO.settings["ANT_MAINTENANCE_TIME"]:
                    logger.info("Overlay for channel '%s' created (maintenance will now run every %d seconds)..." % (command["channel"], ACO.settings["ANT_MAINTENANCE_TIME"]))
                else:
                    logger.info("Overlay for channel '%s' created (maintenance will *NOT* happen after this)..." % command["channel"])
            else:
                if ACO.settings["ANT_MAINTENANCE_TIME"]:
                    logger.info("Could not create overlay for channel '%s' (maintenance will now run every %d seconds)..." % (command["channel"], ACO.settings["ANT_MAINTENANCE_TIME"]))
                else:
                    logger.info("Could not create overlay for channel '%s' (maintenance will *NOT* happen after this)..." % command["channel"])
            if ACO.settings["ANT_MAINTENANCE_TIME"]:
                # call maintain overlay command in ANT_MAINTENANCE_TIME seconds
                self.overlay_maintenance_timers[command["channel"]] = self._add_timer(ACO.settings["ANT_MAINTENANCE_TIME"], {
                    "_command": "ACO__maintain_overlay",
                    "channel": command["channel"],
                    # NOTE: do fixed ttl values destroy the ant optimisation?
                    "ttl": ACO.settings["DEFAULT_ROUNDS"],      # bigger ttl to find new publishers (fixed to this value)
                    "round_count": 1    # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
                })
    
    def __maintain_overlay_command(self, command):
        if command["round_count"] <= ACO.settings["ACTIVATION_ROUNDS"]:
            # don't send out maintenance ants if overlay creation is in progress
            if command["channel"] in self.overlay_creation_timers:
                return
            
            self._send_out_ants(command["channel"], command["round_count"], command["ttl"])
            
            # call us again in ANT_ROUND_TIME seconds
            self.overlay_maintenance_timers[command["channel"]] = self._add_timer(ACO.settings["ANT_ROUND_TIME"], {
                "_command": "ACO__maintain_overlay",
                "channel": command["channel"],
                "ttl": ttl,
                "round_count": command["round_count"] + 1,
            })
        else:
            logger.info("Next overlay maintenance in %d seconds..." % ACO.settings["ANT_MAINTENANCE_TIME"])
            # call us again in ANT_MAINTENANCE_TIME seconds
            self.overlay_maintenance_timers[command["channel"]] = self._add_timer(ACO.settings["ANT_MAINTENANCE_TIME"], {
                "_command": "ACO__maintain_overlay",
                "channel": command["channel"],
                # NOTE: do fixed ttl values destroy the ant optimisation?
                "ttl": ACO.settings["DEFAULT_ROUNDS"],      # bigger ttl to find new publishers (fixed to this value)
                "round_count": 1    # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
            })
    
    def _send_out_ants(self, channel, round_count, ttl=6):
        logger.info("Channel '%s': Round %d: Sending out %d new %s ants..." % (
            channel,
            round_count,
            ACO.settings["ANT_COUNT"],
            "searching" if round_count % ACO.settings["ACTIVATION_ROUNDS"] else "activating"
        ))
        # send out ANT_COUNT ants
        for i in range(0, ACO.settings["ANT_COUNT"]):
            self.ant_versions += 1
            ant = Message("%s_ant" % self.__class__.__name__, {
                "id": str(uuid.uuid4()),
                # to be more private: don't send when not needed
                "version": self.ant_versions if round_count % ACO.settings["ACTIVATION_ROUNDS"] == 0 else 0,
                "channel": channel,
                "ttl": max(ttl, round_count),                   # default minimum ttl is 6
                "strictness": (random.random() * 20) % 20,      # strictness in [0, 20)
                "path": [self.node_id],
                "subscriber": self.subscriber_ids[channel],
                "publisher": None,                              # will be filled by publisher when an activating ant is returned
                "pheromones": 1.0,                              # pheromones to put on each edge (can be changed per ant here, if needed)
                "returning": False,
                # try to activate paths every ACTIVATION_ROUNDS rounds
                "activating": round_count % ACO.settings["ACTIVATION_ROUNDS"] == 0 and i < ACO.settings["ACTIVATING_ANTS"]
            })
            con = self._pheromone_choice(ant["channel"], self.connections, ant["strictness"])
            if con:
                logger.debug("Channel '%s': Round %d: Sending out new searching ant %s to %s..." % (ant["channel"], round_count, str(ant), str(con)))
                self._send_covert_msg(ant, con)
            else:
                logger.warning("Channel '%s': Round %d: Cannot send out new searching ant %s, killing ant!" % (ant["channel"], round_count, str(ant)))
