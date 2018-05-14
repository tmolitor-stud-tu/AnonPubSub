import uuid
import math
import random
import numpy
from operator import itemgetter
import logging
logger = logging.getLogger(__name__)

# own classes
from networking import Message
from .base import Router


class ACO(Router):
    settings = {
        "ANONYMOUS_IDS": True,
        "ANT_COUNT": 5,
        "EVAPORATION_TIME": 5,
        "EVAPORATION_FACTOR": 0.75,
        "DEFAULT_ROUNDS": 15,
        "ACTIVATION_ROUNDS": 5,
        "ANT_ROUND_TIME": 5,
        # maintain overlay every DEFAULT_ROUNDS times or zero, if no maintenance should be done
        #"ANT_MAINTENANCE_TIME": ACO.settings["DEFAULT_ROUNDS"] * ACO.settings["ANT_ROUND_TIME"],
        "ANT_MAINTENANCE_TIME": 0,
    }
    
    def __init__(self, node_id, queue):
        super(ACO, self).__init__(node_id, queue)
        self.pheromones = {}
        self.active_edges = {}
        # needed to send out error messages when needed and to know if overlay is established
        self.active_edges_to_publishers = {}
        self.publishing = set()
        self.subscriber_ids = {}
        self.publisher_ids = {}
        self.overlay_creation_timers = {}
        self.overlay_maintenance_timers = {}
        self.ant_versions = 0
        self.publishers_seen = {}
        self._add_timer(ACO.settings["EVAPORATION_TIME"], {"command": "ACO_evaporation"})
        logger.info("%s router initialized..." % self.__class__.__name__)
    
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
        logger.debug("population: %s" % str(population))
        logger.debug("weights: %s" % str(weights))
        weights_sum = math.fsum(weights)
        if weights_sum > 0.0:
            weights = [(float(i)/weights_sum)/2.0 for i in weights]   # normalize to 0.5
        else:
            weights = [0.5/len(weights) for _ in weights]           # dummy values (normalized to 0.5)
        logger.debug("weights normalized to 0.5: %s" % str(weights))
        weights = [float(i)+(0.5/len(weights)) for i in weights]    # add 0.5/len(weights) (weights should now sum up to 1.0)
        logger.debug("weights ready: %s" % str(weights))
        
        return numpy.random.choice(list(population.values()), p=weights, size=1)[0]
    
    def _route_covert_data(self, msg, incoming_connection=None):
        if msg.get_type().endswith("_publish"):
            return self._route_publish(msg, incoming_connection)
        elif msg.get_type().endswith("_error"):
            return self._route_error(msg, incoming_connection)
        elif msg.get_type().endswith("_unsubscribe"):
            return self._route_unsubscribe(msg, incoming_connection)
        elif msg.get_type().endswith("_teardown"):
            return self._route_teardown(msg, incoming_connection)
        elif msg.get_type().endswith("_ant"):
            return self._route_ant(msg, incoming_connection)
    
    def _route_publish(self, publish, incoming_connection):
        logger.info("Routing publish: %s coming from %s..." % (str(publish), str(incoming_connection)))
        self._init_channel(publish["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        # don't do anything if we have already seen this publish message
        if publish["publisher"] not in self.publishers_seen[publish["channel"]]:
            if publish["channel"] in self.subscriptions:
                logger.info("New publisher %s for subscribed channel '%s' seen..." % (str(publish["publisher"]), str(publish["channel"])))
                self.publishers_seen[publish["channel"]].add(publish["publisher"])
                
                # send out new ants if we didn't see this publisher before
                seen_before = False
                for subscriber in self.active_edges_to_publishers[publish["channel"]]:
                    if publish["publisher"] in self.active_edges_to_publishers[publish["channel"]][subscriber]:
                        seen_before = True
                if not seen_before:
                    logger.info("Recreating overlay for channel '%s' in %s seconds..." % (publish["channel"], str(ACO.settings["ANT_ROUND_TIME"])))
                    # abort all overlay creation or maintenance timers and restart overlay creation from scratch in ANT_ROUND_TIME seconds
                    if publish["channel"] in self.overlay_creation_timers:
                        self._abort_timer(self.overlay_creation_timers[publish["channel"]])
                    if publish["channel"] in self.overlay_maintenance_timers:
                        self._abort_timer(self.overlay_maintenance_timers[publish["channel"]])
                    self.overlay_creation_timers[publish["channel"]] = self._add_timer(ACO.settings["ANT_ROUND_TIME"], {
                        "command": "ACO_create_overlay",
                        "channel": publish["channel"],
                        "round_count": 1,   # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
                        "retry": 0
                    })
            
            # flood publish message further (but not on incoming connection)
            for con in [con for node_id, con in self.connections.items() if node_id != incoming_peer]:
                logger.info("Routing publish to %s..." % str(con))
                self._send_covert_msg(publish, con)
    
    def _route_teardown(self, teardown, incoming_connection):
        logger.info("Routing teardown: %s coming from %s..." % (str(teardown), str(incoming_connection)))
        self._init_channel(teardown["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        # get peer id of reverse active edge to this publisher for this subscriber and delete edge afterwards
        # but only if the edge version is lower or equal to the teardown version (checked only for messages not originating from here)
        node_id = None
        if teardown["subscriber"] in self.active_edges_to_publishers[teardown["channel"]]:
            if teardown["publisher"] in self.active_edges_to_publishers[teardown["channel"]][teardown["subscriber"]]:
                node_id = self.active_edges_to_publishers[teardown["channel"]][teardown["subscriber"]][teardown["publisher"]]["peer"]
                edge_version = self.active_edges_to_publishers[teardown["channel"]][teardown["subscriber"]][teardown["publisher"]]["version"]
                if incoming_connection and edge_version > teardown["version"]:
                    logger.warning("Teardown version (%d) lower than edge version (%d), not processing this outdated teardown!" % (teardown["version"], edge_version))
                    return      # ignore outdated teardown messages not originating from here
                del self.active_edges_to_publishers[teardown["channel"]][teardown["subscriber"]][teardown["publisher"]]
        
        # only handle teardown if we have a matching active edge
        # otherwise don't touch our active edge and don't route the teardown further
        if incoming_connection and (teardown["subscriber"] not in self.active_edges[teardown["channel"]] or
        self.active_edges[teardown["channel"]][teardown["subscriber"]] != incoming_peer):
            logger.info("Not routing teardown further because we have no matching active edge...")
            return
        
        # delete active edge to this subscriber if the teardown didn't originate here AND the reverse active edge
        # for this subscriber is the only one (or if there is no reverse active edge at all because the reverse path ends here).
        # if it is NOT the only one, we are a forwarder for another publisher and the active edge is still needed
        if incoming_connection and ((teardown["subscriber"] in self.active_edges_to_publishers[teardown["channel"]] and 
        len(self.active_edges_to_publishers[teardown["channel"]][teardown["subscriber"]]) <= 1) or
        teardown["subscriber"] not in self.active_edges_to_publishers[teardown["channel"]]):
            del self.active_edges[teardown["channel"]][teardown["subscriber"]]
        
        # route teardown message further along the reverse active edge to this publisher for this subscriber
        if node_id in self.connections:
            logger.info("Routing teardown to %s..." % str(self.connections[node_id]))
            self._send_covert_msg(teardown, self.connections[node_id])
    
    def _route_error(self, error, incoming_connection):
        logger.info("Routing error: %s coming from %s..." % (str(error), str(incoming_connection)))
        self._init_channel(error["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        # recreate broken overlay if needed (we subscribed the channel and incoming_peer is a reverse active edge)
        # edge versions are not relevant here
        if error["channel"] in self.subscriptions and error["subscriber"] == self.subscriber_ids[error["channel"]]:
            if incoming_peer in set(itemgetter("peer")(entry) for entry in
            self.active_edges_to_publishers[error["channel"]][error["subscriber"]].values()):
                logger.warning("Received error, tearing down broken path to publisher %s on channel '%s'..." % (str(error["publisher"]), str(error["channel"])))
                self._route_covert_data(Message("%s_teardown" % self.__class__.__name__, {
                    "channel": error["channel"],
                    "subscriber": error["subscriber"],
                    "publisher": error["publisher"],
                    "version": self.active_edges_to_publishers[error["channel"]][error["subscriber"]][error["publisher"]]["version"]
                }))
                logger.warning("Recreating broken overlay for channel '%s' in %s seconds..." % (error["channel"], str(ACO.settings["ANT_ROUND_TIME"])))
                # abort all overlay creation or maintenance timers and restart overlay creation from scratch in ANT_ROUND_TIME seconds
                if error["channel"] in self.overlay_creation_timers:
                    self._abort_timer(self.overlay_creation_timers[error["channel"]])
                if error["channel"] in self.overlay_maintenance_timers:
                    self._abort_timer(self.overlay_maintenance_timers[error["channel"]])
                self.overlay_creation_timers[error["channel"]] = self._add_timer(ACO.settings["ANT_ROUND_TIME"], {
                    "command": "ACO_create_overlay",
                    "channel": error["channel"],
                    "round_count": 1,   # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
                    "retry": 0
                })
        
        # route error message further along the active edge for this subscriber
        if error["subscriber"] in self.active_edges[error["channel"]]:
            node_id = self.active_edges[error["channel"]][error["subscriber"]]
            if node_id in self.connections:
                logger.info("Routing error to %s..." % str(self.connections[node_id]))
                self._send_covert_msg(error, self.connections[node_id])
    
    def _route_unsubscribe(self, unsubscribe, incoming_connection):
        logger.info("Routing unsubscribe: %s coming from %s..." % (str(unsubscribe), str(incoming_connection)))
        self._init_channel(unsubscribe["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        # route unsubscribe message along all reverse active edges to all publishers
        # edge versions not elevant here
        for node_id in set(itemgetter("peer")(entry) for entry in
        self.active_edges_to_publishers[unsubscribe["channel"]][unsubscribe["subscriber"]].values()):
            if node_id in self.connections:
                logger.info("Routing unsubscribe to %s..." % str(self.connections[node_id]))
                self._send_covert_msg(error, self.connections[node_id])
        
        # delete active edge to this subscriber
        if unsubscribe["subscriber"] in self.active_edges[unsubscribe["channel"]]:
            del self.active_edges[unsubscribe["channel"]][unsubscribe["subscriber"]]
        
        # delete all reverse active edges of this subscriber (it unsubscribed cleanly)
        # edge version not relevant here
        if unsubscribe["subscriber"] in self.active_edges_to_publishers[unsubscribe["channel"]]:
            del self.active_edges_to_publishers[unsubscribe["channel"]][unsubscribe["subscriber"]]
    
    def _route_ant(self, ant, incoming_connection):
        logger.debug("Routing ant: %s coming from %s..." % (str(ant), str(incoming_connection)))
        self._init_channel(ant["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        # route searching ants
        if not ant["returning"]:
            searching_ant = Message(ant)  # clone ant for further searching to not influence ant returning
            searching_ant["ttl"] -= 1
            if searching_ant["ttl"] < 0:                 #ttl expired --> kill ant
                logger.warning("TTL expired, killing ant %s!" % str(searching_ant))
                return
            if self.node_id in searching_ant["path"]:     #loop detected --> kill ant
                logger.warning("Loop detected, killing ant %s!" % str(searching_ant))
                return
            searching_ant["path"].append(self.node_id)
            connections = {key: value for key, value in self.connections.items() if value!=incoming_connection}
            con = self._pheromone_choice(searching_ant["channel"], connections, searching_ant["strictness"])
            if con:
                logger.debug("Sending out searching ant: %s to %s..." % (str(searching_ant), str(con)))
                self._send_covert_msg(searching_ant, con)
            else:
                logger.debug("Cannot route searching ant, killing ant %s!" % str(searching_ant))
            
            # let ant return if we are a publisher for this channel (this duplicates the ant into one searching further and one returning)
            if ant["channel"] in self.publishing:
                ant["returning"] = True
                ant["ttl"] = float("inf")
                if ant["activating"]:       # activating ants carry the publisher id for which the path is activated
                    ant["publisher"] = self.publisher_ids[ant["channel"]]
                return self._route_covert_data(ant)    # route returning ant (originating here, thats why no incoming_connection is given)
        
        # route returning ants
        else:
            # update pheromones on edge to incoming node, serialize pheromones write access through command queue
            if ant["channel"] not in self.publishing and incoming_connection:   # ant didn't start its way here --> put pheromones on incoming edge
                self._call_command({
                    "command": "ACO_update_pheromones",
                    "channel": ant["channel"],
                    "node": incoming_peer,
                    "pheromones": ant["pheromones"]
                })
            
            if ant["activating"] and incoming_connection:
                if ant["subscriber"] not in self.active_edges_to_publishers[ant["channel"]]:
                    self.active_edges_to_publishers[ant["channel"]][ant["subscriber"]] = {}
                if ant["publisher"] not in self.active_edges_to_publishers[ant["channel"]][ant["subscriber"]]:
                    logger.info("Activation for channel '%s' incoming from publisher '%s' via %s..." % (
                        str(ant["channel"]),
                        str(ant["publisher"]),
                        str(incoming_connection)
                    ))
                elif incoming_peer != self.active_edges_to_publishers[ant["channel"]][ant["subscriber"]][ant["publisher"]]["peer"]:
                    logger.warning("New path activation for channel '%s' incoming from publisher '%s' via %s..." % (
                        str(ant["channel"]),
                        str(ant["publisher"]),
                        str(incoming_connection)
                    ))
                    self._route_covert_data(Message("%s_teardown" % self.__class__.__name__, {
                        "channel": ant["channel"],
                        "subscriber": ant["subscriber"],
                        "publisher": ant["publisher"],
                        "version": self.active_edges_to_publishers[ant["channel"]][ant["subscriber"]][ant["publisher"]]["version"]
                    }))
                # don't decrement edge versions
                if (ant["publisher"] not in self.active_edges_to_publishers[ant["channel"]][ant["subscriber"]] or
                ant["version"] >= self.active_edges_to_publishers[ant["channel"]][ant["subscriber"]][ant["publisher"]]["version"]):
                    self.active_edges_to_publishers[ant["channel"]][ant["subscriber"]][ant["publisher"]] = {
                        "version": ant["version"],
                        "peer": incoming_peer
                    }
            
            if not len(ant["path"]):
                logger.debug("Ant returned successfully, killing ant %s!" % str(ant))
                return                      # ant returned successfully --> kill it
            
            # get id of next node to route ant to
            next_node = ant["path"][-1]     # ants return on reverse path
            del ant["path"][-1]
            if next_node not in self.connections:
                logger.warning("Node %s on reverse ant path NOT in current connection list, killing ant %s!" % (next_node, str(ant)))
                return
            
            if ant["activating"]:
                if next_node not in self.active_edges[ant["channel"]].values():
                    logger.info("Activating edge to %s for channel '%s'..." % (str(self.connections[next_node]), str(ant["channel"])))
                self.active_edges[ant["channel"]][ant["subscriber"]] = next_node
            
            logger.debug("Sending out returning ant: %s to %s..." % (str(ant), str(self.connections[next_node])))
            self._send_covert_msg(ant, self.connections[next_node])
    
    def _route_data(self, msg, incoming_connection=None):
        logger.info("Routing data: %s coming from %s..." % (str(msg), str(incoming_connection)))
        self._init_channel(msg["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        if msg["channel"] in self.subscriptions:
            self.subscriptions[msg["channel"]](msg["data"])        #inform own subscriber of new data
        
        # calculate next nodes to route messages to and ignore incoming connection
        connections = [con for node_id, con in self.connections.items() if node_id in self.active_edges[msg["channel"]].values() and node_id != incoming_peer]
        
        # sanity check
        if not len(connections):
            logger.debug("No peers with active edges found, cannot route data further!")
            logger.debug("Pheromones: %s" % str(self.pheromones[msg["channel"]]))
            logger.debug("Active edges: %s" % str(self.active_edges[msg["channel"]]))
            return
        
        # route message to these nodes
        for con in connections:
            logger.info("Routing data to %s..." % str(con))
            self._send_msg(msg, con)
    
    def _init_channel(self, channel):
        if channel not in self.pheromones:
            self.pheromones[channel] = {}
        if channel not in self.active_edges:
            self.active_edges[channel] = {}
        if channel not in self.active_edges_to_publishers:
            self.active_edges_to_publishers[channel] = {}
        if channel not in self.publishers_seen:
            self.publishers_seen[channel] = set()
    
    def _remove_connection_command(self, command):
        # call parent class for common tasks
        super(ACO, self)._remove_connection_command(command)
        peer = command["connection"].get_peer_id()
        
        # clean up pheromones afterwards
        for channel in self.pheromones:
            if peer in self.pheromones[channel]:
                del self.pheromones[channel][peer]
        
        # inform affected subscribers of broken path, so they can reestablish the overlay
        for channel in self.active_edges_to_publishers:
            for subscriber in self.active_edges_to_publishers[channel]:
                for publisher in list(self.active_edges_to_publishers[channel][subscriber].keys()):
                    if peer == self.active_edges_to_publishers[channel][subscriber][publisher]["peer"]:
                        self._route_covert_data(Message("%s_error" % self.__class__.__name__, {
                            "channel": channel,
                            "subscriber": subscriber,
                            "publisher": publisher
                        }), command["connection"])
        
        # teardown broken path to publishers alsong the reverse active edges if the corresponding active edge is broken
        for channel in self.active_edges:
            for subscriber in list(self.active_edges[channel].keys()):
                if self.active_edges[channel][subscriber] == peer and subscriber in self.active_edges_to_publishers[channel]:
                    for publisher in list(self.active_edges_to_publishers[channel][subscriber].keys()):
                        self._route_covert_data(Message("%s_teardown" % self.__class__.__name__, {
                            "channel": channel,
                            "subscriber": subscriber,
                            "publisher": publisher,
                            "version": self.active_edges_to_publishers[channel][subscriber][publisher]["version"]
                        }))
        
        # clean up active edges to subscribers
        for channel in self.active_edges:
            for subscriber in list(self.active_edges[channel].keys()):
                if self.active_edges[channel][subscriber] == peer:
                    del self.active_edges[channel][subscriber]
        
        # clean up reverse active edges to publishers
        for channel in self.active_edges_to_publishers:
            for subscriber in self.active_edges_to_publishers[channel]:
                for publisher in list(self.active_edges_to_publishers[channel][subscriber].keys()):
                    if self.active_edges_to_publishers[channel][subscriber][publisher]["peer"] == peer:
                        del self.active_edges_to_publishers[channel][subscriber][publisher]
    
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
            "command": "ACO_create_overlay",
            "channel": command["channel"],
            "round_count": 1,   # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
            "retry": 0
        })
        
        # call parent class for common tasks (update self.subscriptions)
        super(ACO, self)._subscribe_command(command)
    
    def _unsubscribe_command(self, command):
        # call parent class for common tasks
        super(ACO, self)._unsubscribe_command(command)
        self._init_channel(command["channel"])
        
        self._send_covert_msg(Message("%s_unsubscribe" % self.__class__.__name__, {
            "channel": command["channel"],
            "subscriber": self.subscriber_ids[command["channel"]]
        }))
        
        # remove old subscriber id if not needed anymore
        if command["channel"] not in self.subscriber_ids:
            del self.subscriber_ids[command["channel"]]
    
    def _publish_command(self, command):
        # no need to call parent class here, doing everything on our own
        self._init_channel(command["channel"])
        
        # init publishing identity and flood publishing advertisement to inform potential subscribers if automatic maintenance is deactivated
        if command["channel"] not in self.publishing:
            self.publisher_ids[command["channel"]] = str(uuid.uuid4()) if ACO.settings["ANONYMOUS_IDS"] else self.node_id
            self.publishing.add(command["channel"])
            if not ACO.settings["ANT_MAINTENANCE_TIME"]:
                self._route_covert_data(Message("%s_publish" % self.__class__.__name__, {
                    "channel": command["channel"],
                    "publisher": self.publisher_ids[command["channel"]]
                }))
        
        msg = Message("%s_data" % self.__class__.__name__, {
            "channel": command["channel"],
            "data": command["data"]
        })
        self._route_data(msg)
    
    # *** the following commands are internal to ACO ***
    def _ACO_update_pheromones_command(self, command):
        if command["channel"] not in self.pheromones:
            logger.error("Unknown channel '%s' while updating pheromones, skipping update!" % command["channel"])
        else:
            if command["node"] not in self.pheromones[command["channel"]]:
                self.pheromones[command["channel"]][command["node"]] = 0.0
            self.pheromones[command["channel"]][command["node"]] += command["pheromones"]
            logger.debug("Pheromones updated: %s" % str(self.pheromones))
    
    def _ACO_evaporation_command(self, command):
        for channel in self.pheromones:
            for node_id in self.pheromones[channel]:
                self.pheromones[channel][node_id] *= ACO.settings["EVAPORATION_FACTOR"]
        logger.debug("Pheromones evaporated: %s" % str(self.pheromones))
        self._add_timer(ACO.settings["EVAPORATION_TIME"], {"command": "ACO_evaporation"})   # call this command again in EVAPORATION_TIME seconds
    
    def _ACO_create_overlay_command(self, command):
        # we are (re)creating the overlay from scratch, abort maintenance timer
        if command["channel"] in self.overlay_maintenance_timers:
            self._abort_timer(self.overlay_maintenance_timers[command["channel"]])
        
        active_edges_present = (self.subscriber_ids[command["channel"]] in self.active_edges_to_publishers[command["channel"]] and
        len(set(itemgetter("peer")(entry) for entry in self.active_edges_to_publishers[command["channel"]][self.subscriber_ids[command["channel"]]].values())))
        
        # loop as long as we didn't reach DEFAULT_ROUNDS (use <= to wait for last activating ants if DEFAULT_ROUNDS % ACTIVATION_ROUNDS == 0)
        if command["round_count"] <= ACO.settings["DEFAULT_ROUNDS"]:
            self._send_out_ants(command["channel"], command["round_count"])
            
            # call us again in ANT_ROUND_TIME seconds
            self.overlay_creation_timers[command["channel"]] = self._add_timer(ACO.settings["ANT_ROUND_TIME"], {
                "command": "ACO_create_overlay",
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
                    "command": "ACO_maintain_overlay",
                    "channel": command["channel"],
                    # TODO: do fixed ttl values destroy the ant optimisation
                    "ttl": ACO.settings["DEFAULT_ROUNDS"],      # bigger ttl to find new publishers (fixed to this value)
                    "round_count": 1    # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
                })
    
    def _ACO_maintain_overlay_command(self, command):
        if command["round_count"] <= ACO.settings["ACTIVATION_ROUNDS"]:
            # don't send out maintenance ants if overlay creation is in progress
            if command["channel"] in self.overlay_creation_timers:
                return
            
            self._send_out_ants(command["channel"], command["round_count"], command["ttl"])
            
            # call us again in ANT_ROUND_TIME seconds
            self.overlay_maintenance_timers[command["channel"]] = self._add_timer(ACO.settings["ANT_ROUND_TIME"], {
                "command": "ACO_maintain_overlay",
                "channel": command["channel"],
                "ttl": ttl,
                "round_count": command["round_count"] + 1,
            })
        else:
            logger.info("Next overlay maintenance in %d seconds..." % ACO.settings["ANT_MAINTENANCE_TIME"])
            # call us again in ANT_MAINTENANCE_TIME seconds
            self.overlay_maintenance_timers[command["channel"]] = self._add_timer(ACO.settings["ANT_MAINTENANCE_TIME"], {
                "command": "ACO_maintain_overlay",
                "channel": command["channel"],
                # TODO: do fixed ttl values destroy the ant optimisation
                "ttl": ACO.settings["DEFAULT_ROUNDS"],      # bigger ttl to find new publishers (fixed to this value)
                "round_count": 1    # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
            })
    
    def _send_out_ants(self, channel, round_count, ttl=6):
        if channel not in self.subscriber_ids:
            self.subscriber_ids[channel] = str(uuid.uuid4()) if ACO.settings["ANONYMOUS_IDS"] else self.node_id
        logger.info("Channel '%s': Round %d: Sending out %d new searching ants..." % (channel, round_count, ACO.settings["ANT_COUNT"]))
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
                "activating": round_count % ACO.settings["ACTIVATION_ROUNDS"] == 0
            })
            con = self._pheromone_choice(ant["channel"], self.connections, ant["strictness"])
            if con:
                logger.debug("Channel '%s': Round %d: Sending out new searching ant %s to %s..." % (ant["channel"], round_count, str(ant), str(con)))
                self._send_covert_msg(ant, con)
            else:
                logger.warning("Channel '%s': Round %d: Cannot send out new searching ant %s, killing ant!" % (ant["channel"], round_count, str(ant)))
