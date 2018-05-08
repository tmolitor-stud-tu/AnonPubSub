import uuid
import math
import random
import numpy
import logging
logger = logging.getLogger(__name__)

# own classes
from networking import Message
from .base import Router


# some constants
ANT_COUNT = 5
EVAPORATION_TIME = 5
EVAPORATION_FACTOR = 0.75
DEFAULT_ROUNDS = 15
MAX_ROUNDS = 50
ACTIVATION_ROUNDS = 5
ANT_ROUND_TIME = 5
ANT_MAINTENANCE_TIME = DEFAULT_ROUNDS * ANT_ROUND_TIME  # maintain overlay every DEFAULT_ROUNDS time
RETRY_TIME = 60
MAX_RETRY_TIME = 3 * ANT_MAINTENANCE_TIME   # upper limit of exponential backoff based on RETRY_TIME)

class ACO(Router):
    
    def __init__(self, node_id, queue):
        super(ACO, self).__init__(node_id, queue)
        self.pheromones = {}
        self.active_edges = {}
        # needed to send out error messages when needed and to know if overlay is established
        self.active_edges_to_publishers = {}
        self.publishing = set()
        self.overlay_creation_timers = {}
        self.overlay_maintenance_timers = {}
        self._add_timer(EVAPORATION_TIME, {"command": "ACO_evaporation"})
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
        if msg.get_type().endswith("_error"):
            return self._route_error(msg, incoming_connection)
        elif msg.get_type().endswith("_unsubscribe"):
            return self._route_unsubscribe(msg, incoming_connection)
        elif msg.get_type().endswith("_ant"):
            return self._route_ant(msg, incoming_connection)
    
    def _route_error(self, error, incoming_connection):
        logger.info("Routing error: %s coming from %s..." % (str(error), str(incoming_connection)))
        self._init_channel(error["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        # recreate broken overlay if needed (incoming_peer is a reverse active edge)
        if incoming_peer in self.active_edges_to_publishers[error["channel"]][error["subscriber"]]:
            if error["channel"] in self.subscriptions:
                logger.warning("Recreating broken overlay for channel '%s' in %s seconds..." % (error["channel"], str(ANT_ROUND_TIME)))
                # abort all overlay creation or maintenance timers and start overlay creation from scratch in ANT_ROUND_TIME seconds
                if error["channel"] in self.overlay_creation_timers:
                    self._abort_timer(self.overlay_creation_timers[error["channel"]])
                if error["channel"] in self.overlay_maintenance_timers:
                    self._abort_timer(self.overlay_maintenance_timers[error["channel"]])
                self.overlay_creation_timers[error["channel"]] = self._add_timer(ANT_ROUND_TIME, {
                    "command": "ACO_create_overlay",
                    "channel": error["channel"],
                    "round_count": 1,   # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
                    "retry": 0
                })
        
        if error["subscriber"] in self.active_edges[error["channel"]]:
            node_id = self.active_edges[error["channel"]][error["subscriber"]]
            if node_id in self.connections:
                logger.info("Routing error to %s..." % str(self.connections[node_id]))
                self._send_covert_msg(error, self.connections[node_id])
    
    def _route_unsubscribe(self, unsubscribe, incoming_connection):
        logger.info("Routing unsubscribe: %s coming from %s..." % (str(unsubscribe), str(incoming_connection)))
        self._init_channel(unsubscribe["channel"])
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection else None
        
        # route message along all reverse active edges to all publishers
        for node_id in self.active_edges_to_publishers[unsubscribe["channel"]][unsubscribe["subscriber"]]:
            if node_id in self.connections:
                logger.info("Routing unsubscribe to %s..." % str(self.connections[node_id]))
                self._send_covert_msg(error, self.connections[node_id])
        
        # delete active edge to this subscriber
        if unsubscribe["subscriber"] in self.active_edges[unsubscribe["channel"]]:
            del self.active_edges[unsubscribe["channel"]][unsubscribe["subscriber"]]
        
        # delete all reverse active edges of this subscriber (it unsubscribed cleanly)
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
                    self.active_edges_to_publishers[ant["channel"]][ant["subscriber"]] = set()
                if incoming_peer not in self.active_edges_to_publishers[ant["channel"]][ant["subscriber"]]:
                    logger.info("Activation for channel '%s' incoming from %s..." % (str(ant["channel"]), str(incoming_connection)))
                    self.active_edges_to_publishers[ant["channel"]][ant["subscriber"]].add(incoming_peer)
            
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
        if not channel in self.pheromones:
            self.pheromones[channel] = {}
        if not channel in self.active_edges:
            self.active_edges[channel] = {}
        if not channel in self.active_edges_to_publishers:
            self.active_edges_to_publishers[channel] = {}
    
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
                if peer in self.active_edges_to_publishers[channel][subscriber]:
                    self._route_covert_data(Message("%s_error" % self.__class__.__name__, {
                        "channel": channel,
                        "subscriber": subscriber
                    }), command["connection"])
        
        # clean up active edges to subscribers
        for channel in self.active_edges:
            for subscriber in list(self.active_edges[channel].keys()):
                if self.active_edges[channel][subscriber] == peer:
                    del self.active_edges[channel][subscriber]
        
        # clean up reverse active edges to publishers
        for channel in self.active_edges_to_publishers:
            for subscriber in self.active_edges_to_publishers[channel]:
                self.active_edges_to_publishers[channel][subscriber].discard(peer)
    
    def _subscribe_command(self, command):
        # call parent class for common tasks
        super(ACO, self)._subscribe_command(command)
        self._init_channel(command["channel"])
        
        logger.info("Creating overlay for channel '%s'..." % command["channel"])
        self._call_command({
            "command": "ACO_create_overlay",
            "channel": command["channel"],
            "round_count": 1,   # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
            "retry": 0
        })
    
    def _unsubscribe_command(self, command):
        # call parent class for common tasks
        super(ACO, self)._unsubscribe_command(command)
        self._init_channel(command["channel"])
        
        self._send_covert_msg(Message("%s_unsubscribe" % self.__class__.__name__, {
            "channel": command["channel"],
            "subscriber": self.node_id
        }))
    
    def _publish_command(self, command):
        # no need to call parent class here, doing everything on our own
        self._init_channel(command["channel"])
        self.publishing.add(command["channel"])
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
                self.pheromones[channel][node_id] *= EVAPORATION_FACTOR
        logger.debug("Pheromones evaporated: %s" % str(self.pheromones))
        self._add_timer(EVAPORATION_TIME, {"command": "ACO_evaporation"})   # call this command again in EVAPORATION_TIME seconds
    
    def _ACO_create_overlay_command(self, command):
        # we are (re)creating the overlay from scratch, abort maintenance timer
        if command["channel"] in self.overlay_maintenance_timers:
            self._abort_timer(self.overlay_maintenance_timers[command["channel"]])
        
        # loop as long as we didn't reach DEFAULT_ROUNDS or even further if still no active edges are found
        if (
            # loop as long as we didn't reach DEFAULT_ROUNDS (use <= to wait for last activating ants if DEFAULT_ROUNDS % ACTIVATION_ROUNDS == 0)
            command["round_count"] <= DEFAULT_ROUNDS or
            # or even further if...
            (
                # ...we still have no active edges...
                (
                    self.node_id not in self.active_edges_to_publishers[command["channel"]] or
                    not len(self.active_edges_to_publishers[command["channel"]][self.node_id])
                ) and
                # ...and didn't reach MAX_ROUNDS (use <= to wait for last activating ants if MAX_ROUNDS % ACTIVATION_ROUNDS == 0)
                command["round_count"] <= MAX_ROUNDS
            )
        ):
            self._send_out_ants(command["channel"], command["round_count"])
            
            # call us again in ANT_ROUND_TIME seconds
            self.overlay_creation_timers[command["channel"]] = self._add_timer(ANT_ROUND_TIME, {
                "command": "ACO_create_overlay",
                "channel": command["channel"],
                "round_count": command["round_count"] + 1,
                "retry": command["retry"]
            })
        else:
            if not (
                    self.node_id not in self.active_edges_to_publishers[command["channel"]] or
                    not len(self.active_edges_to_publishers[command["channel"]][self.node_id])
            ):
                logger.info("Overlay for channel '%s' created (maintenance will now run every %d seconds)..." % (command["channel"], ANT_MAINTENANCE_TIME))
                if command["channel"] in self.overlay_creation_timers:
                    del self.overlay_creation_timers[command["channel"]]
                
                # call maintain overlay command in ANT_MAINTENANCE_TIME seconds
                self.overlay_maintenance_timers[command["channel"]] = self._add_timer(ANT_MAINTENANCE_TIME, {
                    "command": "ACO_maintain_overlay",
                    "channel": command["channel"],
                    "round_count": 1    # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
                })
            else:
                retry_time = min(MAX_RETRY_TIME, RETRY_TIME * (2**command["retry"]))
                logger.error("Could not create overlay for channel '%s', retrying in %s seconds..." % (command["channel"], str(retry_time)))
                # wait some time and try again (exponential backoff)
                self.overlay_creation_timers[command["channel"]] = self._add_timer(retry_time, {
                    "command": "ACO_create_overlay",
                    "channel": command["channel"],
                    "round_count": 1,   # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
                    "retry": command["retry"] + 1
                })
    
    def _ACO_maintain_overlay_command(self, command):
        if command["round_count"] <= ACTIVATION_ROUNDS:
            # don't send out maintenance ants if overlay creation is in progress
            if command["channel"] in self.overlay_creation_timers:
                return
            
            self._send_out_ants(command["channel"], command["round_count"])
            
            # call us again in ANT_ROUND_TIME seconds
            self.overlay_maintenance_timers[command["channel"]] = self._add_timer(ANT_ROUND_TIME, {
                "command": "ACO_maintain_overlay",
                "channel": command["channel"],
                "round_count": command["round_count"] + 1,
            })
        else:
            logger.info("Next overlay maintenance in %d seconds..." % ANT_MAINTENANCE_TIME)
            # call us again in ANT_MAINTENANCE_TIME seconds
            self.overlay_maintenance_timers[command["channel"]] = self._add_timer(ANT_MAINTENANCE_TIME, {
                "command": "ACO_maintain_overlay",
                "channel": command["channel"],
                "round_count": 1    # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
            })
    
    def _send_out_ants(self, channel, round_count):
        logger.info("Channel '%s': Round %d: Sending out %d new searching ants..." % (channel, round_count, ANT_COUNT))
        # send out ANT_COUNT ants
        for i in range(0, ANT_COUNT):
            ant = Message("%s_ant" % self.__class__.__name__, {
                "id": str(uuid.uuid4()),
                "channel": channel,
                "ttl": max(6, round_count),
                "strictness": (random.random() * 20) % 20,      # strictness in [0, 20)
                "path": [self.node_id],
                "subscriber": self.node_id,
                "pheromones": 1.0,
                "returning": False,
                # try to activate paths every ACTIVATION_ROUNDS rounds
                "activating": round_count % ACTIVATION_ROUNDS == 0
            })
            con = self._pheromone_choice(ant["channel"], self.connections, ant["strictness"])
            if con:
                logger.debug("Channel '%s': Round %d: Sending out new searching ant %s to %s..." % (ant["channel"], round_count, str(ant), str(con)))
                self._send_covert_msg(ant, con)
            else:
                logger.warning("Channel '%s': Round %d: Cannot send out new searching ant %s, killing ant!" % (ant["channel"], round_count, str(ant)))
