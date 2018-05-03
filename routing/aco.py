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
DEFAULT_ROUNDS = 150
MAX_ROUNDS = 500
ACTIVATION_ROUNDS = 50
ANT_ROUND_TIME = 5
RETRY_TIME = 60
MAX_RETRY_TIME = 3600   # upper limit of exponential backoff based on RETRY_TIME)

class ACO(Router):
    
    def __init__(self, node_id, queue):
        super(ACO, self).__init__(node_id, queue)
        self.pheromones = {}
        self.active_edges = {}
        self.publishing = set()
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
    
    def _route_covert_data(self, ant, incoming_connection):
        logger.debug("Routing ant: %s coming from %s..." % (str(ant), str(incoming_connection)))
        self._init_channel(ant["channel"])
        
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
        if not ant["returning"] and ant["channel"] in self.publishing:
            #ant = ant.copy()    # clone ant and let it return
            ant["returning"] = True
            ant["ttl"] = float("inf")
        
        # route returning ants
        if ant["returning"]:
            # update pheromones on edge to incoming node, serialize pheromones write access through command queue
            if ant["channel"] not in self.publishing and incoming_connection:   #ant didn't start its way here --> put pheromones on incoming edge
                self._ACO_update_pheromones_command({
                    "command": "ACO_update_pheromones",
                    "channel": ant["channel"],
                    "node": incoming_connection.get_peer_id(),
                    "pheromones": ant["pheromones"]
                })
            
            if not len(ant["path"]):
                logger.debug("Ant returned successfully, killing ant %s!" % str(ant))
                return                      # ant returned successfully --> kill it
            next_node = ant["path"][-1]     # ants return on reverse path
            del ant["path"][-1]
            if next_node not in self.connections:
                logger.warning("Node %s on reverse ant path NOT in current connection list, killing ant %s!" % (next_node, str(ant)))
                return
            
            # NOTE: not needed because pheromones are directed
            ## update pheromones on edge to next_node, serialize pheromones write access through command queue
            #self._ACO_update_pheromones_command({
            #    "command": "ACO_update_pheromones",
            #    "channel": ant["channel"],
            #    "node": next_node,
            #    "pheromones": ant["pheromones"]
            #})
            if ant["activating"] and next_node not in self.active_edges[ant["channel"]]:
                logger.info("Activating edge to %s for channel '%s'..." % (str(self.connections[next_node]), str(ant["channel"])))
                self.active_edges[ant["channel"]][next_node] = True
            logger.debug("Sending out returning ant: %s to %s..." % (str(ant), str(self.connections[next_node])))
            self._send_covert_msg(ant, self.connections[next_node])
    
    def _route_data(self, msg, incoming_connection=None):
        logger.info("Routing data: %s coming from %s..." % (str(msg), str(incoming_connection)))
        
        if msg["channel"] in self.subscriptions:
            self.subscriptions[msg["channel"]](msg["data"])        #inform own subscriber of new data
        
        self._init_channel(msg["channel"])
        
        # calculate next nodes to route messages to
        incoming_node = incoming_connection.get_peer_id() if incoming_connection else None
        connections = []
        for node_id in self.connections:
            # ignore incoming connection when searching for active edges
            if node_id in self.active_edges[msg["channel"]] and node_id != incoming_node:
                connections.append(self.connections[node_id])
        
        # route message to these nodes
        if not len(connections):
            logger.warning("No peers with active edges found, cannot route data further!")
            logger.warning("Pheromones: %s" % str(self.pheromones[msg["channel"]]))
            logger.warning("Active edges: %s" % str(self.active_edges[msg["channel"]]))
            return
        
        for con in connections:
            logger.info("Routing data to %s..." % str(con))
            self._send_msg(msg, con)
    
    def _init_channel(self, channel):
        if not channel in self.pheromones:
            self.pheromones[channel] = {}
            self.active_edges[channel] = {}
    
    def _remove_connection_command(self, command):
        # call parent class for common tasks
        super(ACO, self)._remove_connection_command(command)
        peer = command["connection"].get_peer_id()
        
        # clean up pheromones afterwards
        for channel in self.pheromones:
            if peer in self.pheromones[channel]:
                del self.pheromones[channel][peer]
        
        # also clean up active edges
        for channel in self.active_edges:
            if peer in self.active_edges[channel]:
                del self.active_edges[channel][peer]
    
    def _subscribe_command(self, command):
        # call parent class for common tasks
        super(ACO, self)._subscribe_command(command)
        self._init_channel(command["channel"])
        
        logger.info("Creating overlay for channel '%s'..." % command["channel"])
        self._ACO_create_overlay_command({
            "command": "ACO_create_overlay",
            "channel": command["channel"],
            "round_count": 1,   # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
            "retry": 0
        })
    
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
            logger.info("Pheromones updated: %s" % str(self.pheromones))
    
    def _ACO_evaporation_command(self, command):
        for channel in self.pheromones:
            for node_id in self.pheromones[channel]:
                self.pheromones[channel][node_id] *= EVAPORATION_FACTOR
        logger.info("Pheromones evaporated: %s" % str(self.pheromones))
        self._add_timer(EVAPORATION_TIME, {"command": "ACO_evaporation"})   # call this command again in EVAPORATION_TIME seconds
    
    def _ACO_create_overlay_command(self, command):
        # send out ANT_COUNT ants
        for i in range(0, ANT_COUNT):
            ant_id = str(uuid.uuid4())
            strictness = (random.random() * 20) % 20       # strictness in [0, 20)
            ttl = max(6, command["round_count"])
            path = [self.node_id]
            ant = Message("%s_ant" % self.__class__.__name__, {
                "id": ant_id,
                "channel": command["channel"],
                "ttl": ttl,
                "strictness": strictness,
                "path": path,
                "pheromones": 1.0,
                "returning": False,
                # try to activate paths every ACTIVATION_ROUNDS rounds
                "activating": command["round_count"] % ACTIVATION_ROUNDS == 0
            })
            con = self._pheromone_choice(ant["channel"], self.connections, ant["strictness"])
            if con:
                logger.info("Channel '%s': Round %d: Sending out new searching ant %s to %s..." % (ant["channel"], command["round_count"], str(ant), str(con)))
                self._send_covert_msg(ant, con)
            else:
                logger.warning("Channel '%s': Round %d: Cannot send out new searching ant %s, killing ant!" % (ant["channel"], command["round_count"], str(ant)))
        
        # loop as long as we didn't reach DEFAULT_ROUNDS or even further if still no active edges are found
        if (
            # loop as long as we didn't reach DEFAULT_ROUNDS
            command["round_count"] < DEFAULT_ROUNDS or
            # or even further if we still have no active edges and didn't reach MAX_ROUNDS
            (len(self.active_edges[command["channel"]]) == 0 and command["round_count"] < MAX_ROUNDS)
        ):
            self._add_timer(ANT_ROUND_TIME, {
                "command": "ACO_create_overlay",
                "channel": command["channel"],
                "round_count": command["round_count"] + 1,
                "retry": command["retry"]
            })
        else:
            if len(self.active_edges[command["channel"]]):
                logger.info("Overlay for channel '%s' created..." % channel)
            else:
                # wait some time and try again (exponential backoff)
                self._add_timer(min(MAX_RETRY_TIME, RETRY_TIME * (2**command["retry"])), {
                    "command": "ACO_create_overlay",
                    "channel": command["channel"],
                    "round_count": 1,   # don't start at zero because 0 % x == 0 which means activating ants get send out in the very first round
                    "retry": command["retry"] + 1
                })
