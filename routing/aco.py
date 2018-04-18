import uuid
import math
import random
import numpy
from queue import Queue
from threading import Thread
from threading import Event
import logging
logger = logging.getLogger(__name__)

from networking import Connection, Message
from .base import Router


#taken from paper:
ANT_COUNT = 5

#NOT taken from paper:
EVAPORATION_TIME = 5
EVAPORATION_FACTOR = 0.75
MAX_ROUNDS = 2
ANT_WAITING_TIME = 5
PHEROMONES_BUDGET = 1.0
ROUTING_THRESHOLD = 0.01
PHEROMONES_INIT = 1.0
OVERLAY_RECREATION_TIME = 15

class ACO(Router):
    
    def __init__(self, node_id, queue):
        super(ACO, self).__init__(node_id, queue)
        self.pheromones = {}
        self.subscriptions = {}
        self.publishing = set()
        self.create_overlay_threads = []
        self.evaporation_thread = Thread(name="local::"+self.node_id+"::_evaporation", target=self._evaporation)
        self.evaporation_thread.start()
        logger.info("ACO Router initialized...")
    
    def publish(self, channel, data):
        logger.info("Publishing data on channel '%s'..." % str(channel))
        self._init_channel(channel)
        self.publishing.add(channel)
        self._route_data(Message("data", {"channel": channel, "data": data, "ttl": max(6, MAX_ROUNDS)}))
    
    def subscribe(self, channel, callback):
        if channel in self.subscriptions:
            return
        logger.info("Subscribing for data on channel '%s'..." % str(channel))
        self._init_channel(channel)
        self.subscriptions[channel] = callback
        t = Thread(name="local::"+self.node_id+"::_create_overlay", target=self._create_overlay, args=(channel,))
        self.create_overlay_threads.append(t)
        t.start()
    
    def stop(self):
        logger.warning("Stopping router!")
        super(ACO, self).stop()
        self.evaporation_thread.join()
        for t in self.create_overlay_threads:
            t.join()
        logger.warning("Router successfully stopped!");
    
    def _pheromone_choice(self, channel, population, strictness=0.0):
        if not len(population):     #no connection to choose
            return None
        
        #calculate raw value of pheromones
        weights = []
        for node_id in population:
            if node_id in self.pheromones[channel]:
                weights.append(math.fsum(self.pheromones[channel][node_id].values()))
            else:
                weights.append(ROUTING_THRESHOLD)
        
        '''
        #apply equation (4) from ACO paper to those values
        print("weights pow: %s" % str([math.pow(w, strictness) for w in weights]))
        strictness_sum = math.fsum([math.pow(w, strictness) for w in weights])
        print("strictness_sum: %s, len(weights): %d" % (str(strictness_sum), len(weights)))
        weights = [ (1.0/len(weights)) + (math.pow(w, strictness) / strictness_sum) for w in weights]
        '''
        
        #normalize weights (maybe not needed, but done to be on the safe side because numpy.random.choice() throws an exception if not normalized)
        print("population: %s" % str(population))
        print("weights: %s" % str(weights))
        weights_sum = math.fsum(weights)
        print("weights_sum: %s" % str(weights_sum))
        if weights_sum > 0.0:
            weights = [float(i)/weights_sum for i in weights]
        print("weights normalized: %s" % str(weights))
        
        return numpy.random.choice(list(population.values()), p=weights, size=1)[0]

    def _create_overlay(self, channel):
        while True:
            logger.info("(Re)Creating overlay for channel '%s'..." % channel)
            round_count = 0
            while not Router.stopped.isSet() and round_count < MAX_ROUNDS and len(self.connections):
                #send out ANT_COUNT ants
                for i in range(0, ANT_COUNT):
                    ant_id = str(uuid.uuid4())
                    strictness = (random.random() * 20) % 20       #strictness in [0, 20)
                    ttl = max(6, round_count)
                    path = [self.node_id]
                    ant = {
                        "id": ant_id,
                        "channel": channel,
                        "subscriber": self.node_id,
                        "ttl": ttl,
                        "strictness": strictness,
                        "path": path,
                        "returning": False
                    }
                    con = self._pheromone_choice(ant["channel"], self.connections, ant["strictness"])
                    if con:
                        logger.info("%s: Round %d Sending out new searching ant %s to %s..." % (ant["channel"], round_count, str(ant), str(con)))
                        con.send_msg(Message("ant", ant))
                    else:
                        logger.warning("Cannot route new searching ant, killing ant %s!" % str(ant))
                    
                #wait ANT_WAITING_TIME for ants to return before starting next round
                Router.stopped.wait(ANT_WAITING_TIME)
                round_count += 1
            logger.info("Overlay for channel '%s' (re)created..." % channel)
            
            if Router.stopped.wait(OVERLAY_RECREATION_TIME):
                break
    
    def _route_ant(self, ant, incoming_connection):
        logger.info("Routing ant: %s coming from %s..." % (str(ant), str(incoming_connection)))
        self._init_channel(ant["channel"])
        #let ant return if we are a publisher
        if not ant["returning"] and ant["channel"] in self.publishing:
            ant["returning"] = True
            ant["ttl"] = float("inf")
            ant["pheromones"] = PHEROMONES_BUDGET / len(ant["path"])    #distribute pheromones budget equally among traversed edges
        
        if ant["returning"]:
            #update pheromones on edge to incoming node, serialize pheromones write access through command queue (using _process_command())
            if ant["channel"] not in self.publishing:   #ant didn't start its way here --> put pheromones on incoming edge
                self.queue.put({
                    "command": "ACO_update_pheromones",
                    "channel": ant["channel"],
                    "subscriber": ant["subscriber"],
                    "node": incoming_connection.get_peer_id(),
                    "pheromones": ant["pheromones"]
                })
            
            if not len(ant["path"]):
                logger.warning("Ant returned successfully, killing ant %s!" % str(ant))
                return                      #ant returned successfully --> kill it
            next_node = ant["path"][-1]     #ants return on reverse path
            del ant["path"][-1]
            if next_node not in self.connections:
                logger.warning("Node %s on reverse ant path NOT in current connection list, killing ant %s!" % (next_node, str(ant)))
                return
            
            #update pheromones on edge to next_node, serialize pheromones write access through command queue (using _process_command())
            self.queue.put({
                "command": "ACO_update_pheromones",
                "channel": ant["channel"],
                "subscriber": ant["subscriber"],
                "node": next_node,
                "pheromones": ant["pheromones"]
            })
            logger.info("Sending out returning ant: %s to %s..." % (str(ant), str(self.connections[next_node])))
            self.connections[next_node].send_msg(Message("ant", ant))
        else:
            ant["ttl"] -= 1
            if ant["ttl"] == 0:                 #ttl expired --> kill ant
                logger.warning("TTL expired, killing ant %s!" % str(ant))
                return
            if self.node_id in ant["path"]:     #loop detected --> kill ant
                logger.warning("Loop detected, killing ant %s!" % str(ant))
                return
            ant["path"].append(self.node_id)
            connections = {key: value for key, value in self.connections.items() if value!=incoming_connection}
            con = self._pheromone_choice(ant["channel"], connections, ant["strictness"])
            if con:
                logger.info("Sending out searching ant: %s to %s..." % (str(ant), str(con)))
                con.send_msg(Message("ant", ant))
            else:
                logger.warning("Cannot route searching ant, killing ant %s!" % str(ant))
    
    def _route_data(self, msg, incoming_connection=None):
        logger.info("Routing data: %s coming from %s..." % (str(msg), str(incoming_connection)))
        if msg["ttl"] <= 0:
            logger.warning("Ignoring data because of expired ttl!")
            return
        self._init_channel(msg["channel"])
        msg["ttl"] -= 1
        if msg["channel"] in self.subscriptions:
            self.subscriptions[msg["channel"]](msg["data"])        #inform own subscriber of new data
        
        #calculate next nodes to route messages to
        incoming_node = incoming_connection.get_peer_id() if incoming_connection else None
        connections = []
        for node_id in self.connections:
            pheromones_sum = 0.0
            for _node_id in self.pheromones[msg["channel"]]:
                for _subscriber_id in self.pheromones[msg["channel"]][_node_id]:
                    pheromones_sum += self.pheromones[msg["channel"]][_node_id][_subscriber_id]
            if (node_id in self.pheromones[msg["channel"]]
            #apply routing threshold because evaporation will rarely result in 0.0
            and pheromones_sum > ROUTING_THRESHOLD
            #ignore incoming connection for routing
            and node_id != incoming_node):
                connections.append(self.connections[node_id])
        
        #route message to these nodes
        if not len(connections):
            logger.warning("No peers above threshold found, cannot route data further!")
            logger.warning("Pheromones: %s" % str(self.pheromones[msg["channel"]]))
        for con in connections:
            con.send_msg(msg)
    
    def _init_channel(self, channel):
        if not channel in self.pheromones:
            self.pheromones[channel] = {}
    
    def _process_command(self, command):
        if command["command"] == "add_connection":
            con = command["connection"]
            peer = con.get_peer_id()
            self.connections[peer] = con
        elif command["command"] == "remove_connection":
            con = command["connection"]
            peer = con.get_peer_id()
            del self.connections[peer]
            #clean up pheromones
            for channel in self.pheromones:
                if peer in self.pheromones[channel]:
                    del self.pheromones[channel][peer]
        elif command["command"] == "message_received":
            con = command["connection"]
            msg = command["message"]
            msg_type = msg.get_type()
            if msg_type == "data":
                self._route_data(msg, con)
            elif msg_type == "ant":
                self._route_ant(msg, con)
        elif command["command"] == "ACO_update_pheromones":
            if command["channel"] not in self.pheromones:
                logger.error("Unknown channel '%s' while updating pheromones, skipping update!" % command["channel"])
            else:
                if command["node"] not in self.pheromones[command["channel"]]:
                    self.pheromones[command["channel"]][command["node"]] = {}
                pheromones_on_edge = self.pheromones[command["channel"]][command["node"]]
                #calculate pheromone ratio (equation (4) in ACO paper)
                ratio = PHEROMONES_INIT
                if command["subscriber"] not in pheromones_on_edge:
                    pheromones_on_edge[command["subscriber"]] = 0.0
                elif math.fsum(pheromones_on_edge.values()) > 0.0:
                    ratio = pheromones_on_edge[command["subscriber"]] / math.fsum(pheromones_on_edge.values())
                #apply pheromones according to calculated ratio
                pheromones_on_edge[command["subscriber"]] += ratio * command["pheromones"]
                logger.info("Pheromones updated: %s" % str(self.pheromones))
        elif command["command"] == "ACO_evaporation":
            self._do_evaporation(); 
        else:
            logger.error("Unknown routing command '%s', ignoring command!" % command["command"])
    
    def _evaporation(self):
        logger.debug("pheromones evaporation thread started...")
        while not Router.stopped.wait(EVAPORATION_TIME):
            #serialize pheromones write access through command queue (using _process_command() calling _do_evaporation())
            self.queue.put({"command": "ACO_evaporation"})
        logger.debug("pheromones evaporation thread stopped...")
    
    def _do_evaporation(self):
        for channel in self.pheromones:
            for node_id in self.pheromones[channel]:
                for subscriber in list(self.pheromones[channel][node_id].keys()):
                    self.pheromones[channel][node_id][subscriber] *= EVAPORATION_FACTOR
                    if self.pheromones[channel][node_id][subscriber] < ROUTING_THRESHOLD:
                        del self.pheromones[channel][node_id][subscriber]       #delete pheromones beyond threshold
        logger.info("Pheromones evaporated: %s" % str(self.pheromones))
