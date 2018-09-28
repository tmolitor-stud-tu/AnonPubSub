
# *** these filters do NOT see ping messages, nor do routers see them ***
# *** global imports are NOT accessible, do imports in __init__ and bind them to an attribute ***

class Filters(Base):
    def __init__(self, logger):
        
        # init parent class
        super().__init__(logger)
        
        # some imports needed later
        self.random = __import__("random")
        self.base64 = __import__("base64")
        self._thread = __import__("_thread")
        self.time = __import__("time")
        
        # some vars
        #self.master_killed = False
        self.started = 0
        self.data_received = {}
        self.overlay_constructed = False
        self.mutation_logged = False
    
    def gui_command_incoming(self, command):
        global task
        pass
    
    def gui_command_completed(self, command, error):
        global task
        if command["_command"] == "start":
            if not error:
                pass    #self.logger.info("*********** STARTED")
            else:
                pass    #self.logger.info("*********** ERROR STARTING ROUTER: %s" % str(error))
    
    def gui_event_outgoing(self, event):
        global task
        pass
    
    def router_command_incoming(self, command, router):
        global task
        if command["_command"] == "subscribe":
            self.started = self.time.time()
        if command["_command"] == "remove_connection" and task["name"] in ["test", "all_reconnects_on_packetloss"]:
            self.logger.error("*********** CODE_EVENT(remove_connection): reconnects += 1")
    
    def subscribed_datamsg_incoming(self, msg, router):
        global task
        pass
    
    def covert_msg_incoming(self, msg, con):
        global task
        if msg.get_type() == "Flooding_advertise" and not msg["reflood"] and task["name"] in ["flooding_suboptimal_paths"]:
            channel = msg["channel"]
            nonce = self.base64.b64decode(bytes(msg["nonce"], "ascii"))    # decode nonce
            peer_id = con.get_peer_id()
            chain = (self.router._find_nonce(nonce, self.router.advertisement_routing_table[channel])
                        if channel in self.router.advertisement_routing_table else None)
            if chain:       # new advertisements (chain == None) aren't relevant here
                sorting = self.router._compare_nonces(nonce, self.router.advertisement_routing_table[channel][chain].peekitem(0)[0])
                if sorting != -1:
                    return      # received advertisement is longer than already known one
                publisher = self.router._canonize_active_path_identifier(channel, msg['nonce'])
                for subscriber in self.router._ActivePathsMixin__reverse_edges[channel]:
                    if publisher in self.router._ActivePathsMixin__reverse_edges[channel][subscriber]:
                        active_peer = self.router._ActivePathsMixin__reverse_edges[channel][subscriber][publisher]["peer"]
                        if sorting == -1 and active_peer != peer_id:
                            if self.router.node_id == subscriber:
                                self.logger.error("*********** CODE_EVENT(shorter_nonce): shorter_subscribers += 1")
                            else:
                                self.logger.error("*********** CODE_EVENT(shorter_nonce): shorter_intermediates += 1")
                            self.logger.info("*********** DEBUG_DATA: node_id=%s, subscriber=%s, msg=%s" % (self.router.node_id, subscriber, str(msg)))

    def covert_msg_outgoing(self, msg, con):
        global task
        # only check first advertisements
        if msg.get_type() == "Flooding_advertise" and not msg["reflood"] and task["name"] in ["flooding_master_count"]:
            channel = msg["channel"]
            nonce = self.base64.b64decode(bytes(msg["nonce"], "ascii"))     # decode nonce
            peer_id = con.get_peer_id()
            chain = (self.router._find_nonce(nonce, self.router.advertisement_routing_table[channel])
                        if channel in self.router.advertisement_routing_table else None)
            shortest_nonce = self.router.advertisement_routing_table[channel][chain].peekitem(0)[0]
            if None in self.router.advertisement_routing_table[channel][chain][shortest_nonce] and not self.mutation_logged:
                self.logger.error("*********** CODE_EVENT(become_master): master_publishers += 1")
                self.logger.info("*********** DEBUG_DATA: node_id=%s" % self.router.node_id)
                self.mutation_logged = True
    
    def msg_incoming(self, msg, con):
        global task
        
        if task["name"] in [
            "test",
            "flooding_overlay_construction",
            "aco_overlay_construction",
            "aco_overlay_construction_bandwidth",
            "flooding_overlay_construction_packetloss",
            "aco_overlay_construction_packetloss",
            "simple_overlay_construction_packetloss",
        ]:
            if msg["channel"] in self.router.subscriptions and msg["id"] not in self.router.seen_data_ids:      # only check subscriber perspective
                self.logger.info("*********** DEBUG_DATA: received new data")
                if msg["data"] not in self.data_received:
                    self.data_received[msg["data"]] = 0
                self.data_received[msg["data"]] += 1
                self.logger.info("*********** DEBUG_DATA: data_received[%s] --> %s" % (str(msg["data"]), str(self.data_received[msg["data"]])))
                if self.data_received[msg["data"]] == task["publishers"] and not self.overlay_constructed:
                    self.logger.error("*********** CODE_EVENT(overlay_constructed): overlay_construction.append(%.3f)" % (self.time.time() - self.started))
                    self.logger.info("*********** DEBUG_DATA: node_id = %s" % self.router.node_id)
                    self.logger.info("*********** MEASUREMENT_COMPLETED: %s" % self.router.node_id)
                    self.overlay_constructed = True
    
    def msg_outgoing(self, msg, con):
        global task
        #if msg.get_type() == "Flooding_data":
            #if self.time.time() - self.started > 60 and "test" in self.router.master and self.router.master["test"] and not self.master_killed:
                #self.logger.info("*********** KILLING MASTER NODE: node_id=%s" % self.router.node_id)
                #self.master_killed = True
                #self._thread.interrupt_main()
        pass
