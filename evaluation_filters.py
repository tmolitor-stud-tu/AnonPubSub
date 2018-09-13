
# *** these filters do NOT see ping messages, nor do routers see them ***
# *** global imports are NOT accessible, do imports in __init__ and bind them to an attribute ***

class Filters(Base):
    def __init__(self, logger):
        # init parent class
        super().__init__(logger)
        
        # nice_colors:
        self.colors = {
            "red": (255,0,0),
            "green": (0,255,0),
            "blue": (0,0,255),
            "white": (255,255,255)
        }
        
        # some vars
        self.master_killed = False
        self.start = 0
        
        # some imports needed later
        self.random = __import__("random")
        self.base64 = __import__("base64")
        self._thread = __import__("_thread")
        self.time = __import__("time")
    
    def gui_command_incoming(self, command):
        if command["_command"] == "start":
            self.start = self.time.time()
    
    def gui_command_completed(self, command, error):
        if command["_command"] == "start":
            if not error:
                self.logger.info("*********** STARTED")
            else:
                self.logger.info("*********** ERROR STARTING ROUTER: %s" % str(error))
    
    def gui_event_outgoing(self, event):
        pass
    
    def router_command_incoming(self, command, router):
        if command["_command"] == "Flooding__become_master" and "test" in self.router.master and not self.router.master["test"]:
            self.logger.info("*********** BECOMING NEW MASTER: node_id=%s" % self.router.node_id)
    
    def covert_msg_incoming(self, msg, con):
        if self.router.__class__.__name__ == "Flooding":
            if msg.get_type() == "Flooding_advertise" and not msg["reflood"]:
                channel = msg["channel"]
                nonce = self.base64.b64decode(bytes(msg["nonce"], "ascii"))    # decode nonce
                peer_id = con.get_peer_id()
                chain = (self.router._find_nonce(nonce, self.router.advertisement_routing_table[channel])
                         if channel in self.router.advertisement_routing_table else None)
                if chain:
                    sorting = self.router._compare_nonces(nonce, self.router.advertisement_routing_table[channel][chain].peekitem(0)[0])
                    if sorting != -1:
                        return      # received advertisement is longer than already known one
                    publisher = self.router._canonize_active_path_identifier(channel, msg['nonce'])
                    for subscriber in self.router._ActivePathsMixin__reverse_edges[channel]:
                        if publisher in self.router._ActivePathsMixin__reverse_edges[channel][subscriber]:
                            active_peer = self.router._ActivePathsMixin__reverse_edges[channel][subscriber][publisher]["peer"]
                            if sorting == -1 and active_peer != peer_id:
                                self.logger.info("*********** SHORTER NONCE: subscriber=%s, msg=%s" % (subscriber, str(msg)))
                else:
                    return    # new advertisements aren't relevant here

    def covert_msg_outgoing(self, msg, con):
        pass
    
    def msg_incoming(self, msg, con):
        pass
    
    def msg_outgoing(self, msg, con):
        if msg.get_type() == "Flooding_data":
            if self.time.time() - self.start > 60 and "test" in self.router.master and self.router.master["test"] and not self.master_killed:
                self.logger.info("*********** KILLING MASTER NODE: node_id=%s" % self.router.node_id)
                self.master_killed = True
                self._thread.interrupt_main()
