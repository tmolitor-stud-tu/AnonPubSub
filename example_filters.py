
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
        
        # some imports needed later
        self.random = __import__("random")
    
    def gui_command_incoming(self, command):
        if command["_command"] == "start":
            pass    #self.logger.info("*********** STARTING")
    
    def gui_command_completed(self, command, error):
        if command["_command"] == "start":
            if not error:
                pass    #self.logger.info("*********** STARTED")
            else:
                pass    #self.logger.info("*********** ERROR STARTING ROUTER: %s" % str(error))
    
    def gui_event_outgoing(event):
        pass
    
    def router_command_incoming(self, command, router):
        pass
    
    def covert_msg_incoming(self, msg, con):
        #self.logger.info("*********** COVERT INCOMING(%s) connection %s ***********" % (msg.get_type(), con))
        # example to indicate incoming ant
        if msg.get_type() == "ACO_ant":
            # do something meaningful here
            pass    #logger.error("received ant %s" % msg["id"])
        
        # example to indicate incoming returning ant
        if msg.get_type() == "ACO_ant" and msg["returning"]:
            # do something meaningful here
            pass    #logger.error("returning ant coming from peer %s" % con.get_peer_id())

    def covert_msg_outgoing(self, msg, con):
        #self.logger.info("*********** COVERT OUTGOING(%s) connection %s ***********" % (msg.get_type(), con))
        # example to indicate outgoing ant
        if msg.get_type() == "ACO_ant":
            # do something meaningful here
            pass    #logger.error("sending out ant %s" % msg["id"])
        
        # example to indicate outgoing returning activating ant
        if msg.get_type() == "ACO_ant" and msg["returning"] and msg["activating"]:
            # do something meaningful here
            pass    #logger.error("sending returning and activating ant to peer %s" % con.get_peer_id())

    def msg_incoming(self, msg, con):
        #self.logger.info("*********** INCOMING(%s) connection %s ***********" % (msg.get_type(), con))
        if msg.get_type() == "GroupRouter_data":
            self.leds[8].on(self.colors["green"], 0.25)
        elif msg.get_type() == "Flooding_publish":
            self.leds[0].on(self.colors["red"], 0.5)
        else:
            self.leds[4].on(self.colors["red"], 0.5)

    def msg_outgoing(self, msg, con):
        #self.logger.info("*********** OUTGOING(%s) connection %s ***********" % (msg.get_type(), con))
        if msg.get_type() == "GroupRouter_data":
            self.leds[9].on(self.colors["white"], 0.25)
        elif msg.get_type() == "Flooding_publish":
            self.leds[1].on(self.colors["blue"], 0.5)
        else:
            self.leds[5].on(self.colors["blue"], 0.5)
        #if self.router.__class__.__name__ == "Flooding" and self.router.master[msg["channel"]]:
            #return False
        #drop = self.random.choice([True, False])
        #if drop:
            #self.leds[1].on(self.colors["white"], 0.5)
        #return drop     # this will drop the message in 50% of the cases
