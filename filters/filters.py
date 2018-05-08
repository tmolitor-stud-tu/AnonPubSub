import logging
logger = logging.getLogger(__name__)


# *** these filters do NOT see ping messages, nor do routers see them ***

def covert_msg_incoming(msg, con):
    # example to indicate incoming ant
    if msg.get_type() == "ACO_ant":
        # do something meaningful here
        logger.error("received ant %s" % msg["id"])
    
    # example to indicate incoming returning ant
    if msg.get_type() == "ACO_ant" and msg["returning"]:
        # do something meaningful here
        logger.error("returning ant coming from peer %s" % con.get_peer_id())

def covert_msg_outgoing(msg, con):
    # example to indicate outgoing ant
    if msg.get_type() == "ACO_ant":
        # do something meaningful here
        logger.error("sending out ant %s" % msg["id"])
    
    # example to indicate outgoing returning activating ant
    if msg.get_type() == "ACO_ant" and msg["returning"] and msg["activating"]:
        # do something meaningful here
        logger.error("sending returning and activating ant to peer %s" % con.get_peer_id())

def msg_incoming(msg, con):
    # example to indicate incoming data on channel "test"
    if msg.get_type() == "ACO_data" and msg["channel"] == "test":
        # do something meaningful here
        logger.error("received data on channel 'test' from peer %s" % con.get_peer_id())

def msg_outgoing(msg, con):
    # example to indicate outgoing data on channel "test"
    if msg.get_type() == "ACO_data" and msg["channel"] == "test":
        # do something meaningful here
        logger.error("sending data on channel 'test' to peer %s" % con.get_peer_id())
