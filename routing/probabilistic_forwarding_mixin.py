import numpy
import random
import logging
logger = logging.getLogger(__name__)

#own classes
from networking import Message
from utils import get_class_that_defined_method

class ProbabilisticForwardingMixin(object):
    def __init__(self, *args, **kwargs):
        self.__mixin_name = get_class_that_defined_method(self.__configure).__name__
        self.__additional_peers = {}
        self.__probability = 0
        self.__known_trees = set()
    
    def __configure(self, probabilistic_forwarding_fraction):
        logger.info("Probabilistic forwarding probability: %.3f" % probabilistic_forwarding_fraction)
        self.__probability = probabilistic_forwarding_fraction
    
    def __dump_state(self):
        return {
            "additional_peers": self.__additional_peers
        }
    
    def __init_channel(self, channel):
        if channel not in self.__additional_peers:
            self.__additional_peers[channel] = set()
    
    def __add_subtree(self, channel):
        self.__init_channel(channel)
        self.__route_covert_data(Message("%s_expand_tree" % self.__mixin_name, {
            "channel": channel,
        }))
    
    def __get_additional_peers(self, channel, ignore_peers=None):
        self.__init_channel(channel)
        
        if isinstance(ignore_peers, str):
            ignore_peers = [ignore_peers]       # convert string to list containing one item
        try:
            ignore_peers = set(ignore_peers)    # convert any iterable object to a set
        except:
            ignore_peers = set()                # conversion failed, just assume empty set
        
        # return a set of all peers we are still connected to,
        # that are not in our ignore_peers iterable
        # and where selected as additional peers
        return set(con for con in self.__additional_peers[channel] if con.get_peer_id() not in ignore_peers and con in list(self.connections.values()))
    
    def __route_covert_data(self, msg, incoming_connection=None):
        if msg.get_type().endswith("_expand_tree"):
            return self.__route_expand_tree(msg, incoming_connection)
    
    def __route_expand_tree(self, msg, incoming_connection):
        self.__init_channel(msg["channel"])
        
        # only route further if we are not already a part of this tree
        if msg["channel"] in self.__known_trees:
            return
        self.__known_trees.add(msg["channel"])
        
        # select peers to add to subtree and forward message to them
        for con in self.connections.values():
            p = [self.__probability, 1-self.__probability]
            logger.debug("Probability for attribute overlay expansion via connection %s: %s" % (str(con), str(p)))
            rand = numpy.random.choice([True, False], p=p)
            if rand and con != incoming_connection:     # don't add incoming node to tree
                logger.info("Selected connection %s for attribute overlay expansion" % str(con))
                self.__additional_peers[msg["channel"]].add(con)
                self._send_covert_msg(msg, con)
            else:
                logger.info("*NOT* selected connection %s for attribute overlay expansion" % str(con))

