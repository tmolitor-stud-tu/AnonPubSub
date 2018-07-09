import numpy
import logging
logger = logging.getLogger(__name__)


class ProbabilisticForwardingMixin(object):
    def __init__(self, *args, **kwargs):
        self.__probabilistic_forwarding_peers = {}
    
    def __configure(self, probabilistic_forwarding_fraction):
        self.__probabilistic_forwarding_fraction = probabilistic_forwarding_fraction
    
    def __dump_state(self):
        return {}
    
    def __init_channel(self, channel):
        pass
    
    def __get_additional_peers(self, channel, ignore_peers=None):
        if channel not in self.__probabilistic_forwarding_peers:
            self.__probabilistic_forwarding_peers[channel] = set()
        
        if isinstance(ignore_peers, str):
            ignore_peers = [ignore_peers]       # convert string to list containing one item
        try:
            ignore_peers = set(ignore_peers)    # convert any iterable object to a set
        except:
            ignore_peers = set()                # conversion failed, just assume empty set
        # get a set of all peers we are stilkl connected to that are not in our ignore_peers iterable
        connections = set(con for con in self.connections.values() if con.get_peer_id() not in ignore_peers)
        
        # this are the selected peers we are still connected to
        to_remove = self.__probabilistic_forwarding_peers[channel] - connections
        if len(to_remove):
            logger.info("Removing peers from probabilistic forwarding list of channel '%s': %s" % (str(channel), str(to_remove)))
        self.__probabilistic_forwarding_peers[channel] &= connections
        # this is the list of peers to be selected from
        connections -= self.__probabilistic_forwarding_peers[channel]
        # that much peers are needed
        size = round(len(connections) * self.__probabilistic_forwarding_fraction)
        
        # select new peers and add them to our set if it is smaller than the inteded "size" (zero "size" will *not* trigger this)
        if len(self.__probabilistic_forwarding_peers[channel]) < size:
            size -= len(self.__probabilistic_forwarding_peers[channel])     # only add missing peers and don't change already used ones
            to_add = set(numpy.random.choice(
                list(connections),
                size=size
            ))
            logger.info("Adding new peers to probabilistic forwarding list of channel '%s': %s" % (str(channel), str(to_add)))
            self.__probabilistic_forwarding_peers[channel] |= to_add
        logger.debug("Probabilistic forwarding peers for channel '%s': %s..." % (str(channel), str(self.__probabilistic_forwarding_peers[channel])))
        return self.__probabilistic_forwarding_peers[channel]

    
