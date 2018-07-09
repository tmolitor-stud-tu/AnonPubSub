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
    
    def __get_probabilistic_forwarding_peers(self, channel, ignore_peers=None):
        if channel not in self.__probabilistic_forwarding_peers:
            self.__probabilistic_forwarding_peers[channel] = set()
        if not len(self.__probabilistic_forwarding_peers[channel]):
            if not (isinstance(ignore_peers, list) or isinstance(ignore_peers, set)):
                ignore_peers = set(ignore_peers)
            connections = [con for con in self.connections.values() if con.get_peer_id() not in ignore_peers]
            self.__probabilistic_forwarding_peers[channel] = set(numpy.random.choice(
                connections,
                size=round(len(connections) * self.__probabilistic_forwarding_fraction)
            ))
        logger.debug("Probabilistic forwarding peers for channel '%s': %s..." % (str(channel), str(self.__probabilistic_forwarding_peers[channel])))
        return self.__probabilistic_forwarding_peers[channel]

    
