import uuid
import numpy
import logging
logger = logging.getLogger(__name__)

#own classes
from networking import Connection
from utils import get_class_that_defined_method
from .group_router import GroupRouter


class CoverTrafficMixin(object):
    def __init__(self, *args, **kwargs):
        self.__mixin_name = get_class_that_defined_method(self.__configure).__name__
        self.__max_ttl = 60
        self.__groups = {}
    
    def __configure(self, max_ttl):
        self.__max_ttl = max_ttl
    
    def __dump_state(self):
        return {}
    
    def __start_randomwalk_selection_process(self, group_size):
        self.__route_covert_data(Message("%s_create_group" % self.__mixin_name, {
            "size": group_size,
            "ttl": self.__max_ttl,
            "peer_ips": [],
            # this will be hardcoded to 0.5 for now (see __route_create_group)
            "min_interval": 60,     # initialize with 60 seconds (our synchronized send interval will not become bigger than this)
            "channels" : [],
        }))
    
    def __create_group(self, ip_list, channels, min_interval):
        
        GroupRouter.participating_in_groups
    
    def __route_covert_data(self, msg, incoming_connection):
        if msg.get_type().endswith("_init"):
            return self.__route_create_group(msg, incoming_connection)
    
    def __route_create_group(msg, incoming_connection):
        logger.info("Routing create_group: %s coming from %s..." % (str(msg), str(incoming_connection)))
        incoming_peer = incoming_connection.get_peer_id() if incoming_connection != None else None
        
        # add own ip to ip list if not listed already
        if Connection.own_ip not in msg["peer_ips"]:
            msg["peer_ips"].append(Connection.own_ip)
            # for now this is hardcoded to 0.5 seconds
            msg["min_interval"] = min(msg["min_interval"], 0.5)
            # only add channels that are not in some group already
            msg["channels"] = list(set(msg["channels"]).update(self.publishing - GroupRouter.participating_in_groups))
        
        # calculate ttl
        msg["ttl"] -= 1
        if msg["ttl"] < 0:
            logger.warning("TTL expired in create_group message, group will be smaller than specified!")
        
        # create group if all peers are collected or ttl expired
        if len(msg["peer_ips"]) >= msg["size"] or msg["ttl"] < 0:
            logger.info("Collected all peers for group, creating group now...")
            self.__create_group(str(uuid.uuid4()), msg["peer_ips"], msg["channels"], msg["min_interval"])
            return
        
        # select next peer to route message to or create group if no peers are connected (should happen very very rarely)
        connections = list(numpy.random.choice(self.connections, size=1))
        if not len(connections):
            logger.error("Can not route create_group message further, no peers connected!")
            logger.info("Creating group with reduced size...")
            self.__create_group(str(uuid.uuid4()), msg["peer_ips"], msg["channels"], msg["min_interval"])
            return
        self._send_covert_msg(msg, connections[0])
