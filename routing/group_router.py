from queue import Queue, Empty, Full
import logging
logger = logging.getLogger(__name__)

#own classes
from networking import Connection
from networking import Message
from .router_base import RouterBase


class GroupRouter(RouterBase):
    participating_in_groups = set()
    
    def __init__(self, node_id, queue, router):
        super(GroupRouter, self).__init__(node_id, queue)
        self.data_queues = {}
        self.outgoing_timers = {}
        self.unbound_connections = {}
        self.groups = {}
        self.router = router
    
    def stop(self):
        logger.warning("Stopping group router!")
        GroupRouter.participating_in_groups = set()
        super(GroupRouter, self).stop()
        logger.warning("Group router successfully stopped!");
        
    def publish(self, channel, data):
        if channel not in self.outgoing_timers:
            return False    # we are not in any group for this channel
        self._call_command({"_command": "GroupRouter__publish", "channel": channel, "data": data})
        return True         # data will be published
    
    def create_group(self, channel, ip_list, interval):
        self._call_command({"_command": "GroupRouter__create_group", "channel": channel, "ip_list": ip_list, "interval": interval})
    
    def __dump_state(self):
        return {
            "outgoing_timers": self.outgoing_timers,
            "unbound_connections": self.unbound_connections,
            "groups": self.groups
        }
    
    def _add_connection_command(self, command):
        con = command["connection"]
        peer_ip = con.get_peer_ip()
        self.connections[peer_ip] = con
        
        # try to initialize all known groups
        for channel in list(self.unbound_connections.keys()):
            self._init_group(channel)
    
    def _remove_connection_command(self, command):
        con = command["connection"]
        peer_ip = con.get_peer_ip()
        if peer_ip in list(self.connections.keys()):
            del self.connections[peer_ip]
    
    def _init_group(self, channel):
        interval = self.unbound_connections[channel]["interval"]
        # test if all connections for this channel are established
        all_established = True
        for con in self.unbound_connections[channel]["connections"]:
            if con.connection_state != "ESTABLISHED":
                all_established = False
                break
        if all_established:
            logger.info("All group connections for channel '%s' established, sending out init messages..." % channel)
            msg = Message("%s_init" % self.__class__.__name__, {
                "channel": channel,
                "interval": self.unbound_connections[channel]["interval"],
            })
            for con in list(self.unbound_connections[channel]["connections"]):
                logger.info("Sending group init for channel '%s' to %s..." % (channel, str(con)))
                con.send_msg(msg)
            self.groups[channel] = {
                "connections": self.unbound_connections[channel]["connections"],
                "received": {},
            }
            del self.unbound_connections[channel]
    
    def _route_data(self, msg, incoming_connection=None):
        if msg.get_type().endswith("_init"):
            return self._route_init(msg, incoming_connection)
        elif msg.get_type().endswith("_deinit"):
            return self._route_deinit(msg, incoming_connection)
        elif msg.get_type().endswith("_data"):
            return self._route_real_data(msg, incoming_connection)
    
    def _route_init(self, msg, incoming_connection):
        logger.info("Routing init: %s coming from %s..." % (str(msg), str(incoming_connection)))
        command = {"_command": "GroupRouter__publish_data", "channel": msg["channel"], "interval": msg["interval"], "connection": incoming_connection}
        self._call_command(command)
    
    def _route_deinit(self, msg, incoming_connection):
        logger.info("Routing deinit: %s coming from %s..." % (str(msg), str(incoming_connection)))
        if msg["channel"] in self.outgoing_timers:
            logger.info("Ignoring unknown outgoing_timers channel '%s'..." % str(msg["channel"]))
            self._abort_timer(self.outgoing_timers[msg["channel"]]["timer"])
            del self.outgoing_timers[msg["channel"]]
            del self.data_queues[msg["channel"]]
    
    def _route_real_data(self, msg, incoming_connection):
        logger.info("Routing data: %s coming from %s..." % (str(msg), str(incoming_connection)))
        if msg["channel"] not in self.groups:                                   # ignore unknown groups
            logger.info("Ignoring unknown group channel '%s'..." % str(msg["channel"]))
            return
        
        # add message to received list
        if msg["counter"] not in self.groups[msg["channel"]]["received"]:
            self.groups[msg["channel"]]["received"][msg["counter"]] = {}
        self.groups[msg["channel"]]["received"][msg["counter"]][incoming_connection.get_peer_ip()] = msg
        
        # check if we received all messages for any known (incomplete) counter
        for counter in list(self.groups[msg["channel"]]["received"].keys()):
            if len(self.groups[msg["channel"]]["received"][counter]) == len(self.groups[msg["channel"]]["connections"]):
                logger.info("Received round %s for channel '%s', publishing data on behalf of group members..." % (str(counter), str(msg["channel"])))
                for data_msg in self.groups[msg["channel"]]["received"][counter].values():
                    # extract all data messages and publish them
                    if data_msg["type"] == "data":
                        self.router.publish(data_msg["channel"], data_msg["data"]);
                del self.groups[msg["channel"]]["received"][counter]    # counter was complete --> remove it from list
    
    def __publish_data_command(self, command):
        # reshedule this command
        if command["channel"] in self.outgoing_timers:
            self._abort_timer(self.outgoing_timers[command["channel"]]["timer"])
        self.outgoing_timers[command["channel"]] = {
            # increment counter by one starting with zero
            "counter": self.outgoing_timers[command["channel"]]["counter"] + 1 if command["channel"] in self.outgoing_timers else 0,
            # create new timer
            "timer": self._add_timer(command["interval"], command)
        }
        
        # send data to RS
        try:
            if command["channel"] not in self.data_queues:      # treat non existent queue as empty
                raise Empty
            data = self.data_queues[command["channel"]].get(False)
            self.data_queues[command["channel"]].task_done()
            logger.info("Sending out data message for round %s in channel '%s'..." % (str(self.outgoing_timers[command["channel"]]["counter"]), str(command["channel"])))
            msg = Message("%s_data" % self.__class__.__name__, {
                "type": "data",
                "channel": command["channel"],
                "data": data,
                "counter": self.outgoing_timers[command["channel"]]["counter"],
            })
        except Empty as err:
            logger.info("Sending out dummy message for round %s in channel '%s'..." % (str(self.outgoing_timers[command["channel"]]["counter"]), str(command["channel"])))
            msg = Message("%s_data" % self.__class__.__name__, {
                "type": "dummy",
                "channel": command["channel"],
                "data": "DEADBEEF",
                "counter": self.outgoing_timers[command["channel"]]["counter"],
            })
        command["connection"].send_msg(msg)
    
    def __publish_command(self, command):
        if command["channel"] not in self.data_queues:
            self.data_queues[command["channel"]] = Queue()
        self.data_queues[command["channel"]].put(command["data"])
    
    def __create_group_command(self, command):
        # deinit old group if neccessary
        if command["channel"] in self.groups:
            msg = Message("%s_deinit" % self.__class__.__name__, {
                "channel": command["channel"],
            })
            for con in self.groups[command["channel"]]["connections"]:
                con.send_msg(msg)
        
        # create connections and add them to self.unbound_connections (init message will be sent when all those connections are established)
        self.unbound_connections[command["channel"]] = {"interval": command["interval"], "connections": set()}
        for ip in command["ip_list"]:
            if ip in self.connections:
                con = self.connections[ip]      # reuse connections
            else:
                con = Connection.connect_to("group", ip)
            self.unbound_connections[command["channel"]]["connections"].add(con)
        self._init_group(command["channel"])
