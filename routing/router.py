from datetime import datetime
import logging
logger = logging.getLogger(__name__)

# own classes
import filters
from networking import Message
from .router_base import RouterBase


class Router(RouterBase):
    settings = {
        "OUTGOING_TIME": 1.0,       # time to wait until a message is really sent out
        "TIMING_FACTOR": 2.0,       # factor to scale timing values by
    }
    
    # *** public interface ***
    def __init__(self, node_id, queue):
        super(Router, self).__init__(node_id, queue)
        self.publishing = set()
        self.subscriptions = {}
        self.seen_data_ids = set()
        self.last_outgoing_time = {}
    
    def subscribe(self, channel, callback):
        logger.info("Subscribing for data on channel '%s'..." % str(channel))
        self.queue.put({
            "_command": "subscribe",
            "channel": channel,
            "callback": callback
        })
    
    def unsubscribe(self, channel):
        logger.info("Unsubscribing channel '%s'..." % str(channel))
        self.queue.put({
            "_command": "unsubscribe",
            "channel": channel,
        })
    
    def publish(self, channel, data):
        logger.debug("Publishing data on channel '%s'..." % str(channel))
        self.queue.put({
            "_command": "publish",
            "channel": channel,
            "data": data
        })
    
    def unpublish(self, channel):
        logger.debug("Unpublishing channel '%s'..." % str(channel))
        self.queue.put({
            "_command": "unpublish",
            "channel": channel
        })
    
    # *** internal api methods for child classes ***
    def _add_timer(self, timeout, command):
        timeout *= Router.settings["TIMING_FACTOR"]         # delay timer for demonstrating purposes
        return self._RouterBase__real_add_timer(timeout, command)
    
    # _send_msg() and _send_covert_msg() are used by child classes for rate limited sending of messages (for routing demonstration purposes)
    def _send_msg(self, msg, con):
        if not filters.msg_outgoing(msg, con):              # call filters framework
            self.__outgoing("data", msg, con)
    
    def _send_covert_msg(self, msg, con):
        if not filters.covert_msg_outgoing(msg, con):       # call filters framework
            self.__outgoing("covert_data", msg, con)
    
    def _forward_data(self, msg):
        if msg["channel"] in self.subscriptions and msg["id"] not in self.seen_data_ids:
            self.seen_data_ids.add(msg["id"])
            self.subscriptions[msg["channel"]](msg["data"])        # inform own subscriber of new data
    
    # *** command methods that can be overwritten or used as is by child classes ***
    def _add_connection_command(self, command):
        super(Router, self)._add_connection_command(command)
    
    def _remove_connection_command(self, command):
        super(Router, self)._remove_connection_command(command)
        con = command["connection"]
        peer = con.get_peer_id()
        if peer in self.last_outgoing_time:
            del self.last_outgoing_time[peer]
    
    def _subscribe_command(self, command):
        self.subscriptions[command["channel"]] = command["callback"]
    
    def _unsubscribe_command(self, command):
        if command["channel"] in self.subscriptions:
            del self.subscriptions[command["channel"]]
    
    def _publish_command(self, command):
        self.publishing.add(command["channel"])
    
    def _unpublish_command(self, command):
        self.publishing.discard(command["channel"])
    
    # *** special command methods that should NOT be overwritten by child classes ***
    def _dump_command(self, command):
        state = {
            "connections": list(self.connections.values()),
            "publishing": self.publishing,
            "subscriptions": list(self.subscriptions.keys()),
            "seen_data_ids": len(self.seen_data_ids),
        }
        
        # collect child and mixin states
        childs = set(base.__name__ for base in self.__class__.__bases__ if base.__name__.endswith("Mixin"))
        childs.add(self.__class__.__name__)
        for child in childs:
            func = "_%s__dump_state" % child    # child namespace
            if hasattr(self, func):
                state[child] = getattr(self, func)()
        
        # output and return collected states
        logger.info("INTERNAL STATE:\n%s" % str(state))
        if command and "callback" in command and command["callback"]:
            command["callback"](state)
    
    # *** internal methods, DON'T touch from child classes ***
    def __outgoing(self, msg_type, msg, con):
        # no locking required in this method since we are only called from _routing thread
        
        # use "Router__real_send" as reference to our __real_send_command() because of name mangling semantics of double underscore
        # (those methods cannot accidentally be overwritten from child)
        # clone message object to decouple it from changes the caller does after "sending it out"
        command = {"_command": "Router__real_send", "type": msg_type, "message": Message(msg), "connection": con}
        
        if not Router.settings["OUTGOING_TIME"]:        # no delay wanted at all --> call __real_send directly
            self._call_command(command)
            return
        
        # calculate next send time and update timestamps
        peer = con.get_peer_id()
        if peer not in self.last_outgoing_time:
            self.last_outgoing_time[peer] = {"data": datetime.now().timestamp(), "covert_data": datetime.now().timestamp()}
        
        # *** this will send out only one message per OUTGOING_TIME
        # always wait a minimum of OUTGOING_TIME seconds for every message
        #timestamp = max(
            #self.last_outgoing_time[peer][msg_type] + Router.settings["OUTGOING_TIME"],
            #datetime.now().timestamp() + Router.settings["OUTGOING_TIME"]
        #)
        
        # *** this will delay every message for OUTGOING_TIME seconds (but send out more than one message per OUTGOING_TIME if requested)
        # delay every message by OUTGOING_TIME seconds
        timestamp = datetime.now().timestamp() + Router.settings["OUTGOING_TIME"]
        
        self.last_outgoing_time[peer][msg_type] = timestamp     # update last outgoing timestamp
        
        # add send timer
        self._RouterBase__real_add_timer(timestamp - datetime.now().timestamp(), command)
    
    def __real_send_command(self, command):
        try:
            if command["type"] == "data":
                command["connection"].send_msg(command["message"], False)               # don't call filters again
            elif command["type"] == "covert_data":
                command["connection"].send_covert_msg(command["message"], False)        # don't call filters again
            else:
                raise ValueError("Message type '%s' unknown!" % command["type"])
        except Exception as e:
            logger.warning("Failed to really send %s message to peer %s: %s" % (str(command["type"]), str(command["connection"].get_peer_id()), str(e)))
