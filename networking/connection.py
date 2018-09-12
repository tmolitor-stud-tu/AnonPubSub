import os
import base64
import binascii
import struct
import numpy
from collections import deque
from threading import Thread, Event, RLock, current_thread

#logging
import logging
def configure_logger(addr=None, instance_id=None):
    logger = logging.getLogger("%s[%s]@%s:%s" % (__name__, str(instance_id), str(addr[0]), str(addr[1])))
    return logger
logger = logging.getLogger(__name__)    #default logger

#crypto imports
try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except Exception as e:
    logger.warning("Crypto imports failed, ignoring this (make sure to disable crypto in settings!!)")

# own classes
from utils import catch_exceptions
from .listener import Listener
from .message import Message
import filters


class Connection(object):
    # these are shared between all connection types
    reconnections_stopped = Event()
    global_lock = RLock()
    len_size = len(struct.pack("!Q", 0))	# calculate length of packed unsigned long long int
    settings = {
        "MAX_COVERT_PAYLOAD": 1200,         # always +94 bytes overhead for ping added to final encrypted packet (+58 bytes for unencrypted packets)
        "PING_INTERVAL": 0.25,
        "MAX_MISSING_PINGS": 16,            # consecutive missing pings, connection timeout will be MAX_MISSING_PINGS * PING_INTERVAL
        "MAX_RECONNECTS": 3,                # maximum consecutive reconnects after a connection failed
        "ENCRYPT_PACKETS": True,
        "PACKET_LOSS": 0,                   # fraction of packet loss to apply to outgoing packets (covert channel AND data channel)
    }
    
    # these are scoped to connection types
    instances = {}
    node_id = {}
    event_queue = {}
    router_queue = {}
    listener = {}
    port = {}
    
    # public static method to initialize networking
    @staticmethod
    def init(connection_type, node_id, queue, host, port, event_queue):
        logger.warning("Initializing networking (type: %s)..." % connection_type)
        with Connection.global_lock:
            Connection.instances[connection_type] = {}
            Connection.node_id[connection_type] = node_id
            Connection.event_queue[connection_type] = event_queue 
            Connection.router_queue[connection_type] = queue
            Connection.port[connection_type] = port
            Connection.reconnections_stopped.clear()    # clear shutdown flag (this is shared between all connection types)
            Connection.listener[connection_type] = Listener(
                connection_type,
                Connection.node_id[connection_type],
                Connection._incoming_data,
                host,
                Connection.port[connection_type]
            )
    
    @staticmethod
    def check_init(connection_type):
        with Connection.global_lock:
            if connection_type not in Connection.instances:
                raise ValueError("Network (type: %s) not initialized, call Connection.init() first!" % connection_type)
    
    # public static factory method for new outgoing instances
    @staticmethod
    def connect_to(connection_type, host, reconnect_try=0):
        with Connection.global_lock:
            Connection.check_init(connection_type)
            return Connection._new(connection_type, (host, Connection.port[connection_type]), True, reconnect_try)
    
    @staticmethod
    def disconnect_from(connection_type, host):
        with Connection.global_lock:
            Connection.check_init(connection_type)
            addr = (host, Connection.port[connection_type])
            if str(addr) in Connection.instances[connection_type]:
                Connection.instances[connection_type][str(addr)].terminate()
    
    # internal static factory method for incoming packets used by listener class
    @staticmethod
    def _incoming_data(connection_type, addr, data):
        # ignore incoming data if networking is not initialized
        with Connection.global_lock:
            if connection_type not in Connection.instances:
                return
        con = Connection._new(connection_type, addr, False, 0)
        con._incoming(data)
    
    # internal static factory method doing the actual work
    @staticmethod
    def _new(connection_type, addr, active_init, reconnect_try):
        with Connection.global_lock:
            Connection.check_init(connection_type)
            if str(addr) not in Connection.instances[connection_type] or Connection.instances[connection_type][str(addr)].is_dead.is_set():
                Connection.instances[connection_type][str(addr)] = Connection(connection_type, addr, active_init, reconnect_try)
            con = Connection.instances[connection_type][str(addr)]
        if active_init:
            con.active_init = active_init   # force right value (used on reconnect)
        return con
    
    # public static method to terminate all connections
    @staticmethod
    def shutdown(type_to_shutdown):
        logger.warning("Shutting down full networking (types: %s)..." % type_to_shutdown if type_to_shutdown else "all")
        # extract list of initialized network types
        with Connection.global_lock:
            types = list(Connection.instances.keys())
        types = [t for t in types if not type_to_shutdown or t==type_to_shutdown]
        if not len(types):
            logger.warning("Full networking shutdown: nothing to shutdown!")
            return
        # stop all listeners without holding global_lock (listeners receiving data after this thread aqcuired the lock would otherwise deadlock)
        for connection_type in types:
            logger.warning("Stopping listener (type: %s)..." % connection_type)
            # stop listener (this prevents creation of new connections from incoming packets)
            Connection.listener[connection_type].stop()
        # stop reconnections from happening
        Connection.reconnections_stopped.set()
        # now start real shutdown of all connections while holding global_lock
        with Connection.global_lock:
            for connection_type in types:
                logger.warning("Shutting down networking (type: %s)..." % connection_type)
                # terminate all connections (has to be done AFTER stopping the listener to not create new connections by incoming packets)
                for con in list(Connection.instances[connection_type].values()):
                    con.terminate()
                    # wait also for termination of reconnect_thread
                    if con.reconnect_thread and con.reconnect_thread.is_alive() and con.reconnect_thread != current_thread():
                        con.reconnect_thread.join(4.0 + (Connection.settings["PING_INTERVAL"] * Connection.settings["MAX_MISSING_PINGS"]))
                # cleanup static class attributes
                del Connection.instances[connection_type]
                del Connection.node_id[connection_type]
                del Connection.event_queue[connection_type]
                del Connection.router_queue[connection_type]
                del Connection.listener[connection_type]
                del Connection.port[connection_type]
                logger.warning("Networking shutdown complete (type: %s)..." % connection_type)
        logger.warning("Full networking shutdown complete...")
        
    # class constructor
    def __init__(self, connection_type, addr, active_init, reconnect_try):
        self.termination_lock = RLock()
        self.connection_type = connection_type
        self.addr = addr
        self.instance_id = str(binascii.hexlify(os.urandom(2)), 'ascii')
        self.logger=configure_logger(self.addr, self.instance_id)
        self.is_dead = Event()
        if Connection.settings["ENCRYPT_PACKETS"]:
            self.X25519_key = X25519PrivateKey.generate()
        self.peer_key = None
        self.peer_id = None
        self.pinger_thread = None
        self.watchdog_lock = RLock()
        self.watchdog_counter = Connection.settings["MAX_MISSING_PINGS"]    # connection timeout = MAX_MISSING_PINGS * PING_INTERVAL
        self.watchdog_thread = None
        self.connection_state = 'IDLE'
        self.active_init = active_init
        self.reconnect_thread = None
        self.reconnect_try = reconnect_try
        self.covert_msg_queue_lock = RLock()
        self.covert_msg_queue = deque()
        self.covert_messages_sent = 0
        self.covert_messages_received = 0
        
        self.logger.info("Initializing new connection with %s (connect #%s)" % (str(self.addr), str(self.reconnect_try)))
        if self.active_init:
            Connection.event_queue[self.connection_type].put({"type": "connecting", "data": {"addr": str(self.addr[0]), "port": self.addr[1], "active_init": self.active_init}})
            self._send_init_msg("SYN")
        
        # init watchdog thread to terminate connection after MAX_MISSING_PINGS consecutive failures to receive a ping
        self.watchdog_thread = Thread(name="local::"+Connection.node_id[self.connection_type]+"::_watchdog", target=self._watchdog, daemon=True)
        self.watchdog_thread.start()
    
    def terminate(self):
        with self.termination_lock:
            if not self.is_dead.is_set():
                self.logger.info("Terminating connection (type: %s)..." % self.connection_type)
                self.logger.debug("closing connection...")
                self.is_dead.set()
                with Connection.global_lock:
                    if str(self.addr) in Connection.instances:
                        del Connection.instances[str(self.addr)]
                if self.connection_state == "ESTABLISHED":
                    Connection.router_queue[self.connection_type].put({
                        "_command": "remove_connection",
                        "connection": self
                    })
                if self.pinger_thread and self.pinger_thread.is_alive() and self.pinger_thread != current_thread():
                    self.pinger_thread.join(4.0)
                if self.watchdog_thread and self.watchdog_thread.is_alive() and self.watchdog_thread != current_thread():
                    self.watchdog_thread.join(4.0)
                self.logger.info("Connection (tpye: %s) terminated successfully..." % self.connection_type)
                Connection.event_queue[self.connection_type].put({"type": "disconnected", "data": {"addr": str(self.addr[0]), "port": self.addr[1], "active_init": self.active_init}})
            else:
                self.logger.info("Not terminating already dead connection (type: %s)..." % self.connection_type)
    
    def send_msg(self, msg, call_filters = True):
        if self.connection_state != "ESTABLISHED":                      # not connected
            return
        if call_filters and filters.msg_outgoing(msg, self):            # call filters framework
            return
        if self.is_dead.is_set():
            #raise BrokenPipeError("Connection already closed!")
            return      # ignore send on dead connection (will be "garbage collected" by router command "remove_connection" soon)
        data = self._encrypt(self._pack(msg))
        self._raw_send(data)
    
    def send_covert_msg(self, msg, call_filters = True):
        if call_filters and filters.covert_msg_outgoing(msg, self):         # call filters framework
            return
        if self.is_dead.is_set():
            #raise BrokenPipeError("Connection already closed!")
            return      # ignore send on dead connection (will be "garbage collected" by router command "remove_connection" soon)
        with self.covert_msg_queue_lock:
            self.covert_messages_sent += 1      # increment sent counter
            msg = Message(msg)                  # copy original message before adding message counter
            msg["_covert_messages_counter"] = self.covert_messages_sent
            # encode serialized message as base64 because this doesn't need additional escaping when it is json encoded later on
            self.covert_msg_queue.append(str(base64.b64encode(bytes(self._pack(msg))), 'ascii'))
    
    def get_peer_id(self):
        #return str(self.addr)
        return str(self.peer_id)
    
    def get_peer_ip(self):
        return self.addr[0]
    
    def __repr__(self):
        return "Connection<%s[%s]%s>" % (str(self.peer_id), str(self.instance_id), str(self.addr))
    
    # *** internal methods for connection initialisation ***
    def _send_init_msg(self, flag):
        self.logger.debug("sending init message '%s' from state '%s'..." % (str(flag), str(self.connection_state)))
        data = bytearray(flag, 'ascii')
        if flag == "SYN" or flag == "SYN-ACK":      # add key exchange data to SYN and SYN-ACK messages
            data += bytes(Connection.node_id[self.connection_type], 'ascii')
            if Connection.settings["ENCRYPT_PACKETS"]:
                data += self.X25519_key.public_key().public_bytes()
            else:
                data += os.urandom(32)      # dummy data
        self.connection_state = flag
        self.logger.debug("outgoing packet(%s): %s" % (str(len(data)), str(data)))
        self._raw_send(data)
    
    def _derive_key(self, data):
        try:
            if len(data) != 32:
                raise ValueError("Key material size is not 32 bytes")
            if Connection.settings["ENCRYPT_PACKETS"]:
                # derive symmetric peer_key used to encrypt further communication
                self.peer_key = HKDF(
                    algorithm=hashes.SHA256(32),
                    length=32,
                    salt=None,
                    info=None,
                    backend=default_backend()
                ).derive(self.X25519_key.exchange(X25519PublicKey.from_public_bytes(data)))
        except Exception as e:
            self.logger.warning("Could not derive key on connection init, terminating connection! Exception: %s" % str(e))
            self.terminate()
    
    def _finalize_connection(self):
        # our connection is now established, inform router of the new connection and activate pinger thread
        self.connection_state = "ESTABLISHED"
        self.reconnect_try = 0  # connection successful, reset reconnection counter
        self.pinger_thread = Thread(name="local::"+Connection.node_id[self.connection_type]+"::_pinger", target=self._pinger, daemon=True)
        self.pinger_thread.start()
        self.logger.info("Connection with %s initialized" % str(self.addr))
        Connection.event_queue[self.connection_type].put({"type": "connected", "data": {"addr": str(self.addr[0]), "port": self.addr[1], "active_init": self.active_init}})
        Connection.router_queue[self.connection_type].put({
            "_command": "add_connection",
            "connection": self
        })
    
    # *** middle level stuff ***
    # this is called by our listener for every incoming raw udp packet
    def _incoming(self, packet):
        self.logger.debug("incoming packet(%s): %s" % (str(len(packet)), str(packet)))
        if not self.connection_state == "ESTABLISHED":  # init phase (unencrypted)
            if self.connection_state == "IDLE" and packet[:3] == b"SYN":
                Connection.event_queue[self.connection_type].put({"type": "connecting", "data": {"addr": str(self.addr[0]), "port": self.addr[1], "active_init": self.active_init}})
                self.peer_id = packet[3:39].decode("ascii")
                self._derive_key(packet[39:])
                self._send_init_msg("SYN-ACK")
            elif self.connection_state == "SYN-ACK" and packet[:3] == b"ACK":
                self._finalize_connection()
            elif self.connection_state == "SYN" and packet[:7] == b"SYN-ACK":
                self.peer_id = packet[7:43].decode("ascii")
                self._derive_key(packet[43:])
                self._send_init_msg("ACK")
                self._finalize_connection()
            # this can happen if our first SYN was lost
            elif self.connection_state == "SYN" and packet[:3] == b"SYN":
                self.peer_id = packet[3:39].decode("ascii")
                self._derive_key(packet[39:])
                self._send_init_msg("SYN-ACK")
            else:
                #self.logger.warning("Unknown init packet in state '%s': %s" % (str(self.connection_state), str(packet)))
                pass    # ignore everything else (the connection will be garbage collected by watchdog soon)
        else:   # working phase (encrypted)
            messages = self._unpack(packet)
            for msg in messages:
                if msg.get_type() == "_ping":
                    # update ping watchdog
                    with self.watchdog_lock:
                        self.watchdog_counter = Connection.settings["MAX_MISSING_PINGS"]
                    # process covert messages
                    for covert_msg in self._unpack(base64.b64decode(bytes(msg["covert_messages"], "ascii")), False):
                        # no lock for covert_messages_received needed here because _incoming() is only called by one receiving thread
                        # process only covert message expected next (= not already received AND no gap between last one and this one)
                        if covert_msg["_covert_messages_counter"] == self.covert_messages_received + 1:
                            self.covert_messages_received = covert_msg["_covert_messages_counter"]
                            del covert_msg["_covert_messages_counter"]      # this is only internal, do not expose it to routers or filters
                            if not filters.covert_msg_incoming(covert_msg, self):       # call filters framework
                                Connection.router_queue[self.connection_type].put({"_command": "covert_message_received", "connection": self, "message": covert_msg})
                    # send out ack
                    ack_msg = Message("_ack", {
                        # covert_messages_counter in network byte order (big endian) coded to hex (this is a constant length string)
                        "covert_messages_counter": str(binascii.hexlify(struct.pack("!Q", self.covert_messages_received)), 'ascii')
                    })
                    data = self._encrypt(self._pack(ack_msg))
                    if not self.is_dead.is_set():
                        self._raw_send(data)
                elif msg.get_type() == "_ack":
                    # decode counter
                    (msg["covert_messages_counter"],) = struct.unpack("!Q", binascii.unhexlify(msg["covert_messages_counter"]))
                    with self.covert_msg_queue_lock:
                        # remove all acked messages from outgoing queue
                        while (len(self.covert_msg_queue) and
                        self._unpack(base64.b64decode(bytes(self.covert_msg_queue[0], "ascii")), False)[0]["_covert_messages_counter"] <= msg["covert_messages_counter"]):
                            self.covert_msg_queue.popleft()
                else:
                    if not filters.msg_incoming(msg, self):     # call filters framework
                        Connection.router_queue[self.connection_type].put({"_command": "message_received", "connection": self, "message": msg})
    
    # *** internal threads ***
    @catch_exceptions(logger=logger)
    def _reconnect(self):
        # try to reconnect after (PING_INTERVAL * MAX_MISSING_PINGS) + random(0, 2) seconds
        reconnect = (
            (Connection.settings["PING_INTERVAL"] * Connection.settings["MAX_MISSING_PINGS"]) +
            (float(int.from_bytes(os.urandom(2), byteorder='big', signed=False))/32768.0)
        )
        self.logger.info("Reconnecting in %.3f seconds..." % reconnect)
        if not Connection.reconnections_stopped.wait(reconnect):
            Connection.connect_to(self.connection_type, self.addr[0], self.reconnect_try+1)
        else:
            self.logger.info("Reconnection cancelled...")
    
    @catch_exceptions(logger=logger)
    def _watchdog(self):
        while not self.is_dead.wait(Connection.settings["PING_INTERVAL"]):
            with self.watchdog_lock:
                self.watchdog_counter -= 1
                copy = self.watchdog_counter
            if copy <= 0:
                self.logger.warning("Ping watchdog triggered in connection state '%s'!" % self.connection_state)
                self.terminate()
                if self.active_init and self.reconnect_try < Connection.settings["MAX_RECONNECTS"]:
                    if not Connection.reconnections_stopped.is_set():
                        self.reconnect_thread = Thread(name="local::"+Connection.node_id[self.connection_type]+"::_reconnect", target=self._reconnect, daemon=True)
                        self.reconnect_thread.start()
                    else:
                        self.logger.info("Reconnections disallowed by shutdown, not trying to reconnect...")
                else:
                    self.logger.info("Not active_init or maximum reconnects of %s reached, not trying to reconnect..." % str(Connection.settings["MAX_RECONNECTS"]))
    
    # the pinger utilizes our covert channel filled with messages from covert_msg_queue
    @catch_exceptions(logger=logger)
    def _pinger(self):
        while not self.is_dead.wait(Connection.settings["PING_INTERVAL"]):
            bytes_left = Connection.settings["MAX_COVERT_PAYLOAD"]
            to_send = []
            with self.covert_msg_queue_lock:
                # add covert messages to ping message (unacked or new ones) until the size of MAX_COVERT_PAYLOAD would be exceeded
                while len(self.covert_msg_queue) and len(self.covert_msg_queue[0]) < bytes_left:
                    serialized = self.covert_msg_queue.popleft()
                    to_send.append(serialized)
                    bytes_left -= len(serialized)
                # readd popped messages to our queue, they are removed only when a proper ack is received
                self.covert_msg_queue.extendleft(reversed(to_send))     # retain original order!
            
            # send out ping message
            msg = Message("_ping", {"covert_messages": "".join(to_send), "padding": str().ljust(bytes_left, " ")})
            data = self._encrypt(self._pack(msg))
            if not self.is_dead.is_set():
                self._raw_send(data)
    
    # *** low level stuff (handling raw bytes data) ***
    def _raw_send(self, data):
        drop_packet = numpy.random.choice([True, False], p=[Connection.settings["PACKET_LOSS"], 1-Connection.settings["PACKET_LOSS"]], size=1)[0]
        if not drop_packet:
            try:
                Connection.listener[self.connection_type].get_socket().sendto(data, self.addr)
            except Exception as e:
                self.logger.warning("Could not send packet to '%s', terminating connection! Exception: %s" % (str(self.addr), str(e)))
                self.terminate()
        
    def _encrypt(self, packet):
        if not Connection.settings["ENCRYPT_PACKETS"]:
            return packet
        cipher = AESGCM(self.peer_key)
        nonce = os.urandom(13)
        return nonce + cipher.encrypt(nonce, packet, None)
    
    def _decrypt(self, packet):
        if not Connection.settings["ENCRYPT_PACKETS"]:
            return packet
        cipher = AESGCM(self.peer_key)
        nonce = packet[:13]
        ciphertext = packet[13:]
        try:
            return cipher.decrypt(nonce, ciphertext, None)
        except:
            return None
    
    def _pack(self, msg):
        serialized = bytes(msg) # json string
        self.logger.debug("packed json message(%d): %s" % (len(serialized), serialized))
        size = struct.pack("!Q", len(serialized))	# network byte order (big endian) length of the json string
        return size + serialized
        
    def _unpack(self, packet, decrypt=True):
        if decrypt:
            packet = self._decrypt(packet)
            if not packet:
                self.logger.warning("Could not decrypt packet, ignoring it!")
                return []   # packet cannot be decrypted, ignore it
        # unpack every message object in this packet
        messages = []
        while len(packet) > Connection.len_size:  # fewer bytes than Connection.len_size can only be padding bytes
            data, packet = self._extract(packet, Connection.len_size)
            (json_len,) = struct.unpack("!Q", data)
            if not json_len:
                break;      # only padding bytes coming now, skip them
            self.logger.debug("got new packed message of size %d" % json_len)
            if json_len > 65536:		# 64 KiB
                logger.warning("Ignoring message of size %d > 65536 (64 KiB), raw message content: %s" % (json_len, str(packet)))
                return messages
                #raise ValueError("Message size of %d > 65536 (64 KiB)" % json_len)
            data, packet = self._extract(packet, json_len)
            self.logger.debug("unpacked message json contents: %s" % data.decode("UTF-8"))
            messages.append(Message(data))
        return messages
    
    def _extract(self, data, size):
        if size > len(data):
            raise ValueError("Packet truncated!")
        return data[:size], data[size:]
