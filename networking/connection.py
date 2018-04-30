import os
import base64
import binascii
import struct
from collections import deque
from threading import Thread, Event, RLock, current_thread, Timer

#crypto imports
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305

#logging
import logging
def configure_logger(addr=None, instance_id=None):
    logger = logging.getLogger("%s[%s]@%s:%s" % (__name__, str(instance_id), str(addr[0]), str(addr[1])))
    return logger
logger = logging.getLogger(__name__)    #default logger

# own classes
from .message import Message
from filter import *


MAX_COVERT_PAYLOAD = 1200   # + always 94 bytes overhead for ping added to final packet
PING_INTERVAL = 0.25
MAX_MISSING_PINGS = 16      # consecutive missing pings, connection timeout will be MAX_MISSING_PINGS * PING_INTERVAL
MAX_RECONNECTS = 3          # maximum consecutive reconnects after a connection failed
ENCRYPT_PACKETS = True

class Connection(object):
    reconnections_stopped = Event()
    node_id = None
    router_queue = None
    listener = None
    instances_lock = RLock()
    instances = {}
    len_size = len(struct.pack("!Q", 0))	# calculate length of packed unsigned long long int
    
    # public static method to initialize networking
    @staticmethod
    def init(node_id, queue, host):
        Connection.node_id = node_id
        Connection.router_queue = queue
        Connection.listener = Listener(node_id, host)
    
    # public static factory method for new outgoing instances
    @staticmethod
    def connect_to(host, reconnect_try=0):
        return Connection._new((host, 9999), True, reconnect_try)
    
    # internal static factory method for incoming packets used by listener class
    @staticmethod
    def _incoming_data(addr, data):
        con = Connection._new(addr, False, 0)
        con._incoming(data)
    
    # internal static factory method doing the actual work
    @staticmethod
    def _new(addr, active_init, reconnect_try):
        if not Connection.listener:
            raise ValueError("Network not initialized, call Connection.init() first!")
        with Connection.instances_lock:
            if str(addr) not in Connection.instances or Connection.instances[str(addr)].is_dead.is_set():
                Connection.instances[str(addr)] = Connection(addr, active_init, reconnect_try)
            con = Connection.instances[str(addr)]
        if active_init:
            con.active_init = active_init   # force right value (used on reconnect)
        return con
    
    # public static method to terminate all connections
    @staticmethod
    def shutdown():
        # stop listener
        Connection.listener.stop()
        # stop reconnections from happening
        Connection.reconnections_stopped.set()
        # terminate all connections (has to be done AFTER stopping the listener so to not create new connections by incoming packets)
        with Connection.instances_lock:
            for addr, con in list(Connection.instances.items()):
                con.terminate()
        # close socket
        Connection.listener.get_socket().close()
        # cleanup static class attributes
        Connection.listener = None
        Connection.node_id = None
        Connection.router_queue = None
        
    # class constructor
    def __init__(self, addr, active_init, reconnect_try):
        self.addr = addr
        self.instance_id = str(binascii.hexlify(os.urandom(2)), 'ascii')
        self.logger=configure_logger(self.addr, self.instance_id)
        self.is_dead = Event()
        self.X25519_key = X25519PrivateKey.generate()
        self.peer_key = None
        self.peer_id = None
        self.pinger_thread = None
        self.covert_msg_queue = deque()
        self.watchdog_lock = RLock()
        self.watchdog_counter = MAX_MISSING_PINGS    # connection timeout = MAX_MISSING_PINGS * PING_INTERVAL
        self.watchdog_thread = None
        self.connection_state = 'IDLE'
        self.active_init = active_init
        self.reconnect_thread = None
        self.reconnect_try = reconnect_try
        
        self.logger.info("Initializing new connection with %s (connect #%s)" % (str(self.addr), str(self.reconnect_try)))
        if self.active_init:
            self._send_init_msg("SYN")
        
        # init watchdog thread to terminate connection after MAX_MISSING_PINGS consecutive failures to receive a ping
        self.watchdog_thread = Thread(name="local::"+Connection.node_id+"::_watchdog", target=self._watchdog)
        self.watchdog_thread.start()
    
    def terminate(self):
        if self.is_dead.is_set():
            return
        self.logger.info("Terminating connection")
        self.is_dead.set()
        with Connection.instances_lock:
            if str(self.addr) in Connection.instances:
                del Connection.instances[str(self.addr)]
        if self.connection_state == "ESTABLISHED":
            Connection.router_queue.put({
                "command": "remove_connection",
                "connection": self
            })
        if self.pinger_thread and self.pinger_thread != current_thread():
            self.pinger_thread.join()
        if self.watchdog_thread and self.watchdog_thread != current_thread():
            self.watchdog_thread.join()
        if self.reconnect_thread and self.reconnect_thread != current_thread():
            self.reconnect_thread.join()
        self.logger.debug("connection terminated successfully")
    
    def send_msg(self, msg):
        if self.is_dead.is_set():
            raise BrokenPipeError("Connection already closed!")
        data = self._encrypt(self._pack(msg))
        Connection.listener.get_socket().sendto(data, self.addr)
    
    def send_covert_msg(self, msg):
        if self.is_dead.is_set():
            raise BrokenPipeError("Connection already closed!")
        self.covert_msg_queue.append(str(base64.b64encode(bytes(self._pack(msg))), 'UTF-8'))
    
    def get_peer_id(self):
        #return str(self.addr)
        return str(self.peer_id)
    
    def __str__(self):
        return "Connection<%s@%s%s>" % (str(self.peer_id), str(self.instance_id), str(self.addr))
    
    # *** internal methods for connection initialisation ***
    def _send_init_msg(self, flag):
        self.logger.debug("sending init message '%s' from state '%s'..." % (str(flag), str(self.connection_state)))
        data = bytearray(flag, 'ascii')
        if flag == "SYN" or flag == "SYN-ACK":      # add key exchange data to SYN and SYN-ACK messages
            data += bytes(Connection.node_id, 'ascii')
            data += self.X25519_key.public_key().public_bytes()
        self.connection_state = flag
        self.logger.debug("outgoing packet(%s): %s" % (str(len(data)), str(data)))
        Connection.listener.get_socket().sendto(data, self.addr)
    
    def _derive_key(self, data):
        try:
            if len(data) != 32:
                raise ValueError("Key material size is not 32 bytes")
            # derive symmetric peer_key used to encrypt further communication
            self.peer_key = HKDF(
                algorithm=hashes.BLAKE2s(32),
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
        Connection.router_queue.put({
            "command": "add_connection",
            "connection": self
        })
        self.pinger_thread = Thread(name="local::"+Connection.node_id+"::_pinger", target=self._pinger)
        self.pinger_thread.start()
        self.logger.info("Connection with %s initialized" % str(self.addr))
    
    # *** middle level stuff ***
    # this is called by our listener for every incoming raw udp packet
    def _incoming(self, packet):
        self.logger.debug("incoming packet(%s): %s" % (str(len(packet)), str(packet)))
        if not self.connection_state == "ESTABLISHED":  # init phase (unencrypted)
            if self.connection_state == "IDLE" and packet[:3] == b"SYN":
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
                if msg.get_type() == "ping":
                    # update ping watchdog
                    with self.watchdog_lock:
                        self.watchdog_counter = MAX_MISSING_PINGS
                    # process covert messages
                    for covert_msg in self._unpack(base64.b64decode(bytes(msg["covert_messages"], "UTF-8")), False):
                        Connection.router_queue.put({"command": "covert_message_received", "connection": self, "message": covert_msg})
                else:
                    Connection.router_queue.put({"command": "message_received", "connection": self, "message": msg})
    
    def _reconnect(self):
        # try to reconnect after (PING_INTERVAL * MAX_MISSING_PINGS) + random(0, 2) seconds
        reconnect = (PING_INTERVAL * MAX_MISSING_PINGS) + (float(int.from_bytes(os.urandom(2), byteorder='big', signed=False))/32768.0)
        self.logger.info("Reconnecting in %.3f seconds..." % reconnect)
        if not Connection.reconnections_stopped.wait(reconnect):
            Connection.connect_to(self.addr[0], self.reconnect_try+1)
        else:
            self.logger.info("Reconnection cancelled...")
    
    # *** internal threads ***
    def _watchdog(self):
        while not self.is_dead.wait(PING_INTERVAL):
            with self.watchdog_lock:
                self.watchdog_counter -= 1
                copy = self.watchdog_counter
            if copy <= 0:
                self.logger.warning("Ping watchdog triggered in connection state '%s'!" % self.connection_state)
                self.terminate()
                if self.active_init and self.reconnect_try < MAX_RECONNECTS:
                    if not Connection.reconnections_stopped.is_set():
                        self.reconnect_thread = Thread(name="local::"+Connection.node_id+"::_reconnect", target=self._reconnect)
                        self.reconnect_thread.start()
                    else:
                        self.logger.info("Reconnections disallowed by shutdown, not trying to reconnect...")
                else:
                    self.logger.info("Not active_init or maximum reconnects of %s reached, not trying to reconnect..." % str(MAX_RECONNECTS))
    
    # the pinger utilizes our covert channel filled with messages from covert_msg_queue
    def _pinger(self):
        while not self.is_dead.wait(PING_INTERVAL):
            data = ""
            bytes_left = MAX_COVERT_PAYLOAD
            while len(self.covert_msg_queue) and len(self.covert_msg_queue[0]) < bytes_left:
                serialized = self.covert_msg_queue.popleft()
                data += serialized
                bytes_left -= len(serialized)
            msg = Message("ping", {"covert_messages": data, "padding": str().ljust(bytes_left, " ")})
            data = self._encrypt(self._pack(msg))
            if not self.is_dead.is_set():
                Connection.listener.get_socket().sendto(data, self.addr)
    
    # *** low level stuff (handling raw bytes data) ***
    def _encrypt(self, packet):
        if not ENCRYPT_PACKETS:
            return packet
        chacha = ChaCha20Poly1305(self.peer_key)
        nonce = os.urandom(12)
        return nonce + chacha.encrypt(nonce, packet, None)
    
    def _decrypt(self, packet):
        if not ENCRYPT_PACKETS:
            return packet
        chacha = ChaCha20Poly1305(self.peer_key)
        nonce = packet[:12]
        ciphertext = packet[12:]
        try:
            return chacha.decrypt(nonce, ciphertext, None)
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
                raise ValueError("Message size of %d > 65536 (64 KiB)" % json_len)
            data, packet = self._extract(packet, json_len)
            self.logger.debug("unpacked message json contents: %s" % data.decode("UTF-8"))
            messages.append(Message(data))
        return messages
    
    def _extract(self, data, size):
        if size > len(data):
            raise ValueError("Packet truncated!")
        return data[:size], data[size:]


# circular imports!
from .listener import Listener
