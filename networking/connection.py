import math
import time
import struct
import socket
import queue
from threading import Thread
import json
import logging
logger = logging.getLogger(__name__)
logger.propagate = False
with open("logger.json", 'r') as logging_configuration_file:
    fmt = json.load(logging_configuration_file)["formatters"]["with_ip"]["format"]
    log_formatter = logging.Formatter(fmt)
log_handler = logging.StreamHandler() 
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

from .message import Message

class LoggingIPFilter(logging.Filter):
    def __init__(self, addr):
        self.addr = addr
    
    def filter(self, record):
        record.ip = str(self.addr[0])
        record.port = int(self.addr[1])
        return True


class Connection:
    in_shutdown = False
    
    def __init__(self, node_id, queue, sock, addr, start_init):
        if Connection.in_shutdown:      #abort connection here
            raise ValueError("Cannot connect on shutdown!")
        self.is_dead = False
        self.node_id = node_id
        self.len_size = len(struct.pack("!Q", 0))	# calculate length of packed unsigned long long int
        self.router_queue = queue
        self.sock = sock
        self.addr = addr
        logger.addFilter(LoggingIPFilter(self.addr))
        
        self.sock.settimeout(16)     #16 second timeout in init phase
        timer=time.perf_counter()
        if start_init:		# start with sending init message
            logger.debug("sending init message to %s" % str(self.addr))
            self.send_msg(Message("init", {"node": self.node_id}))
            if self._receive_init_message():
                raise ValueError("Initialization failed!")	# error happened, stop here
        else:			# wait for init message
            logger.debug("waiting for init message from %s" % str(self.addr))
            if self._receive_init_message():
                raise ValueError("Initialization failed!")	# error happened, stop here
            logger.debug("sending init message to %s" % str(self.addr))
            self.send_msg(Message("init", {"node": self.node_id}))
        
        # set SO_SNDTIMEO to max(1, 2 * RTT)
        timer = math.ceil(time.perf_counter()-timer)
        self.sndtimeo = max(1, 2 * timer)
        logger.debug("RTT: %d, sndtimeo: %d" % (timer, self.sndtimeo))
        timeval = struct.pack("ll", self.sndtimeo, 0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, timeval)
    
    def get_peer_id(self):
        return self.peer_id
    
    def start_receiver(self):
        self.thread = Thread(name="remote::"+self.peer_id+"::_recv_thread", target=self._recv_thread)
        self.thread.start()
    
    def terminate(self):
        logger.info("Terminating connection with %s" % str(self.addr))
        self._close()
        self.thread.join()
        logger.debug("connection with %s terminated successfully" % str(self.addr))
    
    def shutdown():
        Connection.in_shutdown = True
    
    def send_msg(self, msg):
        if self.is_dead:
            raise BrokenPipeError("Connection already closed!")
        serialized = str(msg)
        logger.debug("sending json message(%d): %s" % (len(serialized), serialized))
        data = struct.pack("!Q", len(serialized))	# network byte order (big endian) length of the json string
        data = data + serialized.encode("UTF-8")	# json string
        try:
            self.sock.sendall(data)
        except socket.error as err:
            logger.info("Got error '%s' in socket.sendall, stopping receiver thread and marking connection object as dead" % str(err))
            self._close()
            # closing the socket and setting is_dead to True should eventually stop the thread, just wait for it to be finished doing its cleanup
            #self.thread.stop()
            self.thread.join()
    
    def _close(self):
        self.is_dead = True	    # no locking needed because this is only set to True once and no timing issues are involved in testing this value
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except:
            pass
        try:
            self.sock.close()
        except:
            pass
    
    def _recv_thread(self):
        logger.debug("receiver thread for %s started" % str(self.addr))
        self.sock.settimeout(self.sndtimeo * 2)     #double our send timeout (=4 * RTT)
        while not self.is_dead:
            try:
                msg = self._recv_msg()
                if Connection.in_shutdown:
                    break
                self.router_queue.put({"command": "message_received", "connection": self, "message": msg})
            except socket.timeout as timeout:
                continue;
            except (socket.error, ValueError) as err:
                logger.info("Got error '%s' in receiver thread, stopping thread and marking connection object as dead" % str(err))
                break	# stop receiver thread and mark connection as dead
        if Connection.in_shutdown:
            logger.info("Shutdown in progress, stopping receiver thread and marking connection object as dead")
            self._close()
            return;
        self._close()
        self.router_queue.put({
            "command": "remove_connection",
            "connection": self
        })
        logger.info("Trying to reconnect to %s" % str((self.addr[0], 9999)))
        connect_to(self.node_id, self.router_queue, self.addr[0])
        logger.debug("receiver thread for %s stopped" % str(self.addr))
    
    def _receive_init_message(self):
        try:
            msg = self._recv_msg()
            if msg.get_type() != "init":
                logger.warning("Got unexpected message of type '%s' while initiating connection sequence, marking connection object as dead" % str(mg.get_type()))
                self._close()
                return
            self.peer_id = msg["node"]
            logger.info("Got peer id %s from %s" % (self.peer_id, str(self.addr)))
            if self.peer_id == self.node_id:
                logger.warning("Peer ID %s equals own ID, dropping connection" % self.peer_id)
                raise ValueError("Peer ID equals own ID!")
        except (socket.error, ValueError) as err:
            logger.warning("Got error '%s' while initiating connection sequence, marking connection object as dead" % str(err))
            self._close()
            return True
        return False
    
    def _recv_msg(self):
        data = self._recv_full(self.len_size)
        (json_len,) = struct.unpack("!Q", data)
        logger.debug("got new message of size %d" % json_len)
        if json_len > 4194304:		# 4 MiB
            raise ValueError("Message size of %d > 4194304 (4MiB)" % json_len)
        data = self._recv_full(json_len)
        logger.debug("message json contents: %s" % data.decode("UTF-8"))
        return Message(data)
    
    def _recv_full(self, size):
        data = bytearray()
        while size > 0:
            new_data = self.sock.recv(size)
            if len(new_data) == 0:
                raise BrokenPipeError("Connection broken!")
            size -= len(new_data)
            data = data + new_data
        return data


# circular imports!
from .client_connection import connect_to
