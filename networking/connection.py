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
	len_size = 0
	
	router_queue = None
	sock = None
	addr = None
	
	is_dead = False
	node_id = ""
	peer_id = ""
	
	def __init__(self, node_id, queue, sock, addr, do_init):
		self.is_dead = False
		self.node_id = node_id
		self.len_size = len(struct.pack("!Q", 0))	# calculate length of packed unsigned long long int
		self.router_queue = queue
		self.sock = sock
		self.addr = addr
		logger.addFilter(LoggingIPFilter(self.addr))
		
		if do_init:		# start with sending init message
			logger.debug("sending init message to %s" % str(self.addr))
			self.send_msg(Message("init", {"node": self.node_id}))
			self._receive_init_message()
		else:			# wait for init message
			logger.debug("waiting for init message from %s" % str(self.addr))
			self._receive_init_message()
			logger.debug("sending init message to %s" % str(self.addr))
			self.send_msg(Message("init", {"node": self.node_id}))
		
		logger.debug("starting receiver thread for %s" % str(self.addr))
		self.thread = Thread(name=self.peer_id+"::_recv_thread", target=self._recv_thread)
		self.thread.start()
	
	def get_peer_id(self):
		return self.peer_id
	
	def send_msg(self, msg):
		if self.is_dead:
			raise BrokenPipeError("Connection already closed!")
		serialized = str(msg)
		logger.debug("sending json message(%d): %s" % (len(serialized), serialized))
		data = struct.pack("!Q", len(serialized))	# network byte order (big endian) length of the json string
		data = data + serialized.encode("UTF-8")	# json string
		self.sock.sendall(data)
	
	def _recv_thread(self):
		while True:
			try:
				msg = self._recv_msg()
				self.router_queue.put({"command": "message_received", "connection": self, "message": msg})
			except (socket.error, ValueError) as err:
				logger.info("Got error '%s' in receiver thread, stopping thread and marking connection object as dead" % str(err))
				break	# stop receiver thread and mark connection as dead
		self.is_dead = True		# no locking needed because this is only written once and no timing issues are involved in testing this value
		self.sock.close()
	
	def _receive_init_message(self):
		try:
			msg = self._recv_msg()
			if msg.get_type() != "init":
				logger.warning("Got unexpected message of type '%s' while initiating connection sequence, marking connection object as dead" % str(mg.get_type()))
				self.is_dead = True
				self.sock.close()
				return
			self.peer_id = msg["node"]
			logger.info("Got peer id %s from %s" % (self.peer_id, str(self.addr)))
		except (socket.error, ValueError) as err:
			logger.warning("Got error '%s' while initiating connection sequence, marking connection object as dead" % str(err))
			self.is_dead = True
			self.sock.close()
			raise
	
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
			size -= len(new_data)
			data = data + new_data
		return data
