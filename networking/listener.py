import socket
import queue
from threading import Thread
import logging
logger = logging.getLogger(__name__)

from .connection import Connection


class Listener:
	node_id = ""
	router_queue = None
	listener_thread = None
	
	def __init__(self, node_id, queue, host):
		self.node_id = node_id
		self.router_queue = queue
		
		logger.debug("initializing listener for node %s" % self.node_id)
		listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		listener_socket.bind((host, 9999))
		listener_socket.listen(4)
		logger.debug("starting listener thread")
		self.listener_thread = Thread(name=self.node_id+"::listener", target=self.listen, args=(listener_socket,))
		self.listener_thread.start()
	
	def listen(self, listener_socket):
		while(True):
			client, addr = listener_socket.accept()
			logger.info("New client connected: %s" % str(addr))
			try:
				con = Connection(self.node_id, self.router_queue, client, addr, True)
				self.router_queue.put({
					"command": "add_connection",
					"connection": con
				})
			except Exception as err:
				logger.error("Got error '%s', while setting up new connection, ignoring this client" % str(err))
