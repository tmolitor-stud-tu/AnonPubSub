import socket
import queue
from threading import Thread
import logging
logger = logging.getLogger(__name__)


def connect_to(node_id, router_queue, host):
	addr = (host, 9999)
	thread = Thread(name="local::"+node_id+"::_connecting_thread", target=_connecting_thread, args=(node_id, router_queue, addr))
	thread.start()
	_connecting_thread

def _connecting_thread(node_id, router_queue, addr):
	logger.debug("connecting thread for %s started" % str(addr))
	try:
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect(addr)
	except socket.error as err:
		logger.info("Got error '%s' while connecting to %s, giving up" % (str(err), str(addr)))
		return
	try:
		con = Connection(node_id, router_queue, sock, addr, True)
		router_queue.put({
			"command": "add_connection",
			"connection": con
		})
		con.start_receiver()
	except Exception as err:
		logger.info("Got error '%s' while connecting to %s, giving up" % (str(err), str(addr)))


#circular imports!
from .connection import Connection
