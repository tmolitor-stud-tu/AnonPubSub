import socket
import queue
from threading import Thread
import logging
logger = logging.getLogger(__name__)

from .connection import Connection


class Listener(object):
    run = True
    node_id = ""
    router_queue = None
    listener_thread = None
    
    def __init__(self, node_id, queue, host):
        self.node_id = node_id
        self.router_queue = queue
        
        logger.debug("initializing listener for node %s on host %s" % (self.node_id, host))
        listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener_socket.bind((host, 9999))
        listener_socket.settimeout(1)
        listener_socket.listen(4)
        self.listener_thread = Thread(name="local::"+self.node_id+"::_listener", target=self._listener, args=(listener_socket,))
        self.listener_thread.start()
    
    def stop(self):
        self.run = False
        self.listener_thread.join()
    
    def _listener(self, listener_socket):
        logger.debug("listener thread started")
        while self.run:
            try:
                client, addr = listener_socket.accept()
            except socket.timeout:
                continue
            except:
                raise
            logger.info("New client connected: %s" % str(addr))
            # this thread is need to avoid blocking our accept loop if the connection is dropped right after accepting it
            thread = Thread(name="local::"+self.node_id+"::_init_connection", target=self._init_connection, args=(client, addr))
            thread.start()
        logger.debug("listener thread stopped")
    
    def _init_connection(self, client, addr):
        logger.debug("connection initialization thread started")
        try:
            con = Connection(self.node_id, self.router_queue, client, addr, False)
            self.router_queue.put({
                "command": "add_connection",
                "connection": con
            })
            con.start_receiver()
            logger.debug("connection initialized, stopping connection initialization thread")
        except Exception as err:
            logger.error("Got error '%s', while setting up new connection to %s, ignoring this client and stopping this connection initialization thread" % (str(err), str(addr)))
            return
