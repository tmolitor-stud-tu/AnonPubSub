import socket
from threading import Thread, current_thread
import logging
logger = logging.getLogger(__name__)


class Listener(object):
    
    def __init__(self, node_id, callback, host, port=9999):
        self.node_id = node_id
        self.callback = callback
        self.run = True
        
        logger.debug("initializing listener for node %s on host %s" % (self.node_id, host))
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.sock.settimeout(1)
        self.listener_thread = Thread(name="local::"+self.node_id+"::_listener", target=self._listener)
        self.listener_thread.start()
    
    def stop(self):
        self.run = False
        if self.listener_thread != current_thread():
            self.listener_thread.join()
        # close socket
        self.sock.close()
    
    def get_socket(self):
        return self.sock
    
    def _listener(self):
        logger.debug("listener thread started")
        while self.run:
            try:
                data, addr = self.sock.recvfrom(65536)
            except socket.timeout:
                continue
            except:
                raise
            self.callback(addr, data)
        logger.debug("listener thread stopped")
