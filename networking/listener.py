import socket
from threading import Thread, current_thread
import logging
logger = logging.getLogger(__name__)

# own classes
from utils import catch_exceptions


class Listener(object):
    
    def __init__(self, listener_type, node_id, callback, host, port=9999):
        self.listener_type = listener_type
        self.node_id = node_id
        self.callback = callback
        self.run = True
        
        logger.debug("Initializing %s listener for node %s on %s" % (self.listener_type, self.node_id, str((host, port))))
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.sock.settimeout(1)
        self.listener_thread = Thread(name="local::"+self.node_id+"::_listener", target=self._listener, daemon=True)
        self.listener_thread.start()
    
    def stop(self):
        self.run = False
        if self.listener_thread != current_thread() and self.listener_thread.is_alive():
            self.listener_thread.join(4.0)
        # close socket
        self.sock.close()
    
    def get_socket(self):
        return self.sock
    
    @catch_exceptions(logger=logger)
    def _listener(self):
        logger.debug("%s listener thread started" % self.listener_type)
        while self.run:
            try:
                data, addr = self.sock.recvfrom(65536)
            except socket.timeout:
                continue
            except:
                raise
            self.callback(self.listener_type, addr, data)
        logger.debug("%s listener thread stopped" % self.listener_type)
