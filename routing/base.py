from queue import Queue, Empty, Full
from threading import Thread
from threading import Event
import logging
logger = logging.getLogger(__name__)

from networking import Connection


class Router:
    stopped = Event()
    
    def __init__(self, node_id, queue):
        self.queue = queue
        self.node_id = node_id
        self.connections = {}
        self.routing_thread = Thread(name="local::"+self.node_id+"::_routing", target=self._routing)
        self.routing_thread.start()
    
    def stop(self):
        for peer_id, con in self.connections.items():
            con.terminate()
        Router.stopped.set()
        self.queue.put({})      #empty put to wake up routing thread after Router.stopped is set to True
        self.routing_thread.join()
    
    def publish(self, channel, data):
        pass
    
    def subscribe(self, channel, callback):
        pass
    
    def _process_command(self, command):
        pass
    
    def _routing(self):
        logger.debug("routing thread started...");
        while not Router.stopped.isSet():
            try:
                command = self.queue.get(1)      #1 second timeout
                if Router.stopped.isSet():
                    break
            except Empty as err:
                logger.debug("routing queue empty");
                continue
            logger.debug("got routing command: %s" % command["command"])
            self._process_command(command)
            self.queue.task_done()
        logger.debug("routing thread stopped...")
    