import cherrypy
from pathlib import Path
from threading import Thread
from logging import LogRecord
import os
import queue
import json
import logging
logger = logging.getLogger(__name__)


class Server(object):
    def __init__(self, addr, event_queue, command_queue):
        self.addr = addr
        self.event_queue = event_queue
        self.command_queue = command_queue
        self.in_generator = False
        self.base = "%s/static" % os.path.dirname(os.path.realpath(__file__))
        self.cherrypy_thread = Thread(name="local::cherrypy_master", target=self._run)
        self.cherrypy_thread.start()
    
    def stop(self):
        self.event_queue.put(None)  # wakeup and stop events generator function
        logger.warning("Stopping cherrypy engine...")
        cherrypy.engine.exit()
        self.cherrypy_thread.join()
        logger.warning("Cherrypy engine stopped...")
    
    def _run(self):
        logger.debug("cherrypy master thread started...")
        conf = {
            "global": {
                "server.socket_host": self.addr,
                "server.socket_port": 9980,
                "server.thread_pool": 16,
                "engine.autoreload.on": False,
                "tools.staticdir.on": True,
                "tools.staticdir.dir": self.base,
                "tools.staticdir.index": "index.html",
                "log.screen": False,
                "log.access_file": "",
                "log.error_file": "",
            }
        }
        cherrypy.quickstart(self, config=conf)
        logger.debug("cherrypy master thread terminating...")
    
    @cherrypy.expose
    def events(self):
        cherrypy.response.headers["Content-Type"] = "text/event-stream"
        self.command_queue.put({"command": "_new_http_client"})
        def content():
            if self.in_generator:
                self.event_queue.put(None)  # wakeup and stop OLD events generator function
            self.in_generator = True
            try:
                while True:
                    try:
                        event = self.event_queue.get(True, 4)        # 4 seconds timeout
                        if not event:
                            logger.debug("Stopping generator function!")
                            return
                        elif isinstance(event, LogRecord):
                            yield "event: log\n"
                            yield "data: "
                            yield json.dumps(event.__dict__, separators=(',',':'))
                            yield "\n\n"
                        elif isinstance(event, str):
                            yield "event: "
                            yield event
                            yield "\n\n"
                        elif isinstance(event, dict):
                            yield "event: "
                            yield event["type"]
                            yield "\n"
                            yield "data: "
                            yield json.dumps(event["data"], separators=(',',':'))
                            yield "\n\n"
                        self.event_queue.task_done()
                    except queue.Empty:
                        yield ":ping\n\n"
            except GeneratorExit:
                pass
            self.in_generator = False
        return content()
    events._cp_config = {'response.stream': True}
    
    @cherrypy.expose
    @cherrypy.tools.json_in()
    def command(self):
        cherrypy.response.headers["Content-Type"] = "text/plain"
        self.command_queue.put(cherrypy.request.json)
        return "OK"
