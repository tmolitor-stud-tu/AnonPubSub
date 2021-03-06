import cherrypy
from functools import wraps
from pathlib import Path
from threading import Thread, Event
from logging import LogRecord
import uuid
import os
import queue
import json
import logging
logger = logging.getLogger(__name__)

# own classes
import filters

# needed for pretty serialisation
from networking import Connection
from utils import catch_exceptions

class ComplexJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, Connection):
            return str(obj)
        return repr(obj)

def handle_cors(func):
    @wraps(func)
    def with_cors(*args, **kwargs):
        if "Origin" in cherrypy.request.headers:
            cherrypy.response.headers["Access-Control-Allow-Origin"] = cherrypy.request.headers["Origin"]
        if "Access-Control-Request-Method" in cherrypy.request.headers:
            cherrypy.response.headers["Access-Control-Allow-Methods"] = cherrypy.request.headers["Access-Control-Request-Method"]
        if "Access-Control-Request-Headers" in cherrypy.request.headers:
            cherrypy.response.headers["Access-Control-Allow-Headers"] = cherrypy.request.headers["Access-Control-Request-Headers"]
        return func(*args, **kwargs)
    return with_cors

class Server(object):
    def __init__(self, addr, event_queue, command_queue):
        self.addr = addr
        self.event_queue = event_queue
        self.command_queue = command_queue
        self.no_generator_running = Event()
        self.no_generator_running.set()
        self.base = "%s/static" % os.path.dirname(os.path.realpath(__file__))
        self.cherrypy_thread = Thread(name="local::cherrypy_master", target=self._run, daemon=True)
        self.cherrypy_thread.start()
    
    def stop(self):
        self.event_queue.put(None)  # wakeup and stop events generator function
        logger.warning("Stopping cherrypy engine...")
        cherrypy.engine.exit()
        self.cherrypy_thread.join(8.0)
        logger.warning("Cherrypy engine stopped...")
    
    @catch_exceptions(logger=logger)
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
    @handle_cors
    def events(self):
        if cherrypy.request.method == "GET":
            cherrypy.response.headers["Content-Type"] = "text/event-stream"
            self.command_queue.put({"_command": "_new_http_client"})
            def content():
                if not self.no_generator_running.is_set():
                    logger.debug("Sending stop signal to old SSE generator...")
                    self.event_queue.put(None)  # wakeup and stop OLD events generator function
                    # wait for generator being stopped
                    logger.debug("Waiting for old SSE generator to exit...")
                    while not self.no_generator_running.wait(60):
                        pass
                self.no_generator_running.clear()
                
                if self.event_queue.qsize() > 128:
                    logger.debug("Event queue is too big (~ %d entries), removing all but 128 entries..." % self.event_queue.qsize())
                    while self.event_queue.qsize() > 128:
                        self.event_queue.get_nowait()
                        self.event_queue.task_done()
                
                logger.debug("Starting SSE stream (~ %d entries in event queue)..." % self.event_queue.qsize())
                try:
                    while True:
                        try:
                            event = self.event_queue.get(True, 4)       # 4 seconds timeout
                            if filters.gui_event_outgoing(event):       # allow gui events to be filtered, too
                                continue
                            if not event:
                                logger.debug("Stopping SSE stream...")
                                break
                            elif isinstance(event, LogRecord):
                                yield "data: "
                                yield json.dumps({"event": "log", "data": event.__dict__}, separators=(',',':'))
                                yield "\n\n"
                            elif isinstance(event, str):
                                yield "data: "
                                yield json.dumps({"event": event}, separators=(',',':'), cls=ComplexJSONEncoder)
                                yield "\n\n"
                            elif isinstance(event, dict):
                                yield "data: "
                                yield json.dumps({"event": event["type"], "data": event["data"]}, separators=(',',':'), cls=ComplexJSONEncoder)
                                yield "\n\n"
                            self.event_queue.task_done()
                        except queue.Empty:
                            yield ":ping\n\n"       # this will be an sse comment
                except GeneratorExit:
                    pass
                self.no_generator_running.set()
                logger.debug("SSE stream stopped...")
            return content()
    events._cp_config = {'response.stream': True}
    
    @cherrypy.expose
    @cherrypy.tools.json_in()
    @handle_cors
    def command(self):
        if cherrypy.request.method == "POST":
            cherrypy.response.headers["Content-Type"] = "text/plain"
            cherrypy.request.json["_id"] = str(uuid.uuid4())
            self.command_queue.put(cherrypy.request.json)
            return "OK\n%s" % cherrypy.request.json["_id"]
        return
