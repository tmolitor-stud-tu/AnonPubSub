#!/usr/bin/python3
# import everything that is needed
import uuid
from queue import Queue, Empty
import argparse
import urllib.request as urllib2
from urllib.parse import quote_plus
import sys
import signal
import time

# our own modules come here
import control
import networking
import routing


# parse commandline
parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, description="AnonPubSub node.\nHTTP Control Port: 9980\nNode Communication Port: 9999")
parser.add_argument("-l", "--listen", metavar='HOSTNAME', help="Local hostname or IP to listen on (Default: 0.0.0.0 e.g. any)", default="0.0.0.0")
parser.add_argument("--log", metavar='LOGLEVEL', help="Loglevel to log", default="INFO")
args = parser.parse_args()

# initialize incoming and outgoing webserver queues
command_queue = Queue()
event_queue = Queue()

# configure logging as early as possible
import json, logging, logging.config
with open("logger.json", 'r') as logging_configuration_file:
    logger_config=json.load(logging_configuration_file)
logger_config["handlers"]["stderr"]["level"] = args.log
logger_config["handlers"]["queue"]["queue"] = event_queue
logging.config.dictConfig(logger_config)
logger = logging.getLogger()
logger.info('Logger configured')

server = control.Server(args.listen, event_queue, command_queue)

# initialize global vars
queue = None
router = None

# use this to cleanup the system and exit
def cleanup_and_exit(code=0):
    global router, server
    signal.signal(signal.SIGINT, signal.SIG_IGN)    # ignore SIGINT while shutting down
    logger.warning("Shutting down!")
    if router:
        router.stop()
    networking.Connection.shutdown()
    server.stop()
    sys.exit(code)

# cleanup on sigint (CTRL-C)
def sigint_handler(sig, frame):
    logger.warning("Got interrupted, shutting down!")
    cleanup_and_exit(0)
signal.signal(signal.SIGINT, sigint_handler)


# our mainloop
node_id = str(uuid.uuid4())
to_publish = {}
received = {}
def apply_settings(data, path, apply_to):
    for item in path:
        if item in data:
            data = data[item]
        else:
            return
    if not apply_to.settings:
        apply_to.settings = {}
    for key, value in dict(data).items():
        apply_to.settings[key] = value
def subscribe(command, router, received):
    if router:  # only subscribe if we have a router
        if command["channel"] not in received or True:
            received[command["channel"]] = 0
            def dummy_receiver(data):
                if data != received[command["channel"]] + 1:
                    logger.error("UNEXPECTED DATA RECEIVED (%s != %s)!!!" % (str(data), str(received[command["channel"]] + 1)))
                received[command["channel"]] = data
            router.subscribe(command["channel"], dummy_receiver)
            event_queue.put({"type": "subscribed", "data": {"channel": command["channel"]}})
        else:
            event_queue.put({"type": "already_subscribed", "data": {"channel": command["channel"]}})
    else:
        logger.error("Cannot subscribe to channel '%s': no router initialized!" % str(command["channel"]))
        event_queue.put({"type": "subscribe_failed", "data": {"channel": command["channel"], "error": "router uninitialized"}})
event_queue.put({"type": "new_node_id", "data": {"node_id": node_id}})
while True:
    # periodically publish a simple counter on all configured channels (about every second)
    for channel in to_publish:
        if router:  # only publish if we have a router
            router.publish(channel, to_publish[channel]);
            to_publish[channel] += 1
        else:
            logger.error("Cannot publish on channel '%s': no router initialized!" % str(channel))
            event_queue.put({"type": "publish_failed", "data": {"channel": channel, "error": "router uninitialized"}})
    
    # process UI commands
    try:
        command = command_queue.get(True, 1)   # 1 second timeout
    except Empty as err:
        #logger.debug("main command queue empty")
        continue
    if command["command"][:1] != "_":       # don't log internal commands
        logger.info("Got GUI command: %s" % str(command))
    if command["command"] == "start":
        if not router:
            # try to determine router class
            try:
                router_class = getattr(routing, command["router"])
            except AttributeError:
                logger.error("Cannot start router '%s': unknown" % str(command["router"]))
                continue
            
            # apply settings if present
            apply_settings(command, ["settings", "networking", "Connection"], networking.Connection)
            apply_settings(command, ["settings", "routing", "Router"], routing.Router)
            apply_settings(command, ["settings", "routing", command["router"]], router_class)
            
            # initialize networking and router
            queue = Queue()
            if "regenerate_node_id" in command and command["regenerate_node_id"]:
                node_id = str(uuid.uuid4())
                event_queue.put({"type": "new_node_id", "data": {"node_id": node_id}})
            networking.Connection.init(node_id, queue, args.listen)
            router = router_class(node_id, queue)
            event_queue.put("start_complete")
        else:
            logger.error("Cannot start new router '%s': old router still initialized" % str(command["router"]))
            event_queue.put({"type": "router_already_initialized", "data": {"new_router": command["router"], "old_router": router.__class__.__name__}})
    elif command["command"] == "stop":
        if router:
            router.stop()
            router = None
        networking.Connection.shutdown()
        queue = None
        to_publish = {}
        event_queue.put("stop_complete")
    elif command["command"] == "reset":
        event_queue.put("reset_pending")
        cleanup_and_exit(0)     # the startup script will restart this node after a few seconds
    elif command["command"] == "connect":
        if router:  # router and network are initialized at once, if we have no router our network is down, too
            networking.Connection.connect_to(command["addr"])
            event_queue.put({"type": "connection_sequence_started", "data": {"addr": command["addr"], "status": "ok"}})
        else:
            logger.error("Cannot connect to peer at %s: network not initialized!" % str(command["addr"]))
            event_queue.put({"type": "connection_sequence_started", "data": {"addr": command["addr"], "status": "error", "error": "router uninitialized"}})
    elif command["command"] == "publish":
        to_publish[command["channel"]] = 0
        event_queue.put({"type": "published", "data": {"channel": command["channel"]}})
    elif command["command"] == "subscribe":
        subscribe(command, router, received)
    elif command["command"] == "dump":
        def cb(state):
            event_queue.put({"type": "dump", "data": state})
        if router:
            router.dump(cb)
    elif command["command"] == "_new_http_client":
        event_queue.put({"type": "new_node_id", "data": {"node_id": node_id}})
    else:
        logger.error("Ignoring unknown GUI command: %s" % str(command))
        event_queue.put({"type": "unknown_gui_command", "data": {"command": command}})
    command_queue.task_done()

