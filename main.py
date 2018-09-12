#!/usr/bin/python3
# import everything that is needed
import uuid
from queue import Queue, Empty
import argparse
import sys
import signal
import time

# our own modules come here
import control
import networking
import routing
import utils
import filters


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
logger.info('Logger configured...')

# start http server
server = control.Server(args.listen, event_queue, command_queue)

# initialize global vars
queue = None
router = None
group_router = None
group_queue = None

# use this to cleanup the system and exit
def cleanup_and_exit(code=0):
    global router, server
    try:
        logger.warning("Shutting down!")
        time.sleep(1);          # wait some time to give other threads a chance to deliver pending events via SSE
        if router:
            router.stop()
        networking.Connection.shutdown()
        server.stop()
    except KeyboardInterrupt as err:        # subsequent keyboard interrupts trigger fast shutdown (kill mode)
        logger.warning("Got interrupted again, killing myself!!")
    sys.exit(code)


# our mainloop and some aux functions it uses
node_id = str(uuid.uuid4())
logger.info("My node id is now '%s'..." % node_id)
all_leds = utils.init_leds(node_id, event_queue)
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
def fail_command(command, error):
    global command_failed
    command_failed = True
    if command["_command"][:1] != "_":
        event_queue.put({"type": "command_failed", "data": {"id": command["_id"], "error": error}})
    logger.error(error)
def subscribe(command, router, received):    
    received[command["channel"]] = 0
    def dummy_receiver(data):
        event_queue.put({"type": "data", "data": {"received": data, "expected": received[command["channel"]] + 1}})
        if data != received[command["channel"]] + 1:
            logger.warning("UNEXPECTED DATA RECEIVED (%s != %s)!!!" % (str(data), str(received[command["channel"]] + 1)))
        received[command["channel"]] = data
    router.subscribe(command["channel"], dummy_receiver)
try:
    while True:
        # periodically publish a simple counter on all configured channels (about every second or every incoming command (whichever comes first))
        for channel in to_publish:
            if router:  # only publish if we have a router
                # try to publish via cover group and publish directly if we are in no cover group for this channel
                if not group_router.publish(channel, to_publish[channel]):
                    router.publish(channel, to_publish[channel]);
                to_publish[channel] += 1
            else:
                logger.error("Cannot publish on channel '%s': no router initialized!" % str(channel))
        
        # process UI commands
        command_failed = False
        try:
            command = command_queue.get(True, 1)   # 1 second timeout
        except Empty as err:
            #logger.debug("main command queue empty")
            continue
        if command["_command"][:1] != "_":       # don't log internal commands
            logger.debug("Got GUI command: %s" % str(command))
        if command["_command"] == "start":
            if not router:
                # try to determine router class
                try:
                    router_class = getattr(routing, command["router"])
                except AttributeError:
                    fail_command(command, "Cannot start router '%s': unknown" % str(command["router"]))
                    continue
                
                # apply settings if present
                apply_settings(command["settings"], ["networking", "Connection"], networking.Connection)
                apply_settings(command["settings"], ["routing", "Router"], routing.Router)
                apply_settings(command["settings"], ["routing", "GroupRouter"], routing.GroupRouter)
                apply_settings(command["settings"], ["routing", command["router"]], router_class)
                
                # initialize networking and routers and update filter attributes with routers
                queue = Queue()
                group_queue = Queue()
                networking.Connection.init("normal", node_id, queue, args.listen, 9999, event_queue)
                networking.Connection.init("group", node_id, group_queue, args.listen, 9998, event_queue)
                router = router_class(node_id, queue)
                group_router = routing.GroupRouter(node_id, group_queue, router)
                filters.update_attributes({"router": router, "group_router": group_router})
            else:
                fail_command(command, "Cannot start new router '%s': old router still initialized" % str(command["router"]))
        elif command["_command"] == "stop":
            if router:
                group_router.stop()
                router.stop()
                router = None
                group_router = None
            networking.Connection.shutdown()
            queue = None
            group_queue = None
            to_publish = {}
            received = {}
        elif command["_command"] == "reset":
            logger.debug("GUI command completed: %s" % str(command["_id"]))
            event_queue.put({"type": "command_completed", "data": {"id": command["_id"]}})
            command_queue.task_done()
            cleanup_and_exit(0)     # the startup script will restart this node after a few seconds
        elif command["_command"] == "connect":
            if router:  # router and network are initialized at once, if we have no router our network is down, too
                networking.Connection.connect_to("normal", command["addr"])
            else:
                fail_command(command, "Cannot connect to peer at %s: network not initialized!" % str(command["addr"]))
        elif command["_command"] == "disconnect":
            if router:  # router and network are initialized at once, if we have no router our network is down, too
                networking.Connection.disconnect_from("normal", command["addr"])
            else:
                fail_command(command, "Cannot disconnect from peer at %s: network not initialized!" % str(command["addr"]))
        elif command["_command"] == "publish":
            if router:
                to_publish[command["channel"]] = 0
            else:
                fail_command(command, "Cannot publish on channel '%s': no router initialized!" % str(command["channel"]))
        elif command["_command"] == "unpublish":
            if router:
                if command["channel"] in to_publish:
                    del to_publish[command["channel"]]
                router.unpublish(command["channel"])
            else:
                fail_command(command, "Cannot unpublish on channel '%s': no router initialized!" % str(command["channel"]))
        elif command["_command"] == "subscribe":
            if router:  # only subscribe if we have a router
                subscribe(command, router, received)
            else:
                fail_command(command, "Cannot subscribe to channel '%s': no router initialized!" % str(command["channel"]))
        elif command["_command"] == "unsubscribe":
            if router:  # only unsubscribe if we have a router
                router.unsubscribe(command["channel"])
            else:
                fail_command(command, "Cannot subscribe to channel '%s': no router initialized!" % str(command["channel"]))
        elif command["_command"] == "dump":
            def cb(state):
                event_queue.put({"type": "state", "data": state})
            if router:
                router.dump(cb)
        elif command["_command"] == "load_filters":
            # load filter definitions
            retval = filters.load(command["code"], {"leds": all_leds, "router": router})
            if retval:
                fail_command(command, retval)
        elif command["_command"] == "create_group":
            if group_router:
                group_router.create_group(command["channel"], command["ips"], command["interval"])
            else:
                fail_command(command, "Cannot create covergroup for channel '%s': no group_router initialized!" % str(command["channel"]))
        elif command["_command"] == "_new_http_client":
            event_queue.put({"type": "node_id", "data": {"node_id": node_id}})
        else:
            fail_command(command, "Ignoring unknown GUI command: %s" % str(command["_id"]))
        if command["_command"][:1] != "_" and not command_failed:       # don't ack internal or failed commands
            logger.debug("GUI command completed: %s" % str(command["_id"]))
            event_queue.put({"type": "command_completed", "data": {"id": command["_id"]}})
        command_queue.task_done()
except KeyboardInterrupt as err:
    logger.warning("Got interrupted, shutting down!")
    cleanup_and_exit(0)    
except Exception as err:
    logger.exception(err)
    logger.error("Shutting down immediately due to exceptionin main thread!")
    cleanup_and_exit(1)     # indicate unclean shutdown
