#!/usr/bin/python3
#import everything that is needed
import uuid
from queue import Queue
import argparse
import urllib.request as urllib2
from urllib.parse import quote_plus
import sys
import signal
import time

#our own modules come here
import networking
import routing


#parse commandline
parser = argparse.ArgumentParser(description='CoolOverlay node.')
parser.add_argument("-l", "--listen", metavar='HOSTNAME', help="Local hostname or IP to listen on", default="localhost")
parser.add_argument("-b", "--bootstrap", metavar='URL', help="Load JSON encoded bootstrap file", default="http://localhost/cool_overlay.php")
parser.add_argument("-u", "--uuid", metavar='UUID', help="Node UUID (default value is randomly generated)", default=str(uuid.uuid4()))
parser.add_argument("-p", "--publish", metavar='TOPIC', help="Topic to publish")
parser.add_argument("-s", "--subscribe", metavar='TOPIC', help="Topic to subscribe to")
parser.add_argument("--log", metavar='LOGLEVEL', help="Loglevel to log", default="DEBUG")
parser.add_argument("--flooding", help="Use Flooding router", action='store_true')
parser.add_argument("--randomwalk", help="Use Randomwalk router", action='store_true')
parser.add_argument("--gossiping", help="Use Gossiping router", action='store_true')
parser.add_argument("--aco", help="Use ACO router (Default)", action='store_true', default=True)
args = parser.parse_args()

#first of all: configure logging
import json, logging, logging.config
with open("logger.json", 'r') as logging_configuration_file:
    logger_config=json.load(logging_configuration_file)
logger_config["root"]["level"]=args.log
logging.config.dictConfig(logger_config)
logger = logging.getLogger()
logger.info('Logger configured')

#initialize global vars
queue = None
router = None

#use this to cleanup the system and exit
def cleanup_and_exit(code=0):
    signal.signal(signal.SIGINT, signal.SIG_IGN)    #ignore SIGINT while shutting down
    logger.warning("Shutting down!")
    if router:
        router.stop()
    networking.Connection.shutdown()
    sys.exit(code)

#cleanup on sigint (CTRL-C)
def sigint_handler(sig, frame):
    logger.warning("Got interrupted, shutting down!")
    cleanup_and_exit(0)
signal.signal(signal.SIGINT, sigint_handler)

#initialize our network listener and router
queue = Queue()
networking.Connection.init(args.uuid, queue, args.listen)
if args.flooding:
    router = routing.Flooding(args.uuid, queue)
elif args.randomwalk:
    router = routing.Randomwalk(args.uuid, queue)
elif args.gossiping:
    router = routing.Gossiping(args.uuid, queue)
elif args.aco:
    router = routing.ACO(args.uuid, queue)
else:
    logger.error("Unknown router specified!")
    cleanup_and_exit(2)

#load bootstrap file defining initial connections and connect to the listed nodes
try:
    url = args.bootstrap + ("?host=%s&uuid=%s" % (quote_plus(args.listen), quote_plus(args.uuid)))
    logger.info("Loading JSON encoded bootstrap file at '%s'." % url)
    req = urllib2.Request(url)
    req.add_header('User-Agent', 'CoolOverlay v0.1')
    req = urllib2.urlopen(req)
    encoding=req.headers['content-type'].split('charset=')[-1]
    data = req.read().decode(encoding)
    logger.debug("json bootstrap data: %s" % str(data))
    bootstrap = json.loads(data)
except Exception as err:
    logger.error("Error loading bootstrap file at '%s': %s" % (url, str(err)))
    logger.debug("DATA(%s): %s" % (str(encoding), str(data)))
    cleanup_and_exit(1)
for node in bootstrap:
    if node==args.listen:
        continue
    time.sleep(2)
    networking.Connection.connect_to(node)


#test system
time.sleep(6)
old_data = 0
def receiver(data):
    global old_data
    if data != old_data + 1:
        logger.error("OUTDATED DATA RECEIVED (%s != %s)!!!" % (str(data), str(old_data + 1)))
    old_data = data
    print("DATA RECEIVED: %s" % data)
if args.subscribe:
    time.sleep(2)
    router.subscribe(args.subscribe, receiver)

#wait for CTRL-C
data = 0
while True:
    if args.publish:
        router.publish(args.publish, data);
        data += 1
    time.sleep(2)

