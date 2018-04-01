#!/usr/bin/python3
#first of all: configure logging
import json, logging, logging.config
with open("logger.json", 'r') as logging_configuration_file:
    logging.config.dictConfig(json.load(logging_configuration_file))
logger = logging.getLogger()
logger.info('Logger configured')

#import everything that is needed here
import uuid
from queue import Queue
import argparse
import urllib.request as urllib2
from urllib.parse import quote_plus
import sys
import signal
import codecs

#our own modules come here
import networking


#parse commandline
parser = argparse.ArgumentParser(description='CoolOverlay node.')
parser.add_argument("-l", "--listen", metavar='HOSTNAME', help="Local hostname or IP to listen on", default="localhost")
parser.add_argument("-b", "--bootstrap", metavar='URL', help="Load JSON encoded bootstrap file", default="http://localhost/cool_overlay.php")
parser.add_argument("-u", "--uuid", metavar='UUID', help="Node UUID (default value is randomly generated)", default=str(uuid.uuid4()))
args = parser.parse_args()

#initialize our network listener
queue = Queue()
l = networking.Listener(args.uuid, queue, args.listen)
connections = {}

#use this to cleanup the system and exit
def cleanup_and_exit(code=0):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    networking.Connection.shutdown()
    if l:
        l.stop();
    for peer_id, con in connections.items():
        con.terminate()
    sys.exit(code)

#cleanup on sigint (CTRL-C)
def sigint_handler(sig, frame):
    logger.warning("Got interrupted, shutting down!")
    cleanup_and_exit(0)
signal.signal(signal.SIGINT, sigint_handler)

#load bootstrap file defining initial connections and connect to the listed nodes
try:
    url = args.bootstrap + ("?host=%s&uuid=%s" % (quote_plus(args.listen), quote_plus(args.uuid)))
    logger.info("Loading JSON encoded bootstrap file at '%s'." % url)
    req = urllib2.Request(args.bootstrap+"")
    req.add_header('User-Agent', 'CoolOverlay v0.1')
    req = urllib2.urlopen(req)
    encoding=req.headers['content-type'].split('charset=')[-1]
    data = req.read().decode(encoding)
    bootstrap = json.loads(data)
except Exception as err:
    logger.error("Error loading bootstrap file at '%s': %s" % (url, str(err)))
    logger.debug("DATA(%s): %s" % (str(encoding), str(data)))
    cleanup_and_exit(1)
for node in bootstrap:
    if node==args.listen:
        continue
    networking.connect_to(args.uuid, queue, node)


while True:
    if not queue.empty():
        command = queue.get()
        logger.info("got routing command: %s" % command["command"])
        if command["command"] == "add_connection":
            con = command["connection"]
            connections[con.get_peer_id()] = con
        elif command["command"] == "remove_connection":
            con = command["connection"]
            del connections[con.get_peer_id()]
        else:
            logger.error("unknown routing command '%s'!" % command["command"])
        queue.task_done()



#router_queue.put({
    #"command": "add_connection",
    #"connection": con
#})


