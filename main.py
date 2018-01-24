import json, logging, logging.config
with open("logger.json", 'r') as logging_configuration_file:
	logging.config.dictConfig(json.load(logging_configuration_file))
logger = logging.getLogger()
logger.info('Logger configured')

import uuid
from queue import Queue

import networking


queue = Queue()
l = networking.Listener(str(uuid.uuid4()), queue, "localhost")
connections = {}

while(True):
	if not queue.empty():
		command = queue.get()
		if command["command"] == "add_connection":
			con = command["connection"]
			connections[con.get_peer_id()] = con
		elif command["command"] == "dummy":
			pass
		else:
			logger.error("unknown routing command '%s'!" % command["command"])
		queue.task_done()



#router_queue.put({
	#"command": "add_connection",
	#"connection": con
#})


