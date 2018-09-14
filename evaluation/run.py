#!/usr/bin/python3
import networkx as nx
from networkx.readwrite import json_graph
import urllib.request as urllib2
from urllib.parse import quote_plus
import sys
import time
import copy
try:
    import commentjson as json
except:
    import json
import random
import subprocess
import numpy
import os
import re
import signal
from functools import reduce
import copy
import logging
import logging.config
import argparse


# some convenience functions
class Struct:
    def __init__(self, d):
        self.__dict__.update(d)
    def __str__(self):
        return str(self.__dict__)

def genGraph(net, n, con=3, avgdegree=4, dim=2, threshold=0.1):
    global logger
    if ("social" == net):
        G = nx.barabasi_albert_graph(n, con)

    if ("random" == net):
        G = nx.gnp_random_graph(n, avgdegree/(n-1))

    if ("euclidean" == net):
        G = nx.random_geometric_graph(n, threshold, dim)

    if not nx.is_connected(G):
        isolated = list(nx.isolates(G))

        for n1 in isolated:
            # identify counterpart
            n2 = random.choice(list(G.nodes()))
            while (G.degree(n2) == 0 or n1 == n2):
                n2 = random.choice(list(G.nodes()))
            G.add_edge(n1, n2)

    if not nx.is_connected(G):
        s = sorted(nx.connected_components(G), key=len, reverse=True)

        while (not nx.is_connected(G)):
            c1 = random.choice(s)
            c2 = random.choice(s)

            while (c1 == c2):
                c2 = random.choice(s)

            n1 = random.sample(c1, 1)
            n2 = random.sample(c2, 1)

            G.add_edge(n1[0], n2[0])

    return G

def download_file(url):
    global logger
    req = urllib2.Request(url)
    return urllib2.urlopen(req)

def post_data(url, data):
    global logger
    req = urllib2.Request(url, data)
    req.add_header("Content-Type",'application/json')
    return urllib2.urlopen(req)

def send_command(ip, command, data=None):
    global logger
    data = copy.deepcopy(data if isinstance(data, dict) else {})
    data["_command"] = command
    logger.debug("******** Sending command '%s' to '%s'..." % (command, ip))
    return post_data("http://"+ip+":9980/command", bytes(json.dumps(data), "UTF-8"))

def extract_data(task, standard_imports):
    code_pattern = re.compile("^.*\*\*\*\*\*\*\*\*\*\*\* CODE_EVENT\((?P<event>[^)]*)\): (?P<code>.*)$")
    # evaluate log output and return result
    logger.info("**** Parsing log output...")
    evaluation = {}
    evaluation.update(copy.deepcopy(task["init"]))
    with open("logs/full.log", "r") as f:
        for line in f:
            match = code_pattern.search(line)
            if not match:
                continue
            event = match.group('event')
            code = match.group('code')
            try:
                exec(code, {}.update(standard_imports), evaluation)
            except BaseException as e:
                logger.error("Exception %s: %s while executing code line '%s'!" % (str(e.__class__.__name__), str(e), code))
                raise
    return Struct(evaluation)

# generate randomly connected graph and add ip addresses and roles as configured
def evaluate(task, settings, standard_imports):
    global logger
    logger.info("**** Creating graph with %d nodes (%d publishers and %d subscribers)..." % (
        task["nodes"],
        task["publishers"],
        task["subscribers"]
    ))
    filter_pattern = re.compile("#\*\*\*\*\*\*\*\*\*\*\* EVALUATOR_EXTENSION_POINT, PLEASE DON'T REMOVE \*\*\*\*\*\*\*\*\*\*\*#")
    base_ip = str(task["base_ip"]).split(".")
    G = genGraph("random", task["nodes"], **task["graph_args"])
    pubs, subs = task["publishers"], task["subscribers"]
    for n in G.nodes():
        roles = {}
        if subs:
            roles["subscriber"] = ["test"]
            subs = subs - 1
        elif pubs:
            roles["publisher"] = ["test"]
            pubs = pubs - 1
        G.node[n] = {"ip": "%d.%d.%d.%d" % (int(base_ip[0]), int(base_ip[1]), int(base_ip[2]), (int(base_ip[3])+n)), "roles": roles}    
        nx.relabel_nodes(G, {n: "ID: %d" % n}, False)

    # start nodes (cleanup on sigint (CTRL-C) while nodes are running)
    def sigint_handler(sig, frame):
        signal.signal(signal.SIGINT, signal.SIG_IGN) # ignore SIGINT while shutting down
        logger.warning("Got interrupted, killing nodes!")
        subprocess.call(["./helpers.sh", "stop"])
        sys.exit(0)
    signal.signal(signal.SIGINT, sigint_handler)
    ips = []
    for n in sorted(list(G.nodes())):
        ips.append(G.node[n]["ip"])
    subprocess.run(["./helpers.sh", "start"], input=bytes("%s\n" % ("\n".join(ips)), "UTF-8"))

    # create json string from graph
    G.graph["settings"] = settings
    node_link_data = json_graph.node_link_data(G)
    graph_data = json.dumps(node_link_data, sort_keys=True, indent=4)
    with open("logs/graph.json", "w") as f:
        f.write(graph_data)

    logger.info("**** Checking for availability of all nodes...")
    for n in sorted(list(G.nodes())):
        ip = G.node[n]["ip"]
        online = False
        for i in range(1, 30):
            try:
                logger.debug("******** Try %d for node '%s' (%s)..." % (i, n, ip))
                download_file("http://%s:9980/" % ip)
                online = True
                break
            except:
                time.sleep(1)
                continue
        if not online:
            logger.info("******** Node '%s' (%s) does not come online, aborting!" % (n, ip))
            sys.exit(1)
        logger.debug("******** Node '%s' (%s) is online..." % (n, ip))
        send_command(ip, "stop")

    logger.info("**** Configuring node filters...")
    with open("filters.py", "r") as f:
        code = f.read()
        code=filter_pattern.sub("""
task = %s
        """ % str(task), code)
        for n in sorted(list(G.nodes())):
            send_command(G.node[n]["ip"], "load_filters", {"code": code})

    logger.info("**** Starting routers...")
    for n in sorted(list(G.nodes())):
        send_command(G.node[n]["ip"], "start", {"router": task["router"], "settings": settings})
    time.sleep(1)

    logger.info("**** Configuring node connections...")
    for n in sorted(list(G.nodes())):
        for neighbor in G[n]:
            send_command(G.node[n]["ip"], "connect", {"addr": G.node[neighbor]["ip"]})
    time.sleep(2)

    logger.info("**** Configuring node roles (%d publishers, %d subscribers)..." % (task["publishers"], task["subscribers"]))
    role_to_command = {"subscriber": "subscribe", "publisher": "publish"}
    for n in sorted(list(G.nodes())):
        for roletype, channellist in G.node[n]["roles"].items():
            for channel in channellist:
                send_command(G.node[n]["ip"], role_to_command[roletype], {"channel": channel})

    logger.info("**** Waiting %.3f seconds for routers doing their work..." % task["runtime"])
    time.sleep(task["runtime"])

    logger.info("**** Stopping routers and killing nodes...")
    for n in sorted(list(G.nodes())):
        send_command(G.node[n]["ip"], "stop")
    time.sleep(2)
    subprocess.call(["./helpers.sh", "stop"])
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    
    return extract_data(task, standard_imports)

def update_settings(settings, setting, value):
    global logger
    # multi var version
    if isinstance(setting, list):
        assert len(setting) == len(value), "Multi var version of iterator has different length: %d != %d" % (len(setting), len(value))
        c = 0
        for s in setting:
            update_settings(settings, s, value[c])
            c += 1
        return
    # single var version
    setting = str(setting).split(".")
    last_entry = setting[len(setting)-1]
    setting = setting[:-1]
    for entry in setting:
        settings = settings[entry]
    settings[last_entry] = value


# parse commandline
parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, description="AnonPubSub node.\nHTTP Control Port: 9980\nNode Communication Port: 9999")
parser.add_argument("--log", metavar='LOGLEVEL', help="Loglevel to log", default="INFO")
args = parser.parse_args()

with open("logger.json", 'r') as logging_configuration_file:
    logger_config=json.load(logging_configuration_file)
logger_config["handlers"]["stderr"]["level"] = args.log
logging.config.dictConfig(logger_config)
logger = logging.getLogger()
logger.info('Logger configured...')

# load tasks file
logger.info("Loading tasks description file...")
with open("tasks.json", "r") as f:
    tasks_json = json.load(f)
    global_settings = tasks_json["settings"]
    task_defaults = tasks_json["task_defaults"]
    tasks = tasks_json["tasks"]

# execute evaluation tasks
all_results = {}
standard_imports = {
    "numpy": numpy,
    "random": random,
    "reduce": reduce    # map and filter are standard, reduce has to be imported
}
for task_name, _task in tasks.items():
    # build task dict
    task = {}
    task.update(task_defaults)
    task.update(_task)
    if "graph_args" not in task or not task["graph_args"]:
        task["graph_args"] = {}
    if "reduce" not in task or not task["reduce"]:
        task["reduce"] = {}
    
    if "output" not in _task or not len(task["output"]):
        logger.info("Ignoring task '%s' (no output defined)..." % task_name)
        continue
    
    logger.info("Executing task '%s'..." % task_name)
    all_results[task_name] = {}
    subprocess.call("./helpers.sh cleanup logs.%s" % task_name, shell=True)
    
    # build settings dict
    settings = {}
    settings.update(global_settings)
    settings.update(task["settings"] if "settings" in task and task["settings"] else {})
    
    # interprete iterator if given
    iterator = ["default_iterator"]       # dummy iterator having only one entry
    if "iterate" in task and task["iterate"]:
        loc = {}
        try:
            exec("iterator = %s" % task["iterate"]["iterator"], {}.update(standard_imports), loc)
        except BaseException as e:
            logger.error("Exception %s: %s while executing code line '%s'!" % (str(e.__class__.__name__), str(e), "iterator = %s" % task["iterate"]["iterator"]))
            raise
        iterator = loc["iterator"]
        logger.info("Iterator '%s' --> %s" % (task["iterate"]["iterator"], str(iterator)))
    
    # use iterator to evaluate task["rounds"] networks and get the average of every expression defined in task["output"]
    iterator_counter = 0
    for iterator_value in iterator:
        # update settings according to iterator values
        if "iterate" in task and task["iterate"]:
            logger.info("[Iteration %d of %d]: %s = %s" % (iterator_counter+1, len(iterator), str(tuple(task["iterate"]["setting"])), str(iterator_value)))
            update_settings(settings, task["iterate"]["setting"], iterator_value)
        
        # collect evaluation outcome for this task iteration averaged over task["rounds"]
        output = {}
        for round_num in range(int(task["rounds"])):
            logger.info("Beginning evaluation round %d/%d..." % (round_num + 1, int(task["rounds"])))
            # evaluate graph
            evaluation = evaluate(task, settings, standard_imports)
            os.rename("logs", "logs.%s%s.r%d" % (task_name, (".i%d" % (iterator_counter+1) if "iterate" in task and task["iterate"] else ""), (round_num+1)))
            #logger.info("EVALUATION: %s" % str(evaluation))
            # generate round output vars from raw evaluation input via code in taskfile
            for var, code in task["output"].items():
                if var not in output:
                    output[var] = []
                result = {}
                try:
                    exec("%s = %s" % (var, code), {
                        "task": task,
                        "settings": settings,
                        "round_num": round_num,
                        "iterator_counter": iterator_counter,
                        "iterator_value": iterator_value,
                        "evaluation": evaluation
                    }.update(standard_imports), result)
                    #logger.info("OUTPUT RESULT: %s" % str(result))
                except BaseException as e:
                    logger.error("Exception %s: %s while executing code line '%s'!" % (str(e.__class__.__name__), str(e), "%s = %s" % (var, code)))
                    raise
                output[var].append(result[var])
        
        # accumulate output of all rounds via reduce function specified in taskfile, if wanted
        for var in list(output.keys()):
            # only reduce when wanted
            if var in task["reduce"]:
                try:
                    result = {}
                    try:
                        exec("reduce_func = (lambda valuelist: %s)" % task["reduce"][var], {
                            "task": task,
                            "settings": settings,
                            "iterator_counter": iterator_counter,
                            "iterator_value": iterator_value
                        }.update(standard_imports), result)
                    except BaseException as e:
                        logger.warning("Exception %s: %s while executing code line '%s'!" % (str(e.__class__.__name__), str(e), "reduce_func = %s" % code))
                        raise
                    #logger.info("RESULT %s(%s) --> %s" % (str(result["reduce_func"]), str(output[var]), str((result["reduce_func"])(output[var]))))
                    output[var] = (result["reduce_func"])(output[var])
                except BaseException as e:
                    logger.warning("Exception %s: %s while accumulating list %s" % (str(e.__class__.__name__), str(e), str(output[var])))
                    logger.warning("Setting output to raw list of round values...")
                    output[var] = output[var]
        
        # save results
        all_results[task_name]["%s = %s" % (str(task["iterate"]["setting"]), str(iterator_value))] = output
        iterator_counter += 1
        
        logger.info("Writing partial evaluation results...")
        with open("results.json", "w") as f:
            json.dump(all_results, f, sort_keys=True, indent=4)

logger.info("Writing evaluation results...")
with open("results.json", "w") as f:
    json.dump(all_results, f, sort_keys=True, indent=4)

logger.info("All done")
sys.exit(0)
