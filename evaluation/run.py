#!/usr/bin/python3
import networkx as nx
from networkx.readwrite import json_graph
import urllib.request as urllib2
from urllib.parse import quote_plus
import sys
import time
import copy
import json
import random
import subprocess
import numpy
import os


# some convenience functions
class Struct:
    def __init__(self, d):
        self.__dict__.update(d)

def genGraph(net, n, con=3, avgdegree=4, dim=2, threshold=0.1):
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
    req = urllib2.Request(url)
    return urllib2.urlopen(req)

def post_data(url, data):
    req = urllib2.Request(url, data)
    req.add_header("Content-Type",'application/json')
    return urllib2.urlopen(req)

def send_command(ip, command, data=None):
    data = copy.deepcopy(data if isinstance(data, dict) else {})
    data["_command"] = command
    print("\t\tSending command '%s' to '%s'..." % (command, ip))
    return post_data("http://"+ip+":9980/command", bytes(json.dumps(data), "UTF-8"))

def evaluate(graph_args, nodes, publishers, subscribers, router, settings, runtime):
    # generate randomly connected graph and add ip addresses and roles as configured
    print("\tCreating graph with %d nodes (%d publishers and %d subscribers)..." % (nodes, publishers, subscribers), file=sys.stderr)
    G = genGraph("random", nodes, **graph_args)
    pubs, subs = publishers, subscribers
    for n in G.nodes():
        roles = {}
        if subs:
            roles["subscriber"] = ["test"]
            subs = subs - 1
        elif pubs:
            roles["publisher"] = ["test"]
            pubs = pubs - 1
        G.node[n] = {"ip": "%d.%d.%d.%d" % (base_ip[0], base_ip[1], base_ip[2], (base_ip[3]+n)), "roles": roles}    
        nx.relabel_nodes(G, {n: "ID: %d" % n}, False)

    # start nodes
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

    print("\tChecking for availability of all nodes...", file=sys.stderr)
    for n in sorted(list(G.nodes())):
        ip = G.node[n]["ip"]
        online = False
        for i in range(1, 30):
            try:
                print("\t\tTry %d for node '%s' (%s)..." % (i, n, ip))
                download_file("http://%s:9980/" % ip)
                online = True
                break
            except:
                time.sleep(1)
                continue
        if not online:
            print("\t\tNode '%s' (%s) does not come online, aborting!" % (n, ip), file=sys.stderr)
            sys.exit(1)
        print("\t\tNode '%s' (%s) is online..." % (n, ip))
        send_command(ip, "stop")
    time.sleep(4)

    print("\tConfiguring filters...", file=sys.stderr)
    with open("filters.py", "r") as f:
        code = f.read()
        for n in sorted(list(G.nodes())):
            send_command(G.node[n]["ip"], "load_filters", {"code": code})

    print("\tStarting routers...", file=sys.stderr)
    for n in sorted(list(G.nodes())):
        send_command(G.node[n]["ip"], "start", {"router": router, "settings": settings})
    time.sleep(4)

    print("\tConfiguring router connections...", file=sys.stderr)
    for n in sorted(list(G.nodes())):
        for neighbor in G[n]:
            send_command(G.node[n]["ip"], "connect", {"addr": G.node[neighbor]["ip"]})
    time.sleep(4)

    print("\tConfiguring router roles...", file=sys.stderr)
    role_to_command = {"subscriber": "subscribe", "publisher": "publish"}
    for n in sorted(list(G.nodes())):
        for roletype, channellist in G.node[n]["roles"].items():
            for channel in channellist:
                send_command(G.node[n]["ip"], role_to_command[roletype], {"channel": channel})

    print("\tWaiting %.3f seconds for routers doing their work..." % runtime, file=sys.stderr)
    time.sleep(runtime)

    print("\tStopping routers...", file=sys.stderr)
    for n in sorted(list(G.nodes())):
        send_command(G.node[n]["ip"], "stop")
    subprocess.call(["./helpers.sh", "stop"])

    # evaluate log output
    subprocess.call(["./helpers.sh", "evaluate"])
    with open("logs/evaluation.py", "r") as f:
        evaluation = {}
        exec(f.read(), {}, evaluation)
        evaluation = Struct(evaluation)     # create object from dictionary for easier access
    evaluation.slave_publishers = (publishers - evaluation.master_publishers)
    return evaluation

def update_settings(settings, setting, value):
    setting = str(setting).split(".")
    last_entry = setting[len(setting)-1]
    setting = setting[:-1]
    for entry in setting:
        settings = settings[entry]
    settings[last_entry] = value


# load tasks file
print("Loading tasks description file...", file=sys.stderr)
with open("tasks.json", "r") as f:
    tasks = json.load(f)
    global_settings = tasks["settings"]
    tasks = tasks["tasks"]

# execute evaluation tasks
all_results = {}
for task_name, task in tasks.items():
    print("Executing task '%s'..." % task_name, file=sys.stderr)
    all_results[task_name] = {}
    settings = {}
    settings.update(global_settings)
    settings.update(task["settings"] if "settings" in task and task["settings"] else {})
    if "graph_args" not in task or not task["graph_args"]:
        task["graph_args"] = {}
    
    # interprete iterator if given
    iterator = ["default_iterator"]       # dummy iterator having only one entry
    if task["iterate"]:
        loc = {}
        exec("iterator = %s" % task["iterate"]["iterator"], {"numpy": numpy, "random": random}, loc)
        iterator = loc["iterator"]
        print("Iterator '%s' --> %s" % (task["iterate"]["iterator"], str(iterator)))
    
    # use iterator to evaluate task["rounds"] networks and get the average of every expression defined in task["output"]
    iterator_counter = 0
    for iterator_value in iterator:
        if task["iterate"]:
            print("Iterating over %s: %s --> %s" % (task["iterate"]["setting"], str(iterator_value), str(iterator)), file=sys.stderr)
            update_settings(settings, task["iterate"]["setting"], iterator_value)
        # collect evaluation outcome for this task iteration averaged over task["rounds"]
        output = {}
        for round_num in range(task["rounds"]):
            print("Beginning evaluation round %d..." % round_num, file=sys.stderr)
            evaluation = evaluate(
                task["graph_args"],
                task["nodes"],
                task["publishers"],
                task["subscribers"],
                task["router"],
                settings,
                task["runtime"]
            )
            os.rename("logs", "logs.%s%s.r%d" % (task_name, (".i%d" % iterator_counter if task["iterate"] else ""), round_num))
            for var, code in task["output"].items():
                if var not in output:
                    output[var] = []
                result = {}
                exec("%s = %s" % (var, code), {"evaluation": evaluation, "numpy": numpy}, result)
                output[var].append(result[var])
        for var in task["output"]:
            output[var] = numpy.mean(output[var])
        all_results[task_name][iterator_value] = output
        iterator_counter += 1
        
        print("Writing partial results...", file=sys.stderr)
        with open("results.json", "w") as f:
            json.dump(all_results, f, sort_keys=True, indent=4)

print("Writing results...", file=sys.stderr)
with open("results.json", "w") as f:
    json.dump(all_results, f)

print("Exiting...", file=sys.stderr)
sys.exit(0)
