#!/usr/bin/python3
import json
import numpy
import matplotlib.pyplot as plt
import logging
import logging.config
import argparse


parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, description="Evaluation plotter.")
parser.add_argument("-R", "--results", metavar='RESULTS_FILE', help="Results file to load", default="results.json")
parser.add_argument("-r", "--run", metavar='TASK', help="List of task results to plot", nargs="+", default="")
parser.add_argument("-f", "--format", metavar="FORMAT", help="Plot data in this format. All mathplotlib formats are supported, default: 'png'", default=[], action="append")
parser.add_argument("-l", "--log", metavar='LOGLEVEL', help="Loglevel to log", default="INFO")
args = parser.parse_args()
if not len(args.format):
    args.format = ["png"]

with open("logger.json", 'r') as logging_configuration_file:
    logger_config=json.load(logging_configuration_file)
logger_config["handlers"]["stderr"]["level"] = args.log
logging.config.dictConfig(logger_config)
logger = logging.getLogger()
logger.info('Logger configured...')

def write_plot(plt, formats, task_name, name=None):
    for fmt in formats:
        filename = "%s%s.%s" % (task_name, "_%s" % name if name else "", fmt)
        logger.info("Writing '%s'..." % filename)
        plt.savefig(filename, bbox_inches='tight')


logger.info("Loading '%s'..." % args.results)
with open(args.results, "r") as f:
	data = json.load(f)

logger.info("Plotting '%s'..." % args.results)
to_run = list(data.keys())
if args.run and len(args.run):
    to_run = args.run
else:
    to_run = list(data.keys())
for task_name in data.keys():
    if task_name not in to_run:
        logger.info("Ignoring task '%s' (requested on commandline)..." % task_name)
for task_name in to_run:
    task_data = data[task_name]
    logger.info("Plotting task: '%s'" % task_name)
    
    # old style results without captions
    if "results" not in task_data:
        task_data = {"results": task_data, "captions": {}}
    
    # extract graph parts
    x = []
    y = {}
    heading = ""
    subfigures = set()
    for _x, _y in task_data["results"].items():
        _x = _x.split("=")
        if len(_x) == 2:
            heading = _x[0].strip()
            x.append(_x[1].strip())
        else:
            heading = ""
            x.append("Messung")
        for _subfigure in list(_y.keys()):
            _subfigure = _subfigure.split("_")
            if _subfigure[-1].strip() in ("min", "max", "avg"):
                ptype = "errorbars"
                subfigures.add("_".join(_subfigure[:-1]).strip())
            else:
                ptype = "normal"
                subfigures.add("_".join(_subfigure).strip())
        for key, value in _y.items():
            if key not in y:
                y[key] = []
            y[key].append(value)
            y[key] = y[key]
    if "plot_type" in task_data:            # overwrite autodetected plot type
        ptype = task_data["plot_type"]
    
    # plot graph
    plt.figure()
    if ptype == "normal":       # normal line based
        plt.figure()
        plot_args = []
        legends = []
        for name in sorted(subfigures):
            # sort lists by x_values together (see: https://stackoverflow.com/a/13668413/3528174)
            x_values, y_values = [list(x) for x in zip(*sorted(zip(x, y[name]), key=lambda pair: pair[0]))]
            legends.append(task_data["captions"][name] if name in task_data["captions"] else name)
            plot_args.append(x_values)
            plot_args.append(y_values)
            plot_args.append("o-")
        plt.plot(*plot_args)
        
        #plt.title(task_name)
        plt.legend(legends, loc="best")
        plt.xlabel(heading)
        write_plot(plt, args.format, task_name)
    if ptype == "errorbars_stapled":    # errorbars stapled
        plt.figure()
        legends = []
        bottoms = [0] * len(x)
        for name in sorted(subfigures):
            min_values = y["%s_%s" % (name, 'min')]
            max_values = y["%s_%s" % (name, 'max')]
            avg_values = y["%s_%s" % (name, 'avg')]
            # sort lists by x_values together (see: https://stackoverflow.com/a/13668413/3528174)
            x_values, y_values = [list(x) for x in zip(*sorted(zip(x, avg_values), key=lambda pair: pair[0]))]
            offsets = numpy.abs(numpy.array([min_values, max_values]) - numpy.array(avg_values)[None, :])
            legends.append(task_data["captions"][name] if name in task_data["captions"] else name)
            plt.bar(x_values, y_values, bottom=bottoms, yerr=offsets, capsize=8)
            bottoms = y_values
        
        #plt.title(task_name)
        plt.legend(legends, loc="best")
        plt.xlabel(heading)
        write_plot(plt, args.format, task_name)
    if ptype == "errorbars":    # single errorbars
        for name in sorted(subfigures):
            plt.figure()
            min_values = y["%s_%s" % (name, 'min')]
            max_values = y["%s_%s" % (name, 'max')]
            avg_values = y["%s_%s" % (name, 'avg')]
            # sort lists by x_values together (see: https://stackoverflow.com/a/13668413/3528174)
            x_values, y_values = [list(x) for x in zip(*sorted(zip(x, avg_values), key=lambda pair: pair[0]))]
            offsets = numpy.abs(numpy.array([min_values, max_values]) - numpy.array(avg_values)[None, :])
            plt.bar(x_values, y_values, yerr=offsets, capsize=8)
            
            #plt.title(task_name)
            caption = task_data["captions"][name] if name in task_data["captions"] else name
            plt.legend([caption], loc="best")
            plt.xlabel(heading)
            write_plot(plt, args.format, task_name, name)

