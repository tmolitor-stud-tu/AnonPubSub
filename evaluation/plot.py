#!/usr/bin/python3
import json
import numpy
import matplotlib.pyplot as plt


with open("results.json", "r") as f:
	data = json.load(f)

print("Plotting tasks data...")
for task_name, task_data in data.items():
    print("task: %s" % task_name)
    
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
    
    # plot graph
    plt.figure()
    if ptype == "normal":       # normal line based
        args = []
        legends = []
        for name in sorted(subfigures):
            # sort lists by x_values together (see: https://stackoverflow.com/a/13668413/3528174)
            x_values, y_values = [list(x) for x in zip(*sorted(zip(x, y[name]), key=lambda pair: pair[0]))]
            legends.append(task_data["captions"][name] if name in task_data["captions"] else name)
            args.append(x_values)
            args.append(y_values)
            args.append("o-")
        plt.plot(*args)
        
        #plt.title(task_name)
        plt.legend(legends, loc="best")
        plt.xlabel(heading)
        plt.savefig("%s.pdf" % task_name, bbox_inches='tight')
    if ptype == "errorbars_stabled":    # errorbars stapled
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
        plt.savefig("%s.pdf" % task_name, bbox_inches='tight')
    if ptype == "errorbars":    # single errorbars
        for name in sorted(subfigures):
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
            plt.savefig("%s_%s.pdf" % (task_name, name), bbox_inches='tight')

