#!/usr/bin/python3
import json
import numpy
import matplotlib.pyplot as plt


with open("results.json", "r") as f:
	data = json.load(f)

print("Plotting tasks data...")
for task_name, task_data in data.items():
    print("task: %s" % task_name)
    
    # extract graph parts
    x = []
    y = {}
    heading = ""
    subfigures = set()
    for _x, _y in task_data.items():
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
            legends.append(name)
            args.append(x_values)
            args.append(y_values)
            args.append("o-")
        plt.plot(*args)
        plt.legend(legends, loc="best")
    if ptype == "errorbars":    # errorbars
        legends = []
        bottoms = [0] * len(x)
        for name in sorted(subfigures):
            min_values = y["%s_%s" % (name, 'min')]
            max_values = y["%s_%s" % (name, 'max')]
            avg_values = y["%s_%s" % (name, 'avg')]
            # sort lists by x_values together (see: https://stackoverflow.com/a/13668413/3528174)
            x_values, y_values = [list(x) for x in zip(*sorted(zip(x, avg_values), key=lambda pair: pair[0]))]
            offsets = numpy.abs(numpy.array([min_values, max_values]) - numpy.array(avg_values)[None, :])
            legends.append(name)
            plt.bar(x_values, y_values, bottom=bottoms, yerr=offsets, capsize=8)
            bottoms = y_values
        plt.legend(legends, loc="best")
    
    #plt.title(task_name)
    plt.xlabel(heading)
    plt.savefig("%s.pdf" % task_name, bbox_inches='tight')
