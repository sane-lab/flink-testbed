import getopt
import os
import sys
from math import ceil

import matplotlib
import matplotlib as mpl
from matplotlib.ticker import PercentFormatter, LogLocator
from numpy import double
from numpy.ma import arange

from analysis.config.default_config import TICK_FONT_SIZE, OPT_FONT_NAME

mpl.use('Agg')

import matplotlib.pyplot as plt
import pylab
from matplotlib.font_manager import FontProperties

from analysis.config.default_config import LABEL_FONT_SIZE, LEGEND_FONT_SIZE, TICK_FONT_SIZE, OPT_FONT_NAME, \
    LINE_COLORS, LINE_WIDTH, MARKERS, MARKER_SIZE, FIGURE_FOLDER, FILE_FOLER, per_task_rate, per_key_state_size, \
    sync_keys, replicate_keys_filter, parallelism, max_parallelism, state_access_ratio, zipf_skew

LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)


matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True
matplotlib.rcParams['xtick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['ytick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['font.family'] = OPT_FONT_NAME


# there are some embedding problems if directly exporting the pdf figure using matplotlib.
# so we generate the eps format first and convert it to pdf.
def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


# example for reading csv file
def ReadFile():
    x_axis = []
    y_axis = []

    parallelism = 8
    max_parallelism = 512
    per_key_state_size = 16384
    replicate_keys_filter = 0
    sync_keys = 0
    per_task_rate = 5000
    zipf_skew = 1
    state_access_ratio = 2
    configScenario = "dynamic"

    col = []
    coly = []
    start_ts = float('inf')
    temp_dict = {}
    for tid in range(0, 8):
        f = open(FILE_FOLER + "/spector-{}-{}-{}-{}-{}-{}-{}-{}-{}/Splitter FlatMap-{}.output"
                 .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, sync_keys,
                         replicate_keys_filter, state_access_ratio, zipf_skew, configScenario, tid))
        read = f.readlines()
        for r in read:
            if r.find("endToEnd latency: ") != -1:
                ts = int(int(r.split("ts: ")[1][:13]) / 1000)
                if ts < start_ts:  # find the starting point from parallel tasks
                    start_ts = ts
                latency = int(r.split("endToEnd latency: ")[1])
                if ts not in temp_dict:
                    temp_dict[ts] = []
                temp_dict[ts].append(latency)

    for ts in temp_dict:
        coly.append(len(temp_dict[ts])) # Aggregated throughput
        col.append(ts - start_ts)
    x_axis.append(col)
    y_axis.append(coly)

    print(coly)

    return x_axis, y_axis


# draw a line chart
def DrawFigure(xvalues, yvalues, legend_labels, x_label, y_label, filename, allow_legend):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(10, 3))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    x_values = xvalues
    y_values = yvalues
    lines = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        lines[i], = figure.plot(x_values[i], y_values[i], color=LINE_COLORS[i], \
                               linewidth=LINE_WIDTH, marker=MARKERS[i], \
                               markersize=MARKER_SIZE, label=FIGURE_LABEL[i],
                                markeredgewidth=2, markeredgecolor='k',
                                markevery=5
                               )

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(lines,
                   FIGURE_LABEL,
                   prop=LEGEND_FP,
                   loc='upper center',
                   ncol=2,
                   #                     mode='expand',
                   bbox_to_anchor=(0.55, 1.5), shadow=False,
                   columnspacing=0.1,
                   frameon=True, borderaxespad=0.0, handlelength=1.5,
                   handletextpad=0.1,
                   labelspacing=0.1)

    plt.yscale('log')
    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')

if __name__ == "__main__":
    x_axis, y_axis = ReadFile()
    legend_labels = ["Spacker"]
    legend = True
    DrawFigure(x_axis, y_axis, legend_labels, "Time (s)", "Throughput (e/s)", "throughput_curve_overview", legend)
