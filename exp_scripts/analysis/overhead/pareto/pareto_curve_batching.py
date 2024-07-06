import os
from math import ceil, floor
import sys
from pathlib import Path

path_root = Path(__file__).parents[3]
sys.path.append(str(path_root))

import matplotlib
import matplotlib as mpl
import numpy as np

import pandas as pd

from analysis.config.default_config import LABEL_FONT_SIZE, LEGEND_FONT_SIZE, TICK_FONT_SIZE, OPT_FONT_NAME, \
    LINE_COLORS, LINE_WIDTH, MARKERS, MARKER_SIZE, FIGURE_FOLDER, FILE_FOLER, PATTERNS, timers_plot, per_key_state_size, \
    replicate_keys_filter, repeat_num, per_task_rate, order_function
from analysis.config.general_utilities import breakdown_total

mpl.use('Agg')

import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)

mpl.rcParams['ps.useafm'] = True
mpl.rcParams['pdf.use14corefonts'] = True
mpl.rcParams['xtick.labelsize'] = TICK_FONT_SIZE
mpl.rcParams['ytick.labelsize'] = TICK_FONT_SIZE
mpl.rcParams['font.family'] = OPT_FONT_NAME
matplotlib.rcParams['pdf.fonttype'] = 42

# there are some embedding problems if directly exporting the pdf figure using matplotlib.
# so we generate the eps format first and convert it to pdf.
def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


# example for reading csv file
def ReadFile():
    x_axis = []
    y_axis = []

    # per_key_state_size = 16384
    # per_key_state_size = 1024
    # max_parallelism = 16384

    w, h = 7, 3
    y = [[0 for x in range(w)] for y in range(h)]

    completion_time_dict = {}
    # keys = [1, 2, 4, 8, 16, 32, 64, 128]
    keys = [1, 2, 4, 8, 16, 32, 64]

    latency_dict = {}

    # for sync_keys in keys:
    for sync_keys in keys:
        # sync_keys = int(max_parallelism / 8 / sync_keys)
        col = []
        coly = []
        start_ts = float('inf')
        temp_dict = {}
        for tid in range(0, 8):
            print(FILE_FOLER + "/spector-{}-{}-{}/Splitter FlatMap-{}.output"
                     .format(per_key_state_size, sync_keys, replicate_keys_filter, tid))

            # f = open(FILE_FOLER + "/spector-{}-{}-{}-{}-{}/Splitter FlatMap-{}.output"
            #          .format(per_task_rate, per_key_state_size, sync_keys, replicate_keys_filter, order_function, tid))
            if not os.path.isfile(FILE_FOLER + "/spector-{}-{}-{}/Splitter FlatMap-{}.output"
                     .format(per_key_state_size, sync_keys, replicate_keys_filter, tid)):
               continue 
            
            f = open(FILE_FOLER + "/spector-{}-{}-{}/Splitter FlatMap-{}.output"
                     .format(per_key_state_size, sync_keys, replicate_keys_filter, tid))
            read = f.readlines()
            for r in read:
                if r.find("endToEnd latency: ") != -1:
                    ts = int(int(r.split("ts: ")[1][:13])/1000)
                    if ts < start_ts: # find the starting point from parallel tasks
                        start_ts = ts
                    latency = int(r.split("endToEnd latency: ")[1])
                    if ts not in temp_dict:
                        temp_dict[ts] = []
                    temp_dict[ts].append(latency)

        for ts in temp_dict:
            # coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
            temp_dict[ts].sort()
            coly.append(temp_dict[ts][floor((len(temp_dict[ts]))*0.99)])
            col.append(ts - start_ts)

        # x_axis.append(col[40:70])
        # y_axis.append(coly[40:70])

        # x_axis.append(col)
        # y_axis.append(coly)

        # Get P95 latency
        coly.sort()
        # latency_dict[sync_keys] = coly[ceil(len(coly)*0.99)]
        latency_dict[sync_keys] = coly[-1]

    for repeat in range(1, repeat_num + 1):
        i = 0
        for sync_keys in keys:
            # sync_keys = int(max_parallelism / 8 / sync_keys)

            exp = FILE_FOLER + '/spector-{}-{}-{}'.format(per_key_state_size, sync_keys,
                                                                    replicate_keys_filter)
            file_path = os.path.join(exp, "timer.output")
            # try:
            stats = breakdown_total(open(file_path).readlines())
            for j in range(3):
                if timers_plot[j] not in stats:
                    y[j][i] = 0
                else:
                    y[j][i] += stats[timers_plot[j]]
            i += 1
            # except Exception as e:
            #     print("Error while processing the file {}: {}".format(exp, e))

    sync_time = []
    replication_time = []
    update_time = []

    for j in range(h):
        for i in range(w):
            y[j][i] = y[j][i] / repeat_num
            if j == 0:
                sync_time.append(y[j][i])
            elif j == 1:
                replication_time.append(y[j][i])
            elif j == 2:
                update_time.append(y[j][i])

    for i in range(w):
        completion_time = 0
        for j in range(h):
            completion_time += y[j][i]
        completion_time_dict[keys[i]] = completion_time

    # curve = {}
    #
    # for key in latency_dict:
    #     curve[completion_time_dict[key]] = latency_dict[key]

    x_axis.append(keys)
    x_axis.append(keys)
    x_axis.append(keys)
    y_axis.append(sync_time)
    y_axis.append(update_time)
    y_axis.append(latency_dict.values())

    return x_axis, y_axis


# draw a line chart
def DrawFigure(xvalues, yvalues, legend_labels, x_label, y_label, y_label_2, filename, allow_legend):
    # you may change the figure size on your own.
    # fig = plt.figure(figsize=(10, 5))
    # figure = fig.add_subplot(111)
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax2 = plt.twinx()

    FIGURE_LABEL = legend_labels

    x_values = xvalues
    y_values = yvalues

    # values in the x_xis
    index = np.arange(len(x_values[0]))
    print(index, y_values[0])
    # the bar width.
    # you may need to tune it to get the best figure.
    padded_x = [x_values[0][0] / 2] + x_values[0] + [x_values[0][-1] * 2]
    width = 0.5
    lefts = [x1 ** (1 - width / 2) * x0 ** (width / 2) for x0, x1 in zip(padded_x[:-2], padded_x[1:-1])]
    rights = [x0 ** (1 - width / 2) * x1 ** (width / 2) for x0, x1 in zip(padded_x[1:-1], padded_x[2:])]
    widths = [r - l for l, r in zip(lefts, rights)]
    bottom_base = np.zeros(len(y_values[0]))

    lines = [None] * (len(FIGURE_LABEL))
    # for i in range(len(y_values)):
    #     lines[i], = figure.plot(x_values[i], y_values[i], color=LINE_COLORS[i],
    #                             linewidth=LINE_WIDTH,
    #                             # marker=MARKERS[i],
    #                             # markersize=MARKER_SIZE,
    #                             label=FIGURE_LABEL[i],
    #                             markeredgewidth=1, markeredgecolor='k',
    #                             markevery=1)

    # lines[0], = ax1.plot(x_values[0], y_values[0], color=LINE_COLORS[0],
    #                             linewidth=LINE_WIDTH,
    #                             marker=MARKERS[0],
    #                             markersize=MARKER_SIZE,
    #                             label=FIGURE_LABEL[0],
    #                             markeredgewidth=1, markeredgecolor='k',
    #                             markevery=50)
    lines[0] = ax1.bar(lefts, y_values[0], widths, hatch=PATTERNS[0], color=LINE_COLORS[0],
                      label=FIGURE_LABEL[0], bottom=bottom_base, edgecolor='black', linewidth=3, align="edge")
    bottom_base = np.array(list(y_values[0])) + bottom_base
    lines[1] = ax1.bar(lefts, y_values[1], widths, hatch=PATTERNS[1], color=LINE_COLORS[1],
                       label=FIGURE_LABEL[1], bottom=bottom_base, edgecolor='black', linewidth=3, align="edge")
    lines[2], = ax2.plot(x_values[2], y_values[2], color=LINE_COLORS[3],
             linewidth=LINE_WIDTH,
             marker=MARKERS[1],
             markersize=MARKER_SIZE,
             label=FIGURE_LABEL[2],
             markeredgewidth=1, markeredgecolor='k',
             markevery=1)

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(lines,
                   FIGURE_LABEL,
                   prop=LEGEND_FP,
                   loc='upper center',
                   ncol=4,
                   #                     mode='expand',
                   bbox_to_anchor=(0.5, 1.25), shadow=False,
                   columnspacing=0.1,
                   frameon=True, borderaxespad=0.0, handlelength=1.5,
                   handletextpad=0.1,
                   labelspacing=0.1)

    # plt.xticks(index + 0.5 * width, x_values[0])
    plt.xscale('log')
    # plt.xticks(x_values[0], [1, 2, 4, 8, 16, 32, 64, 128])
    # plt.xticks(x_values[0], [1, 2, 4, 8, 16, 32, 64])
    plt.xticks(x_values[0], [1, 2, 4, 8, 16, 32, 64])
    # plt.grid()
    ax1.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
    ax2.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
    ax1.set_xlabel(x_label, fontproperties=LABEL_FP)
    ax1.set_ylabel(y_label, fontproperties=LABEL_FP)
    ax2.set_ylabel(y_label_2, fontproperties=LABEL_FP)
    # plt.ylim(0)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')

if __name__ == "__main__":
    x_axis, y_axis = ReadFile()

    print(x_axis, y_axis)
    legend_labels = ["Sync Time", "Update Time", "Latency Spike"]
    legend = False
    DrawFigure(x_axis, y_axis, legend_labels, "Chunk Size", "Completion Time (ms)", "P99 Latency Spike (ms)", "pareto_curve_batching", legend)
