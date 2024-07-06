import os
from math import floor
import sys
from pathlib import Path

path_root = Path(__file__).parents[3]
sys.path.append(str(path_root))

import sys
from pathlib import Path

path_root = Path(__file__).parents[3]
sys.path.append(str(path_root))

import matplotlib
import matplotlib.pyplot as plt
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


def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


def DrawFigure(xvalues, yvalues, legend_labels, x_label, y_label, filename, allow_legend):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(10, 3))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    x_values = xvalues
    y_values = yvalues
    lines = [None] * (len(FIGURE_LABEL))
    for i in range(1, len(y_values)):
        # lines[i], = figure.plot(x_values[i], y_values[i], color=LINE_COLORS[i], \
        #                     linewidth=6, marker=MARKERS[i], \
        #                     markersize=15.0, label=FIGURE_LABEL[i],
        #                     markeredgewidth=1, markeredgecolor='k',
        #                     markevery=5
        #                     )
        lines[i], = figure.plot(x_values[i], y_values[i], color=LINE_COLORS[i], \
                            linewidth=LINE_WIDTH, marker=MARKERS[i], \
                            markersize=10.0, label=FIGURE_LABEL[i],
                                markeredgewidth=1, markeredgecolor='k',
                                markevery=5
                            )

    lines[0], = figure.plot(x_values[0], y_values[0], color=LINE_COLORS[0], \
                               linewidth=6, marker=MARKERS[0], \
                               markersize=15.0, label=FIGURE_LABEL[0],
                                markeredgewidth=1, markeredgecolor='k',
                                markevery=5
                               )

    plt.axvline(x = 10, color = 'tab:gray', label = 'axvline - full height')
    plt.axvline(x = 40, color = 'tab:gray', label = 'axvline - full height')
    plt.axvline(x = 70, color = 'tab:gray', label = 'axvline - full height')

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(lines,
                   FIGURE_LABEL,
                   prop=LEGEND_FP,
                   loc='upper center',
                   ncol=4,
                   #                     mode='expand',
                   bbox_to_anchor=(0.5, 1.3), shadow=False,
                   columnspacing=0.1,
                   frameon=True, borderaxespad=0.0, handlelength=1.5,
                   handletextpad=0.1,
                   labelspacing=0.1)

    plt.yscale('log')
    # plt.ylim(10)
    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')


# the average latency
def averageLatency(lines):
    # get all latency of all files, calculate the average
    totalLatency = 0
    count = 0
    for line in lines:
        if line.startswith('keygroup: '):
            if line.split(": ")[-1][:-1] != "NaN":
                totalLatency += float(line.split(": ")[-1][:-1])
                count += 1

    if count > 0:
        return totalLatency / count
    else:
        return 0


# the average reconfig time
def averageCompletionTime(lines):
    timers = {}
    counts = {}
    for line in lines:
        key = line.split(" : ")[0]
        if key[0:6] == "++++++":
            if line.split(" : ")[0] not in timers:
                timers[key] = 0
                counts[key] = 0
            timers[key] += int(line.split(" : ")[1][:-3])
            counts[key] += 1

    stats = []
    for key in timers:
        totalTime = timers[key]
        count = counts[key]
        if count > 0:
            stats.append(totalTime / count)
        else:
            stats.append(0)
    # reconfig time breakdown
    # print(stats)
    sum = 0
    for i in stats:
        sum += i

    return sum/2

def ReadFile():
    x_axis = []
    y_axis = []

    parallelism = 8
    max_parallelism = 1024
    per_key_state_size = 32768
    # max_parallelism = 1024
    # per_key_state_size = 32768
    replicate_keys_filter = 0
    sync_keys = 0
    per_task_rate = 5000
    zipf_skew = 1
    state_access_ratio = 1
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

    x_axis.append(col[:100])
    y_axis.append(coly[:100])

    configScenario = "static"
    replicate_keys_filter = 0
    sync_keys = 0

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
        # coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
        temp_dict[ts].sort()
        coly.append(temp_dict[ts][floor((len(temp_dict[ts])) * 0.99)])
        col.append(ts - start_ts)

    x_axis.append(col[:100])
    y_axis.append(coly[:100])

    configScenario = "static"
    replicate_keys_filter = 0
    sync_keys = 2

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
        # coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
        temp_dict[ts].sort()
        coly.append(temp_dict[ts][floor((len(temp_dict[ts])) * 0.99)])
        col.append(ts - start_ts)

    x_axis.append(col[:100])
    y_axis.append(coly[:100])

    configScenario = "static"
    replicate_keys_filter = 1
    sync_keys = 0

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
        # coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
        temp_dict[ts].sort()
        coly.append(temp_dict[ts][floor((len(temp_dict[ts])) * 0.99)])
        col.append(ts - start_ts)

    x_axis.append(col[:100])
    y_axis.append(coly[:100])

    # x_axis.append(col)
        # y_axis.append(coly)

    print(x_axis)

    return x_axis, y_axis

if __name__ == '__main__':
    x_axis, y_axis = ReadFile()
    legend_labels = ["Spacker", "All-at-once", "Progressive", "Replication"]
    legend = True
    DrawFigure(x_axis, y_axis, legend_labels, "Elapsed Time (s)", "P99 Latency (ms)", "latency_curve_overview", legend)