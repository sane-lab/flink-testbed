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
    sync_keys, replicate_keys_filter

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
    fig = plt.figure(figsize=(10, 5))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    x_values = xvalues
    y_values = yvalues
    lines = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        lines[i], = figure.plot(x_values[i], y_values[i], color=LINE_COLORS[i], \
                               linewidth=LINE_WIDTH, marker=MARKERS[i], \
                               markersize=MARKER_SIZE, label=FIGURE_LABEL[i],
                                markeredgewidth=3, markeredgecolor='k',
                                markevery=3
                               )

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(lines,
                   FIGURE_LABEL,
                   prop=LEGEND_FP,
                   loc='upper center',
                   ncol=3,
                   #                     mode='expand',
                   bbox_to_anchor=(0.5, 1.3), shadow=False,
                   columnspacing=0.1,
                   frameon=True, borderaxespad=0.0, handlelength=1.5,
                   handletextpad=0.1,
                   labelspacing=0.1)

    plt.yscale('log')
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


    per_key_state_size = 4096
    sync_keys = 0
    replicate_keys_filter = 1
    reconfig_scenario = "profiling"

    col = []
    coly = []
    memory_dict = {}
    cur_ts = 0
    f = open(FILE_FOLER + "/spector-{}-{}-{}-{}/log/flink-myc-taskexecutor-0-camel-sane.out"
             .format(per_key_state_size, sync_keys, replicate_keys_filter, reconfig_scenario))
    read = f.readlines()
    start_ts = 0
    for r in read:
        if r.find("++++++ Current memory consumption: ") != -1:
            memory_size_with_ts = r.split(": ")[1]
            timestamp = int(memory_size_with_ts.split("-")[0])
            if start_ts == 0:
                start_ts = timestamp
            cur_ts = (timestamp - start_ts) / 1000
            memory_size = int(memory_size_with_ts.split("-")[1])
            coly.append(memory_size)
            col.append(cur_ts)
            memory_dict[cur_ts] = memory_size * 8
            # cur_ts += 1

    # x_axis.append(col)
    # y_axis.append(coly)
    x_axis.append(memory_dict.keys())
    y_axis.append(memory_dict.values())



    per_key_state_size = 4096
    sync_keys = 0
    replicate_keys_filter = 0
    reconfig_scenario = "profiling"

    col = []
    coly = []
    memory_dict = {}
    cur_ts = 0
    f = open(FILE_FOLER + "/spector-{}-{}-{}-{}/log/flink-myc-taskexecutor-0-camel-sane.out"
             .format(per_key_state_size, sync_keys, replicate_keys_filter, reconfig_scenario))
    read = f.readlines()
    start_ts = 0
    for r in read:
        if r.find("++++++ Current memory consumption: ") != -1:
            memory_size_with_ts = r.split(": ")[1]
            timestamp = int(memory_size_with_ts.split("-")[0])
            if start_ts == 0:
                start_ts = timestamp
            cur_ts = (timestamp - start_ts) / 1000
            memory_size = int(memory_size_with_ts.split("-")[1])
            coly.append(memory_size)
            col.append(cur_ts)
            memory_dict[cur_ts] = memory_size * 8
            # cur_ts += 1

    # x_axis.append(col)
    # y_axis.append(coly)
    x_axis.append(memory_dict.keys())
    y_axis.append(memory_dict.values())



    per_key_state_size = 4096
    sync_keys = 0
    replicate_keys_filter = 0
    reconfig_scenario = "profiling_baseline"

    col = []
    coly = []
    memory_dict = {}
    cur_ts = 0
    f = open(FILE_FOLER + "/spector-{}-{}-{}-{}/log/flink-myc-taskexecutor-0-camel-sane.out"
             .format(per_key_state_size, sync_keys, replicate_keys_filter, reconfig_scenario))
    read = f.readlines()
    start_ts = 0
    for r in read:
        if r.find("++++++ Current memory consumption: ") != -1:
            memory_size_with_ts = r.split(": ")[1]
            timestamp = int(memory_size_with_ts.split("-")[0])
            if start_ts == 0:
                start_ts = timestamp
            cur_ts = (timestamp - start_ts) / 1000
            memory_size = int(memory_size_with_ts.split("-")[1])
            coly.append(memory_size)
            col.append(cur_ts)
            memory_dict[cur_ts] = memory_size * 8
            # cur_ts += 1

    # x_axis.append(col)
    # y_axis.append(coly)
    x_axis.append(memory_dict.keys())
    y_axis.append(memory_dict.values())

    memory_with_spacker = sum(y_axis[0]) / len(y_axis[0])
    memory_with_spacker_norepl = sum(y_axis[1]) / len(y_axis[1])
    memory_without_spacker = sum(y_axis[2]) / len(y_axis[2])

    print(memory_with_spacker, memory_with_spacker_norepl, memory_without_spacker, memory_with_spacker / memory_without_spacker)


    print(x_axis)

    return x_axis, y_axis

if __name__ == '__main__':
    x_axis, y_axis = ReadFile()
    legend_labels = ["Flink w/ Spacker, Repl-100%", "Flink w/ Spacker, Repl-0%", "Flink w/o Spacker"]
    legend = False
    DrawFigure(x_axis, y_axis, legend_labels, "Elapsed Time (s)", "Memory Consumption (mb)", "memory_consumption_tm", legend)