import os
from math import ceil
import sys
from pathlib import Path

path_root = Path(__file__).parents[3]
sys.path.append(str(path_root))

import matplotlib
import matplotlib as mpl

from analysis.config.default_config import LABEL_FONT_SIZE, LEGEND_FONT_SIZE, TICK_FONT_SIZE, OPT_FONT_NAME, \
    LINE_COLORS, LINE_WIDTH, MARKERS, MARKER_SIZE, FIGURE_FOLDER, FILE_FOLER, PATTERNS, timers_plot
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

    w, h = 2, 3
    y = [[0 for x in range(w)] for y in range(h)]

    repeat_num = 1
    completion_time_dict = {}
    keys = [1, 2, 4, 8, 16, 32, 64, 128, 256]

    per_key_state_size = 32768
    replicate_keys_filter = 0
    sync_keys = 1
    per_task_rate = 6000

    latency_dict = {}

    for order_function in ["default", "reverse"]:
        col = []
        coly = []
        start_ts = float('inf')
        temp_dict = {}
        for tid in range(0, 1):
            f = open(FILE_FOLER + "/spector-{}-{}-{}-{}-{}/Splitter FlatMap-{}.output"
                     .format(per_task_rate, per_key_state_size, sync_keys, replicate_keys_filter, order_function, tid))
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
            coly.append(temp_dict[ts][ceil((len(temp_dict[ts])) * 0.99)])
            col.append(ts - start_ts)


        # Get P95 latency
        coly.sort()
        latency_dict[order_function] = coly[ceil(len(coly)*0.99)]

    for repeat in range(1, repeat_num + 1):
        i = 0
        for order_function in ["default", "reverse"]:
            exp = FILE_FOLER + '/spector-{}-{}-{}-{}-{}'.format(per_task_rate, per_key_state_size, sync_keys,
                                                                          replicate_keys_filter, order_function)
            file_path = os.path.join(exp, "timer.output")
            # try:
            stats = breakdown_total(open(file_path).readlines())
            print(stats)
            for j in range(3):
                if timers_plot[j] not in stats:
                    y[j][i] = 0
                else:
                    y[j][i] += stats[timers_plot[j]]
            i += 1
            # except Exception as e:
            #     print("Error while processing the file {}: {}".format(exp, e))

    for j in range(h):
        for i in range(w):
            y[j][i] = y[j][i] / repeat_num

    for i in range(w):
        completion_time = 0
        for j in range(h):
            completion_time += y[j][i]
        completion_time_dict[keys[i]] = completion_time

    # curve = {}
    #
    # for key in latency_dict:
    #     curve[completion_time_dict[key]] = latency_dict[key]

    x_axis.append(completion_time_dict.values())
    y_axis.append(latency_dict.values())

    return x_axis, y_axis


# draw a line chart
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
                                markevery=1
                               )

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(lines,
                   FIGURE_LABEL,
                   prop=LEGEND_FP,
                   loc='upper center',
                   ncol=3,
                   #                     mode='expand',
                   bbox_to_anchor=(0.5, 1.2), shadow=False,
                   columnspacing=0.1,
                   frameon=True, borderaxespad=0.0, handlelength=1.5,
                   handletextpad=0.1,
                   labelspacing=0.1)

    # plt.yscale('log')
    # plt.ylim(100)
    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')

if __name__ == "__main__":
    x_axis, y_axis = ReadFile()

    print(x_axis, y_axis)
    legend_labels = ["Pareto Curve"]
    legend = True
    DrawFigure(x_axis, y_axis, legend_labels, "Completion Time (ms)", "Latency (ms)", "pareto_curve_ordering", legend)
