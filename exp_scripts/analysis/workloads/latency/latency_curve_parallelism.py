import os
from math import ceil, floor
import sys
from pathlib import Path

path_root = Path(__file__).parents[3]
sys.path.append(str(path_root))

import matplotlib
import matplotlib as mpl

from analysis.config.default_config import LABEL_FONT_SIZE, LEGEND_FONT_SIZE, TICK_FONT_SIZE, OPT_FONT_NAME, \
    LINE_COLORS, LINE_WIDTH, MARKERS, MARKER_SIZE, FIGURE_FOLDER, FILE_FOLER

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

    per_key_state_size = 32768
    replicate_keys_filter = 0
    sync_keys = 0
    state_access_ratio = 2
    per_task_rate = 5000

    for parallelism in [2, 4, 8]:
        col = []
        coly = []
        start_ts = float('inf')
        temp_dict = {}
        for tid in range(0, 1):
            f = open(FILE_FOLER + "/workloads/spector-{}-{}-{}-{}-{}-{}/Splitter FlatMap-{}.output"
                     .format(per_task_rate, per_key_state_size, sync_keys, replicate_keys_filter, parallelism, state_access_ratio, tid))
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
            coly.append(temp_dict[ts][floor((len(temp_dict[ts]))*0.95)])
            col.append(ts - start_ts)

        x_axis.append(col[40:70])
        y_axis.append(coly[40:70])

        # x_axis.append(col)
        # y_axis.append(coly)

    print(x_axis)

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

    plt.yscale('log')
    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')

if __name__ == "__main__":
    x_axis, y_axis = ReadFile()
    legend_labels = ["2", "4", "8"]
    legend = True
    DrawFigure(x_axis, y_axis, legend_labels, "Time(ms)", "Latency(ms)", "latency_curve_parallelism", legend)
