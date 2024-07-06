import getopt
import os
from math import ceil
import sys
from pathlib import Path

path_root = Path(__file__).parents[3]
sys.path.append(str(path_root))

import matplotlib
import matplotlib as mpl
from matplotlib.ticker import PercentFormatter, LogLocator
from numpy import double
from numpy.ma import arange

mpl.use('Agg')

import matplotlib.pyplot as plt
import pylab
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


# there are some embedding problems if directly exporting the pdf figure using matplotlib.
# so we generate the eps format first and convert it to pdf.
def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


# example for reading csv file
def ReadFile():
    x_axis = []
    y_axis = []

    f = open("/home/myc/cpu.csv")
    lines = f.readlines()

    col = []
    coly = []

    ts = 0
    CYCLE_IDX = 1
    FREQUENCY_IDX = 4
    for line in lines:
        arr = line.split(",")
        if len(arr) == 8:
            cpu_util = float(arr[CYCLE_IDX])
            ts += 1
            col.append(ts)
            coly.append(cpu_util)


    x_axis.append(col[0:100])
    y_axis.append(coly[0:100])
    x_axis.append(col[0:100])
    y_axis.append(coly[130:230])

    cpu_with_spacker = sum(coly[0:100]) / len(coly[0:100])
    cpu_without_spacker = sum(coly[130:230]) / len(coly[0:100])

    print(cpu_with_spacker, cpu_without_spacker, cpu_with_spacker / cpu_without_spacker)

    legend_labels = ["Flink w/ Spacker", "Flink w/o Spacker"]

    return legend_labels, x_axis, y_axis


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
        lines[i], = figure.plot(x_values[i], y_values[i], color=LINE_COLORS[i],
                                linewidth=LINE_WIDTH,
                                marker=MARKERS[i], \
                                markersize=MARKER_SIZE,
                                label=FIGURE_LABEL[i],
                                markeredgewidth=2, markeredgecolor='k',
                                markevery=3
                                )

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(lines,
                   FIGURE_LABEL,
                   prop=LEGEND_FP,
                   loc='upper center',
                   ncol=5,
                   #                     mode='expand',
                   bbox_to_anchor=(0.5, 1.35), shadow=False,
                   columnspacing=0.1,
                   frameon=True, borderaxespad=0.0, handlelength=1.5,
                   handletextpad=0.1,
                   labelspacing=0.1)

    plt.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)
    # plt.yscale('log')
    # plt.ylim(0, 100)
    # plt.grid()
    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')


if __name__ == "__main__":
    legend_labels, x_axis, y_axis = ReadFile()
    # legend_labels = ["1", "2", "3", "4", "5"]
    legend = True
    DrawFigure(x_axis, y_axis, legend_labels, "Elapsed Time (s)", "CPU Consumption (ticks)", "cpu_utilization", legend)
