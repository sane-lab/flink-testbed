import os
from math import ceil

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

OPT_FONT_NAME = 'Helvetica'
TICK_FONT_SIZE = 20
LABEL_FONT_SIZE = 24
LEGEND_FONT_SIZE = 26
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)

MARKERS = (['o', 's', 'v', "^", "h", "v", ">", "x", "d", "<", "|", "", "|", "_"])
# you may want to change the color map for different figures
COLOR_MAP = ('#B03A2E', '#2874A6', '#239B56', '#7D3C98', '#F1C40F', '#F5CBA7', '#82E0AA', '#AEB6BF', '#AA4499')
# you may want to change the patterns for different figures
PATTERNS = (["\\", "///", "o", "||", "\\\\", "\\\\", "//////", "//////", ".", "\\\\\\", "\\\\\\"])
LABEL_WEIGHT = 'bold'
LINE_COLORS = COLOR_MAP
LINE_WIDTH = 3.0
MARKER_SIZE = 0.0
MARKER_FREQUENCY = 1000

matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True
matplotlib.rcParams['xtick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['ytick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['font.family'] = OPT_FONT_NAME

FIGURE_FOLDER = '/data/results'
FILE_FOLER = '/data/raw'


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
    plt.ylim(1)
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

    col_avg = []
    coly_avg = []

    col_median = []
    coly_median = []

    col_p95 = []
    coly_p95 = []

    col_p99 = []
    coly_p99 = []
    start_ts = float('inf')
    temp_dict = {}
    for tid in range(0, 1):
        f = open("/data/spector" + "/Splitter FlatMap-{}.output".format(tid))
        read = f.readlines()
        for r in read:
            if r.find("endToEnd latency: ") != -1:
                ts = int(int(r.split("ts: ")[1][:13])/100)
                if ts < start_ts: # find the starting point from parallel tasks
                    start_ts = ts
                latency = int(r.split("endToEnd latency: ")[1])
                if ts not in temp_dict:
                    temp_dict[ts] = []
                temp_dict[ts].append(latency)

        for ts in temp_dict:
            temp_dict[ts].sort()

            # coly_avg.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
            coly_avg.append(len(temp_dict[ts]))
            col_avg.append(ts - start_ts)

            coly_median.append(temp_dict[ts][ceil((len(temp_dict[ts])) * 0.5)])
            col_median.append(ts - start_ts)

            coly_p95.append(temp_dict[ts][ceil((len(temp_dict[ts])) * 0.95)])
            col_p95.append(ts - start_ts)

            coly_p99.append(temp_dict[ts][ceil((len(temp_dict[ts])) * 0.99)])
            col_p99.append(ts - start_ts)



        x_axis.append(col_avg)
        y_axis.append(coly_avg)

        x_axis.append(col_median)
        y_axis.append(coly_median)

        x_axis.append(col_p95)
        y_axis.append(coly_p95)

        x_axis.append(col_p99)
        y_axis.append(coly_p99)

    print(x_axis)

    return x_axis, y_axis

if __name__ == '__main__':
    x_axis, y_axis = ReadFile()
    legend_labels = ['avg', 'median', 'p95', 'p99']
    legend = True
    DrawFigure(x_axis, y_axis, legend_labels, "Time(ms)", "Latency(ms)", "latency_curve_test", legend)