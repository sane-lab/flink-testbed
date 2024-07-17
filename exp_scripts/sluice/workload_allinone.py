import math
import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

SMALL_SIZE = 25
MEDIUM_SIZE = 30
BIGGER_SIZE = 35

plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=SMALL_SIZE)    # font-size of the tick labels
plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title
MARKERSIZE=4

outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/infocom/"

exps = {}
currentType = ""
with open("./workload_results.txt") as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split()
        if len(split) > 0 and split[0].startswith("linear_road"):
            expName = split[0]
            exps[currentType][split[0]] = [split[1], float(split[2]), float(split[3]), float(split[4])]
        elif len(split) == 1 and len(split[0]) > 5:
            currentType = split[0]
            exps[currentType] = {}

for exp_type in exps.keys():
    filename = "workload_" + exp_type
    results = {}
    xlabel = ""
    if exp_type == "Input_Rate":
        xlabel = "Input Rate Ratio"
    elif exp_type == "Processing_Rate":
        xlabel = "Process Rate Ratio"
    elif exp_type == "State_Size":
        xlabel = "State Size"
    elif exp_type == "Skewness":
        xlabel = "Skewness Factor"
    xticks_label = []
    success_rate = []
    average_latency = []
    average_resource = []
    for exp in exps[exp_type].keys():
        xticks_label += [exps[exp_type][exp][0]]
        success_rate += [exps[exp_type][exp][1]]
        average_latency += [exps[exp_type][exp][2]]
        average_resource += [exps[exp_type][exp][3]]

    print(success_rate)
    print(average_latency)
    fig, axs = plt.subplots(1, 1, figsize=(12, 9), layout='constrained')  # (24, 9)
    fig.tight_layout(rect=[0.02, 0, 0.953, 1])
    ax1 = axs
    ax2 = ax1.twinx()
    ax1.plot(range(1, len(xticks_label) + 1), success_rate, "d", color="blue")
    ax2.plot(range(1, len(xticks_label) + 1), average_latency, "o-", color="red")
    ax1.set_ylabel("Success Rate")
    ax2.set_ylabel("Average Latency (ms)")
    legend = ["Success Rate"]
    ax1.legend(legend, loc='upper left', bbox_to_anchor=(0, 1.5), ncol=1, markerscale=4.)
    legend = ["Average Latency"]
    ax2.legend(legend, loc='upper right', bbox_to_anchor=(0, 1.5), ncol=1, markerscale=4.)
    ax1.set_xticks(np.arange(0, len(xticks_label) + 2, 1))
    ax1.set_xticklabels([""] + xticks_label + [""])
    ax1.set_xlabel(xlabel)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + filename + "_latency.png", bbox_inches='tight')
    plt.close(fig)

    fig, axs = plt.subplots(1, 1, figsize=(12, 9), layout='constrained')  # (24, 9)
    fig.tight_layout(rect=[0.02, 0, 0.953, 1])
    ax1 = axs
    ax1.bar(xticks_label, average_resource, color="blue")
    ax1.set_ylabel("Average Slots Used")
    legend = ["Slots Used"]
    ax1.legend(legend, loc='upper left', bbox_to_anchor=(0, 1.5), ncol=1, markerscale=4.)
    ax1.set_xlabel(xlabel)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + filename + "_resource.png", bbox_inches='tight')
    plt.close(fig)


