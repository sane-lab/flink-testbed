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
with open("../workload_result.txt") as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split()
        if len(split) > 0 and split[0].startswith("linear_road"):
            expName = split[0]
            exps[currentType][split[0]] = [float(split[1]), float(split[2]), float(split[3])]
        elif len(split) == 1 and len(split[0]) > 5:
            currentType = split[0]
            exps[currentType] = {}

for exp_type in exps.keys():
    filename = "workload_" + exp_type
    results = {}
    xlabel = ""
    if exp_type == "Input_Rate":
        xlabel = "Input Rate Ratio"
        for exp in exps[exp_type].keys():

    elif exp_type == "Processing_Rate":
        xlabel = ""
    elif exp_type == "State_Size":
        xlabel = ""
    elif exp_type == "Skewness":
        xlabel = ""


