import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def parseSetting(fileName):
    split = fileName.split('-')
    setting = {}
    setting["topology"] = split[2]
    setting["amplitude"] = split[7]
    setting["period"] = split[8]
    setting["skewness"] = split[10]
    setting["statesize"] = split[14]
    return setting


def checkIsThisDimension(dimension, dimensions, baseline, expSetting):
    for other in dimensions:
        if other != dimension and baseline[other] != expSetting[other]:
            return False
    return True

outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/"

exps = {}
with open("../workload_result.txt") as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split()
        if split[0].startswith("microbench"):
            expName = split[0]
            result_str = lines[i+1].rstrip().split()
            result = [int(result_str[0])]
            for j in range(1, len(result_str)):
                result += [float(result_str[j])]
            exps[expName] = result

dimensions = ["amplitude", "period", "statesize", "skewness", "topology"]
baselineSetting = parseSetting("microbench-workload-2op-3660-10000-10000-10000-5000-120-1-0-1-50-1-10000-12-1000-1-10000-4-357-1-10000-1000-500-100-true-1")
baseRange = 10000
for dimension in dimensions:
    print("For dimension " + dimension)
    orderResult = {}
    for exp in exps.keys():
        expSetting = parseSetting(exp)
        if checkIsThisDimension(dimension, dimensions, baselineSetting, expSetting):
            print(expSetting)
            orderResult[float(expSetting[dimension])] = exps[exp]
    x = []
    y = []
    utilizations = {}
    opSize = 0
    for key in sorted(orderResult.keys()):
        if dimension == 'amplitude':
            lower = baseRange - key
            upper = baseRange + key
            x += [str(lower) + '-\n' + str(upper)]
        elif dimension == 'statesize':
            x += [str(key)]
        elif dimension == 'period':
            x += [str(key)]
        elif dimension == 'skew':
            x += [str(key)]
        y += [orderResult[key][0]]
        opSize = len(orderResult[key]) - 1
        for opIndex in range(1, len(orderResult[key])):
            opName = "OP_" + str(opIndex)
            if opName not in utilizations:
                utilizations[opName] = []
            utilizations[opName] += [orderResult[key][opIndex]]

    print(x)
    print(y)
    print(utilizations)
    print("Draw dimension " + dimension)
    fig, axs = plt.subplots(1, 2, figsize=(10, 4), layout='constrained') #(24, 9)
    figName = "limit_" + dimension
    ax1 = axs[0]
    p = ax1.bar(x, y, width=0.8, bottom=None, align='center')
    ax1.bar_label(p, label_type='center')
    #plt.plot(x, y, "*", markersize=6)
    ax1.set_title("Best latency limit under different " + str(dimension))
    ax1.set_ylabel("Best Limit(ms)")

    print("Draw utilizations...")
    print(utilizations)
    ax1 = axs[1]
    width = 1.0/(opSize + 1)
    multiplier = 0
    nx = np.arange(len(orderResult.keys()))
    for opName, utilizationList in utilizations.items():
        offset = width * multiplier
        rects = ax1.bar(nx + offset, utilizationList, width, label=opName)
        ax1.bar_label(rects, padding=3, label_type='center')
        multiplier += 1
    ax1.set_xticks(nx)
    ax1.set_xticklabels(x)

    # plt.plot(x, y, "*", markersize=6)
    ax1.set_title("Average utiliation under different " + dimension)
    ax1.set_ylabel("Utilization")
    ax1.legend(loc='upper left', ncols=3)

    #plt.xlabel(dimension)

    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figName + ".pdf", bbox_inches='tight')
    plt.close(fig)
