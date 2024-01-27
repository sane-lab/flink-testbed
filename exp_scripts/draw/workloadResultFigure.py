import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

topologyToIndex = {
    "1op": 1,
    "2op": 2,
    "1split2": 3,
    "1split3": 4,
    "3op": 5,
    "2split2": 6,
}
topologyName = {}
for topology in topologyToIndex.keys():
    topologyName[topologyToIndex[topology]] = topology

def parseSetting(fileName):
    split = fileName.split('-')
    setting = {}
    if split[2] == "server":
        startIndex = 1
    else:
        startIndex = 0
    setting["topology"] = split[startIndex + 2]
    setting["amplitude"] = split[startIndex + 7]
    setting["period"] = split[startIndex + 8]
    setting["skewness"] = split[startIndex + 10]
    setting["statesize"] = split[startIndex + 14]
    return setting


def checkIsThisDimension(dimension, dimensions, baseline, expSetting):
    for other in dimensions:
        if other != dimension and baseline[other] != expSetting[other]:
            return False
    return True

outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/workload_1split2/"

exps = {}
with open("../workload_result.txt") as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split()
        if len(split) > 0 and split[0].startswith("microbench"):
            expName = split[0]
            result_str = lines[i+1].rstrip().split()
            result = [int(result_str[0])]
            for j in range(1, len(result_str)):
                result += [float(result_str[j])]
            exps[expName] = result


dimensions = ["amplitude", "period", "statesize", "skewness", "topology"]
baselineSetting = parseSetting("microbench-workload-server-1split2-3660-10000-10000-10000-5000-120-1-0-4-50-1-10000-4-50-1-10000-60-1000-1-10000-1200-500-100-true-1")
baseRange = 10000
for dimension in dimensions:
    print("For dimension " + dimension)
    orderResult = {}
    for exp in exps.keys():
        expSetting = parseSetting(exp)
        if checkIsThisDimension(dimension, dimensions, baselineSetting, expSetting):
            print(expSetting)
            if dimension == "topology":
                orderResult[topologyToIndex[expSetting[dimension]]] = exps[exp]
            else:
                orderResult[float(expSetting[dimension])] = exps[exp]

    x = []
    y = []
    utilizations = {}
    opSize = 0
    keyIndex = 0
    for key in sorted(orderResult.keys()):
        keyIndex += 1
        if dimension == 'amplitude':
            # lower = baseRange - key
            # upper = baseRange + key
            # x += [str(lower) + '-\n' + str(upper)]
            x += ['%.1f%%' % (key/float(baseRange) * 100)]
        elif dimension == 'statesize':
            x += ['%.0fMB' % (key /100)]
        elif dimension == 'period':
            x += ['%.0fs' % (key)]
        elif dimension == 'skewness':
            x += [str(key)]
        elif dimension == 'topology':
            x += [topologyName[key]] #[str(int(key)) + '_OP']
        y += [orderResult[key][0]]
        opSize = len(orderResult[key]) - 1
        for opIndex in range(1, len(orderResult[key])):
            opName = "OP_" + str(opIndex)
            if opName not in utilizations:
                utilizations[opName] = []
            if dimension == "topology" and len(utilizations[opName]) < keyIndex - 1:
                utilizations[opName] += [0] * (keyIndex - 1 - len(utilizations[opName]))
            utilizations[opName] += [orderResult[key][opIndex] * 100]

    print(x)
    print(y)
    print(utilizations)
    print("Draw dimension " + dimension)
    fig, axs = plt.subplots(1, 2, figsize=(12, 2), layout='constrained') #(24, 9)
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
        if dimension == "topology":
            nx = np.arange(len(utilizationList))
        rects = ax1.bar(nx + offset, utilizationList, width, label=opName)
        ax1.bar_label(rects, fmt='%.0f', padding=3, label_type='center')
        ax1.set_ylim(0, 100)
        ax1.set_yticks(np.arange(0, 120, 20))
        multiplier += 1
    ax1.set_xticks(np.arange(len(orderResult.keys())))
    ax1.set_xticklabels(x)

    # plt.plot(x, y, "*", markersize=6)
    ax1.set_title("Average utiliation under different " + dimension)
    ax1.set_ylabel("Utilization (%)")
    ax1.legend(loc='upper left', ncols=3)

    #plt.xlabel(dimension)

    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    #plt.savefig(outputDir + figName + ".pdf", bbox_inches='tight')
    plt.close(fig)
