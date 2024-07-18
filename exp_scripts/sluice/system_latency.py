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

def addLatencyLimitMarker(plt):
    x = [0, 10000000]
    y = [latencyLimit, latencyLimit]
    plt.plot(x, y, color='red', linewidth=1.5)
def readGroundTruthLatency(rawDir, expName, windowSize):
    initialTime = -1

    groundTruthLatencyPerTuple = {}
    groundTruthLatency = []

    taskExecutors = []  # "flink-samza-taskexecutor-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("taskexecutor") == 1:
                taskExecutors += [file]
    fileInitialTimes = {}
    for taskExecutor in taskExecutors:
        groundTruthPath = rawDir + expName + "/" + taskExecutor
        print("Reading ground truth file:" + groundTruthPath)
        fileInitialTime = - 1
        counter = 0
        with open(groundTruthPath) as f:
            lines = f.readlines()
            for i in range(0, len(lines)):
                line = lines[i]
                split = line.rstrip().split()
                counter += 1
                if (counter % 5000 == 0):
                    print("Processed to line:" + str(counter))
                if (split[0] == "GT:"):
                    completedTime = int(split[2].rstrip(","))
                    latency = int(split[3].rstrip(","))
                    arrivedTime = completedTime - latency
                    if (fileInitialTime == -1 or fileInitialTime > arrivedTime):
                        fileInitialTime = arrivedTime
                    if(not isSingleOperator):
                        tupleId = split[4].rstrip()
                        if tupleId not in groundTruthLatencyPerTuple:
                            groundTruthLatencyPerTuple[tupleId] = [arrivedTime, latency]
                        elif groundTruthLatencyPerTuple[tupleId][1] < latency:
                            groundTruthLatencyPerTuple[tupleId][1] = latency
                    else:
                        groundTruthLatency += [[arrivedTime, latency]]
        fileInitialTimes[taskExecutor] = fileInitialTime
        if (initialTime == -1 or initialTime > fileInitialTime):
            initialTime = fileInitialTime
    print("FF: " + str(fileInitialTimes))
    if(not isSingleOperator):
        for value in groundTruthLatencyPerTuple.values():
            groundTruthLatency += [value]

    streamsluiceOutput = "flink-samza-standalonesession-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("standalonesession") == 1:
                streamsluiceOutput = file
    streamSluiceOutputPath = rawDir + expName + "/" + streamsluiceOutput
    print("Reading streamsluice output:" + streamSluiceOutputPath)
    counter = 0
    with open(streamSluiceOutputPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if (len(split) >= 7 and split[0] == "+++" and split[1] == "[MODEL]" and split[6] == "cur_ete_l:"):
                estimateTime = int(split[3].rstrip('\n'))
                if (initialTime == -1 or initialTime > estimateTime):
                    initialTime = estimateTime

    aggregatedGroundTruthLatency = {}
    for pair in groundTruthLatency:
        index = int((pair[0] - initialTime) / windowSize)
        if index not in aggregatedGroundTruthLatency:
            aggregatedGroundTruthLatency[index] = []
        aggregatedGroundTruthLatency[index] += [pair[1]]

    averageGroundTruthLatency = [[], []]
    for index in sorted(aggregatedGroundTruthLatency):
        time = index * windowSize
        x = int(time)
        if index in aggregatedGroundTruthLatency:
            sortedLatency = sorted(aggregatedGroundTruthLatency[index])
            size = len(sortedLatency)
            target = min(math.ceil(size * 0.99), size) - 1
            y = sortedLatency[target]
            averageGroundTruthLatency[0] += [x]
            averageGroundTruthLatency[1] += [y]
    return [averageGroundTruthLatency, initialTime]

def draw(rawDir, outputDir, exps, windowSize):
    averageGroundTruthLatencies = []
    for i in range(0, len(exps)):
        expFile = exps[i][1]
        result = readGroundTruthLatency(rawDir, expFile, windowSize)
        averageGroundTruthLatencies += [result[0]]
    # print("+++ " + str(averageGroundTruthLatencies))

    successRatePerExps = {}
    for i in range(0, len(exps)):
        totalSuccess = len([x for x in range(0, len(averageGroundTruthLatencies[i][0])) if
                              averageGroundTruthLatencies[i][0][x] >= startTime * 1000 and
                              averageGroundTruthLatencies[i][0][x] <= (startTime + 1800) * 1000 and averageGroundTruthLatencies[i][1][x] <= latencyLimit])
        totalWindows = len([x for x in range(0, len(averageGroundTruthLatencies[i][0])) if
                              averageGroundTruthLatencies[i][0][x] >= startTime * 1000 and
                              averageGroundTruthLatencies[i][0][x] <= (startTime + 1800) * 1000])
        successRatePerExps[exps[i][0]] = totalSuccess / float(totalWindows)

        overall_latency[app][i] = [averageGroundTruthLatencies[i][1][x] for x in range(0, len(averageGroundTruthLatencies[i][0])) if
                              averageGroundTruthLatencies[i][0][x] >= startTime * 1000 and
                              averageGroundTruthLatencies[i][0][x] <= (startTime + 1800) * 1000]
        print("Avg latencies: " + str(sum([averageGroundTruthLatencies[i][1][x] for x in range(0, len(averageGroundTruthLatencies[i][0])) if
                              averageGroundTruthLatencies[i][0][x] >= startTime * 1000 and
                              averageGroundTruthLatencies[i][0][x] <= (startTime + 1800) * 1000])/len([averageGroundTruthLatencies[i][1][x] for x in range(0, len(averageGroundTruthLatencies[i][0])) if
                              averageGroundTruthLatencies[i][0][x] >= startTime * 1000 and
                              averageGroundTruthLatencies[i][0][x] <= (startTime + 1800) * 1000])))
    print(successRatePerExps)
    #print(averageGroundTruthLatencies)
    #fig = plt.figure(figsize=(24, 3))
    fig = plt.figure(figsize=(10, 5))
    print("Draw ground truth curve...")
    legend = []
    for i in range(0, len(exps)):
        legend += [exps[i][0]]
        averageGroundTruthLatency = averageGroundTruthLatencies[i]

        sample_factor = 5
        sampledLatency = [[], []]
        sampledLatency[0] = [averageGroundTruthLatency[0][i] for i in range(0, len(averageGroundTruthLatency[0]), sample_factor)]
        sampledLatency[1] = [max([averageGroundTruthLatency[1][y] for y in range(x, min(x + sample_factor, len(averageGroundTruthLatency[1])))]) for x in range(0, len(averageGroundTruthLatency[0]), sample_factor)]

        #plt.plot(averageGroundTruthLatency[0], averageGroundTruthLatency[1], 'o-', color=exps[i][2], markersize=2, linewidth=2)
        plt.plot(sampledLatency[0], sampledLatency[1], 'o-', color=exps[i][2], markersize=4,
                 linewidth=2)

    legend += ["Limit"]
    addLatencyLimitMarker(plt)
    plt.legend(legend, bbox_to_anchor=(0.5, 1.3), loc='upper center', ncol=6, markerscale=4.)
    #plt.xlabel('Time (min)')
    plt.ylabel('Latency (ms)')
    #plt.title('Latency Curves')
    axes = plt.gca()
    axes.set_xlim(startTime * 1000, (startTime + 600) * 1000)
    axes.set_xticks(np.arange(startTime * 1000, (startTime + 600) * 1000 + 300000, 300000))
    axes.set_xticklabels([int((x - startTime * 1000) / 60000) for x in np.arange(startTime * 1000, (startTime + 600) * 1000 + 300000, 300000)])
    #axes.set_ylim(0, 5000)
    #axes.set_yticks(np.arange(0, 6000, 1000))
    axes.set_ylim(0, 10000)
    axes.set_yticks(np.arange(0, 12000, 2000))
    # axes.set_yscale('log')
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    #plt.savefig(outputDir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.savefig(outputDir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.close(fig)

rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"

exps = ["systemsensitivity-streamsluice-streamsluice-when-1split2join1-330-6000-4000-5000-1-0-3-444-1-10000-3-444-1-10000-3-444-1-10000-5-500-10000-2500-2000-100-10-true-1"]

import sys
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]

overall_latency = {}
for exp in exps:
    windowSize = 500
    latencyLimit = 2500 #1000
    startTime=20 #+300 #30
    isSingleOperator = False #True
    expName = exp
    print(expName)
    draw(rawDir, outputDir + expName + "/", exp, windowSize)

