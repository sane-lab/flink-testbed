import math
import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

OPERATOR_NAMING = {
    "0a448493b4782967b150582570326227": "Stateful Map",
    "c21234bcbf1e8eb4c61f1927190efebd": "Splitter",
    "22359d48bcb33236cf1e31888091e54c": "Counter"
}

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
    print("+++ " + str(averageGroundTruthLatencies))

    successRatePerExps = {}
    for i in range(0, len(exps)):
        totalSuccess = len([x for x in range(0, len(averageGroundTruthLatencies[i][0])) if
                              averageGroundTruthLatencies[i][0][x] >= startTime * 1000 and
                              averageGroundTruthLatencies[i][0][x] <= (startTime + 3600) * 1000 and averageGroundTruthLatencies[i][1][x] <= latencyLimit])
        totalWindows = len([x for x in range(0, len(averageGroundTruthLatencies[i][0])) if
                              averageGroundTruthLatencies[i][0][x] >= startTime * 1000 and
                              averageGroundTruthLatencies[i][0][x] <= (startTime + 3600) * 1000])
        successRatePerExps[exps[i][0]] = totalSuccess / float(totalWindows)
    print(successRatePerExps)
    #fig = plt.figure(figsize=(24, 6))
    fig = plt.figure(figsize=(24, 3))
    print("Draw ground truth curve...")
    legend = []
    for i in range(0, len(exps)):
        legend += [exps[i][0]]
        averageGroundTruthLatency = averageGroundTruthLatencies[i]
        plt.plot(averageGroundTruthLatency[0], averageGroundTruthLatency[1], 'o', color=exps[i][2], markersize=2)
    legend += ["Limit"]
    addLatencyLimitMarker(plt)
    plt.legend(legend, bbox_to_anchor=(0.5, 1.3), loc='upper center', ncol=6, markerscale=4.)
    #plt.xlabel('Time (min)')
    plt.ylabel('Latency (ms)')
    #plt.title('Latency Curves')
    axes = plt.gca()
    axes.set_xlim(startTime * 1000, (startTime + 3600) * 1000)
    axes.set_xticks(np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000))
    axes.set_xticklabels([int((x - startTime * 1000) / 60000) for x in np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000)])
    axes.set_ylim(0, 5000)
    axes.set_yticks(np.arange(0, 6000, 1000))
    # axes.set_yscale('log')
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    #plt.savefig(outputDir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.savefig(outputDir + 'ground_truth_latency_curves.pdf', bbox_inches='tight')
    plt.close(fig)


rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
#expName = "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-true-1"
import sys
exps = [
    #["StreamSluice", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-true-1", "blue"],
    #["StreamSwitch", "stock-sb-4hr-50ms.txt-streamswitch-streamswitch-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-true-1", "green"],
    #["Static-1", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-false-1", "gray"],
    #["Static-2", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-2-1000-1-100-3-2000-1-100-8-5000-1-100-2000-100-false-1", "orange"],
    #["Static-3", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-2-1000-1-100-4-2000-1-100-10-5000-1-100-2000-100-false-1", "brown"],

    # statesize=100
    #["StreamSluice", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-100-3-2000-1-100-7-5000-1-100-2000-100-true-1", "blue"],
    #["StreamSwitch", "stock-sb-4hr-50ms.txt-streamswitch-streamswitch-3690-30-1000-20-2-1000-1-100-3-2000-1-100-7-5000-1-100-2000-100-true-1", "green"],
    #["DS2", "stock-sb-4hr-50ms.txt-ds2-ds2-3690-30-1000-20-2-1000-1-100-3-2000-1-100-7-5000-1-100-2000-100-true-1", "purple"],
    #["Static-1", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-100-3-2000-1-100-7-5000-1-100-2000-100-false-1", "gray"],
    #["Static-2", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-false-1", "orange"],
    #["Static-3", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-3-1000-1-100-4-2000-1-100-9-5000-1-100-2000-100-false-1", "brown"],

    # statesize=2000
    # ["Static-1",
    #  "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-false-1",
    #  "gray", "*"],
    # ["Static-2",
    #  "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-4-1000-1-500-6-2000-1-500-12-5000-1-500-1000-100-false-1",
    #  "orange", "*"],
    # ["DS2", "stock-sb-4hr-50ms.txt-ds2-ds2-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "purple", "d"],
    # ["StreamSwitch",
    #  "stock-sb-4hr-50ms.txt-streamswitch-streamswitch-3690-30-1000-20-3-1000-1-500-4-2000-1-500-9-5000-1-500-1000-100-true-1",
    #  "green", "p"],
    # ["StreamSluice",
    #  "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "blue", "o"],

    # # Split and join
    # ["Static-1",
    #  "stock-split3hBsb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-false-1",
    #  "gray", "*"],
    # ["Static-2",
    #  "stock-split3hBsb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-4-1000-1-500-6-2000-1-500-12-5000-1-500-1000-100-false-1",
    #  "orange", "*"],
    # ["DS2", "stock-split3-sb-4hr-50ms.txt-ds2-ds2-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "purple", "d"],
    # ["StreamSwitch",
    #  "stock-split3-sb-4hr-50ms.txt-streamswitch-streamswitch-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "green", "p"],
    # ["StreamSluice",
    #  "stock-split3hBsb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "blue", "o"],

    # Cluster
    # ["Static-1",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-false-1",
    #  "gray", "*"],
    # ["Static-2",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-4-1000-1-500-6-2000-1-500-12-5000-1-500-1000-100-false-1",
    #  "orange", "*"],
    # ["DS2", "stock-server-split3-sb-4hr-50ms.txt-ds2-ds2-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "purple", "d"],
    # ["StreamSwitch",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamswitch-streamswitch-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "green", "p"],
    #  ["StreamSluice",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "blue", "o"],

    # Change rate
    ["Static-1",
     "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3990-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-false-1",
     "gray", "*"],
    ["Static-2",
     "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3990-30-1000-20-4-1000-1-500-6-2000-1-500-12-5000-1-500-1000-100-false-1",
     "orange", "*"],
    ["DS2", "stock-server-split3-sb-4hr-50ms.txt-ds2-ds2-3990-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
     "purple", "d"],
    ["StreamSwitch",
     "stock-server-split3-sb-4hr-50ms.txt-streamswitch-streamswitch-3990-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
     "green", "p"],
    ["Spacker",
     "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3990-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
     "blue", "o"],
]
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]

windowSize = 500
latencyLimit = 1000
endTime = 270 #150 #630
startTime=30+300 #30
isSingleOperator = False #True
expName = [exp[1] for exp in exps if exp[0] == "StreamSluice" or exp[0] == "Spacker"][0]
print(expName)
draw(rawDir, outputDir + expName + "/", exps, windowSize)
