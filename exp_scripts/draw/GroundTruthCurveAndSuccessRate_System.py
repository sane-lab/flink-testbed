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

def addLatencyLimitMarker(plt, latencyLimits):
    if (autotuneFlag):
        latencyLimits[0][0] = 0
        for i in range(0, len(latencyLimits[0])):
            x1 = 10000000
            if(i+1 < len(latencyLimits[0])):
                x1 = latencyLimits[0][i+1]
            x = [latencyLimits[0][i], x1]
            y = [latencyLimits[1][i], latencyLimits[1][i]]
            plt.plot(x, y, color='red', linewidth=1.5)
    else:
        x = [0, 10000000]
        y = [latencyLimit, latencyLimit]


def readGroundTruthLatency(rawDir, expName, windowSize):
    initialTime = -1

    groundTruthLatencyPerTuple = {}
    groundTruthLatency = []
    latencyLimits = [[], []]

    taskExecutor = "flink-samza-taskexecutor-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("taskexecutor") == 1:
                taskExecutor = file
    groundTruthPath = rawDir + expName + "/" + taskExecutor
    print("Reading ground truth file:" + groundTruthPath)
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
                if (initialTime == -1 or initialTime > arrivedTime):
                    initialTime = arrivedTime
                if(not isSingleOperator):
                    tupleId = split[4].rstrip()
                    if tupleId not in groundTruthLatencyPerTuple:
                        groundTruthLatencyPerTuple[tupleId] = [arrivedTime, latency]
                    elif groundTruthLatencyPerTuple[tupleId][1] < latency:
                        groundTruthLatencyPerTuple[tupleId][1] = latency
                else:
                    groundTruthLatency += [[arrivedTime, latency]]

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
            if (len(split) >= 7 and split[0] == "[AUTOTUNE]" and split[4] == "initial"):
                latencyLimits[0] += [int(split[2].rstrip('\n'))]
                latencyLimits[1] += [int(split[6].rstrip('\n'))]
            if (len(split) >= 7 and split[0] == "[AUTOTUNE]" and (split[4] == "decrease," or split[4] == "increase,")):
                latencyLimits[0] += [int(split[2].rstrip('\n'))]
                latencyLimits[1] += [int(split[7].rstrip('\n'))]

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

    latencyLimits[0] = [x - initialTime for x in latencyLimits[0]]
    return [averageGroundTruthLatency, initialTime, latencyLimits]

def draw(rawDir, outputDir, exps, windowSize):

    averageGroundTruthLatencies = []

    for i in range(0, len(exps)):
        expFile = exps[i][1]
        result = readGroundTruthLatency(rawDir, expFile, windowSize)
        averageGroundTruthLatencies += [result[0]]
        latencyLimits = result[2]

        #print(averageGroundTruthLatency)
    fig = plt.figure(figsize=(24, 10))
    print("Draw ground truth curve...")
    legend = []
    for i in range(0, len(exps)):
        legend += [exps[i][0]]
        averageGroundTruthLatency = averageGroundTruthLatencies[i]
        plt.plot(averageGroundTruthLatency[0], averageGroundTruthLatency[1], 'o', color=exps[i][2], markersize=2)
    addLatencyLimitMarker(plt, latencyLimits)
    plt.plot()
    plt.legend(legend, loc='upper left')
    plt.xlabel('Time (min)')
    plt.ylabel('Ground Truth Latency (ms)')
    #plt.title('Latency Curves')
    axes = plt.gca()
    axes.set_xlim(startTime * 1000, (startTime + 120) * 1000)
    axes.set_xticks(np.arange(startTime * 1000, (startTime + 120) * 1000 + 20000, 20000))
    #axes.set_xticklabels([int((x - startTime * 1000) / 60000) for x in np.arange(startTime * 1000, (startTime + 660) * 1000 + 30000, 30000)])
    axes.set_ylim(0, 4000)
    axes.set_yticks(np.arange(0, 5000, 1000))
    # axes.set_yscale('log')
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    #plt.savefig(outputDir + 'ground_truth_latency_curves.png')
    plt.savefig(outputDir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.close(fig)


rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
#expName = "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-true-1"
exps = [
    ["StreamSluice", "microbench-system-streamsluice-streamsluice-gradient-3op-60-4000-4000-5000-1000-3-1-0-3-444-1-10000-3-444-1-10000-5-1000-1-10000-2000-2000-100-10-true-1", "blue"],
    ["StreamSluice-1", "microbench-system-streamsluice_earlier-streamsluice-gradient-3op-60-4000-4000-5000-1000-3-1-0-3-444-1-10000-3-444-1-10000-5-1000-1-10000-2000-2000-100-10-true-1", "green"],
    #["StreamSluice-2", "microbench-system-streamsluice_later-op_1_3_keep-gradient-3op-60-4000-4000-5000-1000-10-1-0-3-444-1-10000-3-444-1-10000-5-1000-1-10000-2000-2000-100-10-true-1", "orange"],
]
import sys
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]

windowSize = 200
latencyLimit = 2000
endTime = 270 #150 #630
startTime = 0
isSingleOperator = False #True
autotuneFlag = False
draw(rawDir, outputDir + exps[0][1] + "/", exps, windowSize)
