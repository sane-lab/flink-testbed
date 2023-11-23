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

def draw(rawDir, outputDir, expName, baselineName, windowSize):

    result = readGroundTruthLatency(rawDir, expName, windowSize)
    averageGroundTruthLatency = result[0]
    if baselineName != "":
        result = readGroundTruthLatency(rawDir, baselineName, windowSize)
        baselineLatency = result[0]
    #print(averageGroundTruthLatency)
    fig = plt.figure(figsize=(12, 4))
    print("Draw ground truth curve...")
    legend = ["Ground Truth Latency"]
    plt.plot(averageGroundTruthLatency[0], averageGroundTruthLatency[1], 'D', color='blue', markersize=2)
    if baselineName != "":
        legend += ["Baseline Ground Truth Latency"]
        plt.plot(baselineLatency[0], baselineLatency[1], 'D', color="gray", markersize=MARKERSIZE)
    addLatencyLimitMarker(plt)
    plt.plot()
    plt.legend(legend, loc='upper left')
    plt.xlabel('Time (min)')
    plt.ylabel('Latency (ms)')
    #plt.title('Latency Curves')
    axes = plt.gca()
    axes.set_xlim(0, endTime * 1000)#averageGroundTruthLatency[0][-1])
    axes.set_xticks(np.arange(0, endTime * 1000, 30000))# averageGroundTruthLatency[0][-1], 10000))

    xlabels = []
    for x in range(0, endTime * 1000, 30000): #averageGroundTruthLatency[0][-1], 10000):
        xlabels += [str(int(x / 1000))]
    axes.set_xticklabels(xlabels)
    # axes.set_xlim(startTime * 1000, (startTime + 3600) * 1000)
    # axes.set_xticks(np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000))
    # axes.set_xticklabels([int((x - startTime * 1000) / 60000) for x in np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000)])
    #axes.set_yscale('log')
    axes.set_ylim(0, 10000)
    axes.set_yticks(np.arange(0, 10000, 2000))
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    #plt.savefig(outputDir + 'ground_truth_latency_curves.png')
    plt.savefig(outputDir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.close(fig)


rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
expName = "autotune_4op-false-390-10000-12500-60-12500-20-12500-60-1-0-2-125-1-5000-2-120-1-5000-3-250-1-5000-6-500-5000-2000-1500-100-true-1"
import sys
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]
#expName = "streamsluice-scaletest-400-400-550-5-2000-1000-100-1"
baselineName = "" #"streamsluice-4op-300-5000-5000-10000-120-1-0-2-200-1-100-5-500-1-100-3-333-1-100-3-250-100-1000-500-100-false-1"
windowSize = 500
#latencyLimit = 2000
if expName.count("autotune") > 0:
    latencyLimit = int(expName.split("-")[-5])
else:
    latencyLimit = int(expName.split("-")[-4])
latencyLimit = 3000
endTime = 270 #150 #630
startTime = 0 #120
isSingleOperator = False #True
draw(rawDir, outputDir + expName + "/", expName, baselineName, windowSize)
