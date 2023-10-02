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

def addLatencyLimitMarker(plt, limit):
    x = [0, 10000000]
    y = [limit, limit]
    plt.plot(x, y, color='red', linewidth=1.5)
def readGroundTruthLatency(rawDir, expName, windowSize):
    initialTime = -1

    groundTruthLatency = []

    groundTruthPath = rawDir + expName + "/" + "flink-samza-taskexecutor-0-eagle-sane.out"
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
                latency = int(split[3])
                arrivedTime = completedTime - latency
                if (initialTime == -1 or initialTime > arrivedTime):
                    initialTime = arrivedTime
                groundTruthLatency += [[arrivedTime, latency]]

    streamSluiceOutputPath = rawDir + expName + "/" + "flink-samza-standalonesession-0-eagle-sane.out"
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
        index = (pair[0] - initialTime) / windowSize
        if index not in aggregatedGroundTruthLatency:
            aggregatedGroundTruthLatency[index] = [0, 0]
        aggregatedGroundTruthLatency[index][0] += pair[1]
        aggregatedGroundTruthLatency[index][1] += 1

    averageGroundTruthLatency = [[], []]
    for index in sorted(aggregatedGroundTruthLatency):
        time = index * windowSize
        x = int(time)
        y = int(aggregatedGroundTruthLatency[index][0] / float(aggregatedGroundTruthLatency[index][1]))
        averageGroundTruthLatency[0] += [x]
        averageGroundTruthLatency[1] += [y]
    return [averageGroundTruthLatency, initialTime]

def draw(rawDir, outputDir, expName, baselineName, windowSize):

    result = readGroundTruthLatency(rawDir, expName, windowSize)
    averageGroundTruthLatency = result[0]
    if baselineName != "":
        result = readGroundTruthLatency(rawDir, baselineName, windowSize)
        baselineLatency = result[0]
    print(averageGroundTruthLatency)
    fig = plt.figure(figsize=(24, 18))
    print("Draw ground truth curve...")
    legend = ["StreamSluice Ground Truth Latency"]
    plt.plot(averageGroundTruthLatency[0], averageGroundTruthLatency[1], '*', color='blue', markersize=MARKERSIZE)
    if baselineName != "":
        legend += ["Baseline Ground Truth Latency"]
        plt.plot(baselineLatency[0], baselineLatency[1], '*', color="gray", markersize=MARKERSIZE)
    addLatencyLimitMarker(plt, 2000)
    plt.plot()
    plt.legend(legend, loc='upper left')
    plt.xlabel('Time (s)')
    plt.ylabel('Latency (ms)')
    plt.title('Latency Curves')
    axes = plt.gca()
    axes.set_xlim(0, 240000)#averageGroundTruthLatency[0][-1])
    axes.set_xticks(np.arange(0, 240000, 10000))# averageGroundTruthLatency[0][-1], 10000))

    xlabels = []
    for x in range(0, 240000, 10000): #averageGroundTruthLatency[0][-1], 10000):
        xlabels += [str(int(x / 1000))]
    axes.set_xticklabels(xlabels)
    # axes.set_yscale('log')
    axes.set_ylim(0, 5000)
    axes.set_yticks(np.arange(0, 5000, 500))
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + 'ground_truth_latency_curves.png')
    plt.close(fig)


rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
expName = "streamsluice-scaletest-400-600-500-5-2000-1000-100-1"
#expName = "streamsluice-scaletest-400-400-550-5-2000-1000-100-1"
baselineName = "streamsluice-scaletest-400-600-500-5-2000-1000-100-false-1"
windowSize = 1
draw(rawDir, outputDir + expName + "/", expName, baselineName, windowSize)
