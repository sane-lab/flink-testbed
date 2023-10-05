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
LINEWIDTH=1.5

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
    plt.plot(x, y, color='red', linewidth=LINEWIDTH)
def addScalingMarker(plt, scalingMarker):
    for scaling in scalingMarker:
        time = scaling[0]
        type = scaling[1]
        if type == 2:
            color = "orange"
        elif type == 1:
            color = "green"
        else:
            color = "gray"
        x = [time, time]
        y = [0, 10000000]
        plt.plot(x, y, color=color, linewidth=LINEWIDTH)

def draw(rawDir, outputDir, expName, windowSize):

    initialTime = -1

    groundTruthLatency = []
    scalingMarkerByOperator = {}
    currentLatency = [[], []]
    currentSpikes = [[], []]
    nextEpochLatency = [[], []]
    nextSpikes = [[], []]

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
            if(split[0] == "GT:"):
                completedTime = int(split[2].rstrip(","))
                latency = int(split[3].rstrip(","))
                arrivedTime = completedTime - latency
                if (initialTime == -1 or initialTime > arrivedTime):
                    initialTime = arrivedTime
                groundTruthLatency += [[arrivedTime, latency]]

    streamSluiceOutputPath = rawDir + expName + "/" + "flink-samza-standalonesession-0-eagle-sane.out"
    print("Reading streamsluice output:" + streamSluiceOutputPath)
    counter = 0
    with open(streamSluiceOutputPath) as f:
        lines = f.readlines()
        lastScalingOperators = []
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            #print(split)
            if (len(split) >= 7 and split[0] == "+++" and split[1] == "[MODEL]" and split[6] == "cur_ete_l:"):
                estimateTime = int(split[3].rstrip('\n'))
                if (initialTime == -1 or initialTime > estimateTime):
                    initialTime = estimateTime
                cLatency = float(split[7].rstrip('\n'))
                nLatency = float(split[9].rstrip('\n'))
                currentLatency[0] += [estimateTime - initialTime]
                currentLatency[1] += [cLatency]
                #nextEpochLatency[0] += [estimateTime]
                #nextEpochLatency[1] += [nLatency]
                if (len(split) <= 10):
                    cSpike = cLatency + 1000
                    nSpike = nLatency + 1000
                else:
                    cSpike = float(split[11].rstrip('\n'))
                    nSpike = float(split[13].rstrip('\n'))
                currentSpikes[0] += [estimateTime - initialTime]
                currentSpikes[1] += [cSpike]
                nextSpikes[0] += [estimateTime - initialTime]
                nextSpikes[1] += [nSpike]
            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale" and split[8] == "operator:"):
                time = int(split[3])
                # if (time > lastTime):
                #    continue
                if (split[7] == "in"):
                    type = 1
                elif (split[7] == "out"):
                    type = 2

                lastScalingOperators = [split[9].lstrip('[').rstrip(']')]
                for operator in lastScalingOperators:
                    if (operator not in scalingMarkerByOperator):
                        scalingMarkerByOperator[operator] = []
                    scalingMarkerByOperator[operator] += [[time - initialTime, type]]
            if (len(split) >= 8 and split[0] == "+++" and split[1] == "[CONTROL]" and split[4] == "all" and split[5] == "scaling" and split[6] == "plan" and split[7] == "deployed."):
                time = int(split[3])
                # if (time > lastTime):
                #    continue
                for operator in lastScalingOperators:
                    if (operator not in scalingMarkerByOperator):
                        scalingMarkerByOperator[operator] = []
                    scalingMarkerByOperator[operator] += [[time - initialTime, 3]]
                lastScalingOperators = []

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
        averageGroundTruthLatency[0] += [int(time)]
        averageGroundTruthLatency[1] += [int(aggregatedGroundTruthLatency[index][0] / float(aggregatedGroundTruthLatency[index][1]))]

    #print(averageGroundTruthLatency)
    fig = plt.figure(figsize=(24, 18))
    print("Draw ground truth curve...")
    legend = []
    #legend += ["Ground Truth"]
    #plt.plot(averageGroundTruthLatency[0], averageGroundTruthLatency[1], '*', color='gray', markersize=MARKERSIZE)
    legend += ["Current Latency"]
    plt.plot(currentLatency[0], currentLatency[1], '*-', color='black',
             markersize=MARKERSIZE)
    legend += ["Current Spike"]
    plt.plot(currentSpikes[0], currentSpikes[1], '*-', color='blue',
             markersize=MARKERSIZE)
    legend += ["Next Epoch Spike"]
    plt.plot(nextSpikes[0], nextSpikes[1], '*-', color='green',
             markersize=MARKERSIZE)
    for operator in scalingMarkerByOperator:
        addScalingMarker(plt, scalingMarkerByOperator[operator])
    addLatencyLimitMarker(plt)

    plt.legend(legend, loc='upper left')
    plt.xlabel('Time (s)')
    plt.ylabel('Latency (ms)')
    plt.title('Latency Curves')
    axes = plt.gca()
    axes.set_xlim(0, averageGroundTruthLatency[0][-1])
    axes.set_xticks(np.arange(0, averageGroundTruthLatency[0][-1], 10000))

    xlabels = []
    for x in range(0, averageGroundTruthLatency[0][-1], 10000):
        xlabels += [str(int(x / 1000))]
    axes.set_xticklabels(xlabels)
    # axes.set_yscale('log')
    axes.set_ylim(0, 3000)
    axes.set_yticks(np.arange(0, 3000, 200))
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + 'latency_curves.png')
    plt.close(fig)

rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
expName = "streamsluice-scaleout-streamsluice_latency_only-streamsluice-180-600-1000-800-240-10-0.25-1000-500-10-100-true-1"
#expName = "streamsluice-scaletest-400-400-550-5-2000-1000-100-1"
windowSize = 1
latencyLimit = 1000
draw(rawDir, outputDir + expName + "/", expName, windowSize)
