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

def addScalingMarker(plt, scalingMarker, estimatedLatencySpike):
    lastY = 0
    for scaling in scalingMarker:
        time = scaling[0]
        type = scaling[1]
        if type == 2:
            color = "orange"
        elif type == 1:
            color = "orange"
        else:
            color = "gray"
        x = [time, time]
        if type == 3:
            y1 = lastY
        else:
            y1 = 2000
            for spike in estimatedLatencySpike:
                if spike[0][0] == time:
                    y1 = spike[1][0]
                    break
            lastY = y1
        y = [0, y1]
        plt.plot(x, y, color=color, linewidth=1)

def addLatencyLimitMarker(plt):
    x = [0, 10000000]
    y = [latencyLimit, latencyLimit]
    plt.plot(x, y, color='red', linewidth=1.5)
def addLatencyLimitCurveMarker(plt, limits):
    for i in range(0, len(limits[0]) - 1):
        plt.plot(limits[0][i:i+2], limits[1][i:i+2], color='red', linewidth=1.5)
def readGroundTruthLatencyAndEstimatedSpikes(rawDir, expName, windowSize):
    initialTime = -1

    groundTruthLatencyPerTuple = {}
    groundTruthLatency = []

    estimatedLatency = [[], []]
    estimatedLatencySpike = []
    scalingMarker = []
    limits = [[], []]
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
                estimatedLatency[0] += [estimateTime]
                estimatedLatency[1] += [float(split[7])]
            if (len(split) >= 7 and split[0] == "+++" and split[1] == "[MODEL]" and split[6] == "new_ete_l:"):
                estimateTime = int(split[3].rstrip('\n'))
                if (initialTime == -1 or initialTime > estimateTime):
                    initialTime = estimateTime
                estimatedLatencySpike += [[[estimateTime, estimateTime + float(split[9])], [float(split[7]), float(split[7])]]]
            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale" and split[
                8] == "operator:"):
                time = int(split[3])
                if (initialTime == -1 or initialTime > time):
                    initialTime = time
                # if (time > lastTime):
                #    continue
                if (split[7] == "in"):
                    type = 1
                elif (split[7] == "out"):
                    type = 2
                scalingMarker += [[time - initialTime, type]]
            if (len(split) >= 8 and split[0] == "+++" and split[1] == "[CONTROL]" and split[4] == "all" and split[
                5] == "scaling" and split[6] == "plan" and split[7] == "deployed."):
                time = int(split[3])
                if (initialTime == -1 or initialTime > time):
                    initialTime = time
                # if (time > lastTime):
                #    continue
                scalingMarker += [[time - initialTime, 3]]
            if (len(split) >= 8 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "current" and split[
                7] == "limit:"):
                time = int(split[3])
                if (initialTime == -1 or initialTime > time):
                    initialTime = time
                limit = int(split[8])
                limits[0] += [time]
                limits[1] += [limit]
            if (len(split) >= 8 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "current" and split[
                7] == "limit:"):
                time = int(split[3])
                if (initialTime == -1 or initialTime > time):
                    initialTime = time
                limit = int(split[8])
                limits[0] += [time]
                limits[1] += [limit]
            if (len(split) >= 16 and split[0] == "+++" and split[1] == "[CONTROL]" and split[15] == "new" and split[
                16] == "limit:"):
                time = int(split[3])
                if (initialTime == -1 or initialTime > time):
                    initialTime = time
                limit = int(split[17])
                limits[0] += [time]
                limits[1] += [limit]
            if (len(split) >= 13 and split[0] == "+++" and split[1] == "[CONTROL]" and split[11] == "new" and split[
                12] == "limit:"):
                time = int(split[3])
                if (initialTime == -1 or initialTime > time):
                    initialTime = time
                limit = int(split[13])
                limits[0] += [time]
                limits[1] += [limit]

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
    estimatedLatency[0] = [x - initialTime for x in estimatedLatency[0]]
    estimatedLatencySpike = [[[spike[0][0] - initialTime, spike[0][1] - initialTime], [spike[1][0], spike[1][1]]] for spike in estimatedLatencySpike]
    limits[0] = [x - initialTime for x in limits[0]]
    return [averageGroundTruthLatency, estimatedLatency, estimatedLatencySpike, scalingMarker, limits]

def draw(rawDir, outputDir, expName, windowSize):

    result = readGroundTruthLatencyAndEstimatedSpikes(rawDir, expName, windowSize)
    averageGroundTruthLatency = result[0]
    estimatedLatency = result[1]
    estimatedLatencySpike = result[2]
    scalingMarker = result[3]
    limits = result[4]
    fig = plt.figure(figsize=(24, 12))
    print("Draw ground truth curve...")
    legend = ["Ground Truth Latency"]
    plt.plot(averageGroundTruthLatency[0], averageGroundTruthLatency[1], 'D', color='gray', markersize=2)
    legend += ["Estimated Latency & Spikes"]
    plt.plot(estimatedLatency[0], estimatedLatency[1], 'D', color='blue', markersize=2)
    #print(estimatedLatencySpike)
    for spike in estimatedLatencySpike:
        print(spike)
        plt.plot(spike[0], spike[1], color='red', linewidth=4)
            #plt.plot(estimatedLatencySpike[0], estimatedLatencySpike[1], 'o', color='red', markersize=4)

    if limitLineFlag:
        addLatencyLimitCurveMarker(plt, limits)
    else:
        addLatencyLimitMarker(plt)
    print(scalingMarker)
    addScalingMarker(plt, scalingMarker, estimatedLatencySpike)
    plt.plot()
    plt.legend(legend, loc='upper left')
    plt.xlabel('Time (s)')
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
    #axes.set_ylim(0, 30000)
    #axes.set_yticks(np.arange(0, 30000, 5000))
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    #plt.savefig(outputDir + 'ground_truth_latency_curves.png')
    plt.savefig(outputDir + 'latency_curves.png', bbox_inches='tight')
    plt.close(fig)


rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
expName = "autotune4op-false-270-10000-17500-240-17500-240-19000-120-1-0-1-40-1-5000-2-111-1-5000-2-142-1-5000-3-250-5000-3000-100-1500-100-true-1"
import sys
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]
#expName = "streamsluice-scaletest-400-400-550-5-2000-1000-100-1"
windowSize = 100
#latencyLimit = 2000
if expName.count("autotune") > 0:
    latencyLimit = int(expName.split("-")[-5])
else:
    latencyLimit = int(expName.split("-")[-4])
latencyLimit = 3000
limitLineFlag = True
endTime = 390 #630
startTime = 0
isSingleOperator = False #True
draw(rawDir, outputDir + expName + "/", expName, windowSize)
