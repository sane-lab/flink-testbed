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
def draw(streamSluiceOutputPath, groundTruthPath, outputDir, windowSize):

    initialTime = -1

    groundTruthLatency = []
    groundTruthLatencyByArrivedTime = {}
    estimatedLatency = []

    print("Reading ground truth file:" + groundTruthPath)
    counter = 0
    with open(groundTruthPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split(' ')
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if(split[0] == "GT:"):
                completedTime = int(split[3].rstrip(","))
                latency = int(split[4])
                arrivedTime = completedTime - latency
                if (initialTime == -1 or initialTime > arrivedTime):
                    initialTime = arrivedTime
                groundTruthLatency += [[completedTime, latency]]
                if(arrivedTime not in groundTruthLatencyByArrivedTime):
                    groundTruthLatencyByArrivedTime[arrivedTime] = []
                groundTruthLatencyByArrivedTime[arrivedTime] += [latency]

    print("Reading streamsluice output:" + groundTruthPath)
    counter = 0
    with open(streamSluiceOutputPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split(' ')
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if (split[0] == "++++++" and split[1] == "Estimated" and split[2] == "End-to-end" and split[3] == "Latency:"):
                estimatedL = float(split[4])
                estimateTime = int(split[8].rstrip('\n'))
                if (initialTime == -1 or initialTime > estimateTime):
                    initialTime = estimateTime
                estimatedLatency += [[estimateTime, estimatedL]]

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

    alignedEstimatedLatency = [[[x[0] - initialTime] for x in estimatedLatency],
                                       [[x[1]] for x in estimatedLatency]]
    alignedAverageGroundTruthLatency = [[], []]
    alignedWorstGroundTruthLatency = [[], []]
    for x in estimatedLatency:
        maxLatency = 0
        totalLatency = 0
        nums = 0
        for time in range(x[0] - int(windowSize/2), x[0] + int(windowSize/2)):
            if time not in groundTruthLatencyByArrivedTime:
                continue
            for y in groundTruthLatencyByArrivedTime[time]:
                totalLatency += y
                nums += 1
                maxLatency = max(maxLatency, y)
        alignedWorstGroundTruthLatency[0] += [x[0] - initialTime]
        alignedWorstGroundTruthLatency[1] += [maxLatency]
        if nums == 0:
            averageLatency = 0.0
        else:
            averageLatency = totalLatency/float(nums)
        alignedAverageGroundTruthLatency[0] += [x[0] - initialTime]
        alignedAverageGroundTruthLatency[1] += [averageLatency]

    estimatedLatencyByCompletedTime = [[[x[0] + x[1] - initialTime] for x in estimatedLatency], [[x[1]] for x in estimatedLatency]]

    print(averageGroundTruthLatency)
    print(alignedEstimatedLatency)
    fig = plt.figure(figsize=(24, 18))
    print("Draw ground truth curve...")
    legend = ["Ground Truth (Window Maximum)"]
    #plt.plot(averageGroundTruthLatency[0], averageGroundTruthLatency[1], '*', color='gray', markersize=MARKERSIZE)
    plt.plot(alignedWorstGroundTruthLatency[0], alignedWorstGroundTruthLatency[1], '*', color='red', markersize=MARKERSIZE)
    legend += ["Ground Truth (Window Average)"]
    plt.plot(alignedAverageGroundTruthLatency[0], alignedAverageGroundTruthLatency[1], '*', color='gray',
             markersize=MARKERSIZE)
    legend += ["Estimated Latency (at Arrived)"]
    plt.plot(alignedEstimatedLatency[0], alignedEstimatedLatency[1], "b*", markersize=MARKERSIZE)
    #legend += ["Estimated Latency (at Completed)"]
    #plt.plot(estimatedLatencyByCompletedTime[0], estimatedLatencyByCompletedTime[1], "g*", markersize=MARKERSIZE)


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
    axes.set_ylim(0, 20000)
    axes.set_yticks(np.arange(0, 15000, 2000))
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + 'latency_curves.png')
    plt.close(fig)


streamSluiceOutputPath = "/home/swrrt11/Workspace/flinks/flink-extended/build-target/log/flink-swrrt11-standalonesession-0-dl.out"
groundTruthPath = "/home/swrrt11/Workspace/flinks/flink-extended/build-target/log/flink-swrrt11-taskexecutor-0-dl.out"
outputDir = "/home/swrrt11/Workspace/StreamSluice/Experiments/test/"
windowSize = 100
draw(streamSluiceOutputPath, groundTruthPath, outputDir, windowSize)
