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

    print("Reading streamsluice output:" + streamSluiceOutputPath)
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
                estimateTime = int(split[8].rstrip('\n'))
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


    print(averageGroundTruthLatency)
    fig = plt.figure(figsize=(24, 18))
    print("Draw ground truth curve...")
    legend = ["Ground Truth"]
    plt.plot(averageGroundTruthLatency[0], averageGroundTruthLatency[1], '*', color='gray', markersize=MARKERSIZE)

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
    axes.set_ylim(0, 10000)
    axes.set_yticks(np.arange(0, 10000, 1000))
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + 'ground_truth_latency_curves.png')
    plt.close(fig)


streamSluiceOutputPath = "/home/swrrt11/Workspace/flinks/flink-extended/build-target/log/flink-swrrt11-standalonesession-0-dl.out"
groundTruthPath = "/home/swrrt11/Workspace/flinks/flink-extended/build-target/log/flink-swrrt11-taskexecutor-0-dl.out"
outputDir = "/home/swrrt11/Workspace/StreamSluice/Experiments/test/"
windowSize = 1
draw(streamSluiceOutputPath, groundTruthPath, outputDir, windowSize)
