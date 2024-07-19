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
    axes.set_xlim(startTime * 1000, (startTime + 1800) * 1000)
    axes.set_xticks(np.arange(startTime * 1000, (startTime + 1800) * 1000 + 300000, 300000))
    axes.set_xticklabels([int((x - startTime * 1000) / 60000) for x in np.arange(startTime * 1000, (startTime + 1800) * 1000 + 300000, 300000)])
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


workload_label = {
    0: "Stock",
    1: "Twitter",
    2: "Linear_Road",
    3: "Spam_Detection",
}
controller_label = {
    0: "DS2",
    1: "StreamSwitch",
    2: "Sluice",
}
controller_color = {
    0: "#fdae61",
    1: "#abdda4",
    2: "#2b83ba",
}
def drawOverallLatency(output_directory: str, workload_latency: dict):
    print("Draw latency")
    data_controller = {}
    workload_labels = []
    for workload_index in range(0, len(workload_latency.keys())):
        workload = sorted(workload_latency.keys())[workload_index]
        for controller_index in range(0, len(workload_latency[workload].keys())):
            controller = sorted(workload_latency[workload].keys())[controller_index]
            if controller_index not in data_controller:
                data_controller[controller] = [[], [], []]
            data_controller[controller_index][workload_index] = workload_latency[workload][controller]
        workload_labels.append(workload)

    #print(data_controller)
    print(workload_labels)
    def set_box_color(bp, color):
        plt.setp(bp['boxes'], color=color)
        plt.setp(bp['whiskers'], color=color)
        plt.setp(bp['caps'], color=color)
        plt.setp(bp['medians'], color='r') #color)

    fig, axs = plt.subplots(1, 1, figsize=(12, 9), layout='constrained') #(24, 9)
    figName = "Overall_latency.png"
    for controller in range(0, 3):
        bp = plt.boxplot(data_controller[controller], positions=np.array(range(len(data_controller[controller]))) * 3.0 - 0.4 + controller * 0.4, sym='+', widths=0.4)
        set_box_color(bp, controller_color[controller])
        plt.plot([], c=controller_color[controller], label=controller_label[controller])
    plt.legend()
    plt.xticks(range(0, len(workload_labels) * 3, 3), workload_labels)
    plt.plot([-3, len(workload_labels) * 3], [0.01, 0.01], color="red")
    plt.xlim(-3, len(workload_labels)*3)
    plt.ylim(100, 200000)
    plt.yscale("log")
    plt.gca().set_yticks([100, 1000, 10000, 100000])
    plt.xlabel("Workload")
    plt.ylabel("Latency(ms)")
    #plt.gca().invert_yaxis()
    #plt.gca().set_yticklabels(1 - plt.gca().get_yticks())
    plt.tight_layout()
    plt.savefig(output_directory+figName)




rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"

exps = {
    "Stock": [
        ["DS2",
         "stock_analysis-ds2-ds2-3990-30-1000-20-2-500-6-5000-3-1000-4-3000-1-5-4000-2000-100-true-3-true-1",
         "purple", "d"],
        ["StreamSwitch",
         "stock_analysis-streamswitch-streamswitch-3990-30-1000-20-2-500-6-5000-3-1000-4-3000-1-5-4000-2000-100-true-3-true-5",
         #"stock_analysis-streamswitch-streamswitch-2190-30-1000-20-2-500-6-5000-3-1000-4-3000-1-5-4000-2000-100-true-3-true-2",
         "green", "p"],
        ["Sluice",
          "stock_analysis-streamsluice-streamsluice-3990-30-1000-20-2-500-6-5000-3-1000-4-3000-1-5-4000-2000-100-true-3-true-1",
          "blue", "o"],
    ],
    "Tweet": [
        ["DS2",
         "tweet_alert-ds2-ds2-2190-30-1800-1-30-5000-10-1000-1-50-1-100-2000-100-true-3-true-1",
         "purple", "d"],
        ["StreamSwitch",
         "tweet_alert-streamswitch-streamswitch-2190-30-1800-1-30-5000-10-1000-1-50-1-100-2000-100-true-3-true-1",
         "green", "p"],
        ["Sluice",
          "tweet_alert-streamsluice-streamsluice-2190-30-1800-1-30-5000-10-1000-1-50-1-100-2000-100-true-3-true-2",
          "blue", "o"],
    ],
    "Linear_Road": [
        ["DS2",
         "linear_road-ds2-ds2-2190-30-1000-10-2-100-20-2000-4-100-70-1500-2000-100-true-3-true-2",
         "purple", "d"],
        ["StreamSwitch",
         "linear_road-streamswitch-streamswitch-2190-30-1000-10-2-100-20-2000-4-100-70-1500-2000-100-true-3-true-2",
         "green", "p"],
        ["Sluice",
          "linear_road-streamsluice-streamsluice-2190-30-1000-10-2-100-20-2000-4-100-70-1500-2000-100-true-3-true-3",
          "blue", "o"],
        # ["Sluice",
        #  "linear_road-streamsluice-streamsluice-2190-30-1000-10-2-100-20-2000-4-100-60-1500-2000-100-1-200-0.0-true-3-true-4",
        #  "orange", "o"],
    ]
}
import sys
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]

overall_latency = {}
for app in exps.keys():
    windowSize = 500
    latencyLimit = 2000 #1000
    startTime=30+300 #30
    isSingleOperator = False #True
    expName = [exp[1] for exp in exps[app] if exp[0] == "StreamSluice" or exp[0] == "Sluice"][0]
    print(expName)
    overall_latency[app] = {}
    draw(rawDir, outputDir + expName + "/", exps[app], windowSize)
#drawOverallLatency(outputDir + expName + "/", overall_latency)

