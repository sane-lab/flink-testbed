import math
import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

OPERATOR_NAMING = {
    "0a448493b4782967b150582570326227": "Stateful Map",
    "c21234bcbf1e8eb4c61f1927190efebd": "Splitter",
    "22359d48bcb33236cf1e31888091e54c": "Counter",
    "a84740bacf923e828852cc4966f2247c": "OP2",
    "eabd4c11f6c6fbdf011f0f1fc42097b1": "OP3",
    "d01047f852abd5702a0dabeedac99ff5": "OP4",
    "d2336f79a0d60b5a4b16c8769ec82e47": "OP5",
    "TOTAL": "TOTAL",
}
COLOR = {
    "TOTAL": "red",
    "Stateful Map": "red",
    "Splitter": "blue",
    "Counter": "green",
    "OP2": "blue",
    "OP3": "green",
    "OP4": "purple",
    "OP5": "orange",
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
LINEWIDTH=3

def addLatencyLimitMarker(plt, latencyLimits):
    #x = [0, 10000000]
    #y = [latencyLimit, latencyLimit]
    latencyLimits[0][0] = 0
    for i in range(0, len(latencyLimits[0])):
        x1 = 10000000
        if(i+1 < len(latencyLimits[0])):
            x1 = latencyLimits[0][i+1]
        x = [latencyLimits[0][i], x1]
        y = [latencyLimits[1][i], latencyLimits[1][i]]
        plt.plot(x, y, color='red', linewidth=1.5)

def parsePerTaskValue(splits):
    taskValues = {}
    for split in splits:
        split = split.lstrip("{").rstrip("}").rstrip(",")
        words = split.split("=")
        taskName = words[0]
        value = float(words[1])
        taskValues[taskName] = value
    return taskValues

def parseMapping(split):
    mapping = {}
    for word in split:
        word = word.lstrip("{").rstrip("}")
        if "=" in word:
            x = word.split("=")
            job = x[0].split("_")[0]
            task = x[0]
            key = x[1].lstrip("[").rstrip(",").rstrip("]")
            if job not in mapping:
                mapping[job] = {}
            mapping[job][task] = [key]
        else:
            key = word.rstrip(",").rstrip("]")
            mapping[job][task] += [key]
    return mapping


def readGroundTruthLatency(rawDir, expName, windowSize):
    initialTime = -1

    groundTruthLatencyPerTuple = {}
    groundTruthLatency = []
    latencyLimits = [[], []]

    ParallelismPerJob = {}
    scalingMarkerByOperator = {}

    arrivalRatePerTask = {}
    serviceRatePerTask = {}

    taskExecutors = []  # "flink-samza-taskexecutor-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("taskexecutor") == 1:
                taskExecutors += [file]
    for taskExecutor in taskExecutors:
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
            if (len(split) >= 7 and split[0] == "[AUTOTUNE]" and (split[4] == "decrease," or split[4] == "increase," or split[4] == "unchange,")):
                latencyLimits[0] += [int(split[2].rstrip('\n'))]
                latencyLimits[1] += [int(split[7].rstrip('\n'))]
            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale" and split[
                8] == "operator:"):
                time = int(split[3])
                if (split[7] == "in"):
                    type = 1
                elif (split[7] == "out"):
                    type = 2
                lastScalingOperators = [split[9].lstrip('[').rstrip(']')]
                for operator in lastScalingOperators:
                    if (operator not in scalingMarkerByOperator):
                        scalingMarkerByOperator[operator] = []
                    scalingMarkerByOperator[operator] += [[time - initialTime, type]]
                mapping = parseMapping(split[12:])
            if (len(split) >= 8 and split[0] == "+++" and split[1] == "[CONTROL]" and split[4] == "all" and split[
                5] == "scaling" and split[6] == "plan" and split[7] == "deployed."):
                time = int(split[3])
                # if (time > lastTime):
                #    continue
                for operator in lastScalingOperators:
                    if (operator not in scalingMarkerByOperator):
                        scalingMarkerByOperator[operator] = []
                    scalingMarkerByOperator[operator] += [[time - initialTime, 3]]
                lastScalingOperators = []
                for job in mapping:
                    ParallelismPerJob[job][0].append(time - initialTime)
                    ParallelismPerJob[job][1].append(len(mapping[job].keys()))
            if (split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "backlog:"):
                time = int(split[3])
                backlogs = parsePerTaskValue(split[6:])
                parallelism = {}
                for task in backlogs:
                    job = task.split("_")[0]
                    if job not in parallelism:
                        parallelism[job] = 0
                    parallelism[job] += 1
                for job in parallelism:
                    if job not in ParallelismPerJob:
                        ParallelismPerJob[job] = [[time - initialTime], [parallelism[job]]]
                        print(ParallelismPerJob)
            if(split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "arrivalRate:"):
                time = int(split[3])
                #if (time > lastTime):
                #   continue
                arrivalRates = parsePerTaskValue(split[6:])
                for task in arrivalRates:
                    if task not in arrivalRatePerTask:
                        arrivalRatePerTask[task] = [[], []]
                    import math
                    if not math.isnan(arrivalRates[task]) and not math.isinf(arrivalRates[task]):
                        arrivalRatePerTask[task][0] += [time - initialTime]
                        arrivalRatePerTask[task][1] += [int(arrivalRates[task] * 1000)]
            if (split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "serviceRate:"):
                time = int(split[3])
                #if (time > lastTime):
                #    continue
                serviceRates = parsePerTaskValue(split[6:])
                for task in serviceRates:
                    if task not in serviceRatePerTask:
                        serviceRatePerTask[task] = [[], []]
                    serviceRatePerTask[task][0] += [time - initialTime]
                    serviceRatePerTask[task][1] += [int(serviceRates[task] * 1000)]

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

    ParallelismPerJob["TOTAL"] = [[], []]
    for job in ParallelismPerJob:
        if job != "TOTAL":
            for i in range(0, len(ParallelismPerJob[job][0])):
                if i >= len(ParallelismPerJob["TOTAL"][0]):
                    ParallelismPerJob["TOTAL"][0].append(ParallelismPerJob[job][0][i])
                    ParallelismPerJob["TOTAL"][1].append(ParallelismPerJob[job][1][i])
                else:
                    ParallelismPerJob["TOTAL"][1][i] += ParallelismPerJob[job][1][i]

    totalArrivalRatePerJob = {}
    totalServiceRatePerJob = {}
    utilizationPerJob = {}
    parallelismPerJob = {}
    for task in arrivalRatePerTask:
        job = task.split("_")[0]
        n = len(arrivalRatePerTask[task][0])
        if job not in totalServiceRatePerJob:
            totalServiceRatePerJob[job] = {}
            totalArrivalRatePerJob[job] = {}
            utilizationPerJob[job] = {}
            parallelismPerJob[job] = {}
        for i in range(0, n):
            ax = arrivalRatePerTask[task][0][i]
            index = math.floor(ax / windowSize) * windowSize
            ay = arrivalRatePerTask[task][1][i]
            sx = serviceRatePerTask[task][0][i]
            sy = serviceRatePerTask[task][1][i]
            if index not in totalArrivalRatePerJob[job]:
                totalArrivalRatePerJob[job][index] = ay
            else:
                totalArrivalRatePerJob[job][index] += ay
            if sx not in totalServiceRatePerJob[job]:
                totalServiceRatePerJob[job][sx] = sy
            else:
                totalServiceRatePerJob[job][sx] += sy

            utilization = 1
            if sy > 0:
                utilization = min(ay / sy, 1.0)
            if index not in utilizationPerJob[job]:
                utilizationPerJob[job][index] = utilization
                parallelismPerJob[job][index] = 1
            else:
                utilizationPerJob[job][index] += utilization
                parallelismPerJob[job][index] += 1
    avgUtilizationPerJob = {}
    for job in utilizationPerJob.keys():
        avgUtilizationPerJob[job] = {}
        for index in utilizationPerJob[job].keys():
            avgUtilizationPerJob[job][index] = utilizationPerJob[job][index] / parallelismPerJob[job][index]

    return [averageGroundTruthLatency, initialTime, latencyLimits, ParallelismPerJob, avgUtilizationPerJob, totalArrivalRatePerJob]


def drawLatency(outputDir, averageGroundTruthLatency, latencyLimits):
    # Draw Latency
    fig = plt.figure(figsize=(24, 10))
    print("Draw ground truth curve...")
    legend = ["Ground Truth"]
    plt.plot(averageGroundTruthLatency[0], averageGroundTruthLatency[1], 'o', color='b', markersize=2)
    legend += ["Limit"]
    addLatencyLimitMarker(plt, latencyLimits)
    plt.plot()
    plt.legend(legend, loc='upper left')
    plt.xlabel('Time (min)')
    plt.ylabel('Ground Truth Latency (ms)')
    #plt.title('Latency Curves')
    axes = plt.gca()
    axes.set_xlim(startTime * 1000, (startTime + 3660) * 1000)
    axes.set_xticks(np.arange(startTime * 1000, (startTime + 3660) * 1000 + 300000, 300000))
    axes.set_xticklabels([int((x - startTime * 1000) / 60000) for x in np.arange(startTime * 1000, (startTime + 3660) * 1000 + 300000, 300000)])
    axes.set_ylim(0, 4000)
    axes.set_yticks(np.arange(0, 4500, 500))
    # axes.set_yscale('log')
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    #plt.savefig(outputDir + 'ground_truth_latency_curves.png')
    plt.savefig(outputDir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.close(fig)

def drawUtilization(outputDir, avgUtilizationPerJob):


    print("Draw total figure...")
    figName = "avg_utilization"
    legend = []
    fig, axs = plt.subplots(len(avgUtilizationPerJob), 1, figsize=(24, 5), layout='constrained')
    for jobIndex in range(0, len(avgUtilizationPerJob)):
        job = jobList[jobIndex]
        limitx = [0, 100000000]
        limity = [0.5, 0.5]
        legend = ["50%"]
        if (len(avgUtilizationPerJob) == 1):
            ax1 = axs
        else:
            ax1 = axs[jobIndex]
        ax1.plot(limitx, limity, color='red', linewidth=1.5)
        legend += ["Utilization"]
        avgUtilization = avgUtilizationPerJob[job]
        # print(avgUtilization)
        ax = sorted(avgUtilization.keys())
        ay = [avgUtilization[x] for x in ax]
        print("Draw " + job + " utilization...")
        # plt.subplot(len(totalArrivalRatePerJob.keys()), 1, i+1)
        ax1.plot(ax, ay, "o", color="blue", markersize=MARKERSIZE)
        ax1.set_ylabel('Utilization (ratio)')
        ax1.set_ylim(0, 1.0)
        ax1.set_yticks(np.arange(0, 1.2, 0.2))

        ax1.set_xlim(startTime * 1000, (startTime + 3600) * 1000)
        ax1.set_xticks(np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000))
        ax1.set_xticklabels([int((x - startTime * 1000) / 60000) for x in
                             np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000)])
        ax1.grid(True)
    plt.legend(legend, loc='upper left')
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    # plt.savefig(outputDir + figName + ".png")
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)

def drawParallelism(outputDir, ParallelismPerJob):
    figName = "Parallelism"

    fig = plt.figure(figsize=(12, 4))

    legend = []
    for job in ParallelismPerJob:
        print("Draw Job " + job + " curve...")
        legend += [OPERATOR_NAMING[job]]
        line = [[], []]
        for i in range(0, len(ParallelismPerJob[job][0])):
            x0 = ParallelismPerJob[job][0][i]
            y0 = ParallelismPerJob[job][1][i]
            if i + 1 >= len(ParallelismPerJob[job][0]):
                x1 = 10000000
                y1 = y0
            else:
                x1 = ParallelismPerJob[job][0][i + 1]
                y1 = ParallelismPerJob[job][1][i + 1]
            line[0].append(x0)
            line[0].append(x1)
            line[1].append(y0)
            line[1].append(y0)
            line[0].append(x1)
            line[0].append(x1)
            line[1].append(y0)
            line[1].append(y1)
        plt.plot(line[0], line[1], color=COLOR[OPERATOR_NAMING[job]], linewidth=LINEWIDTH)
    plt.legend(legend, loc='upper right', ncol=5)
    # for operator in scalingMarkerByOperator:
    #    addScalingMarker(plt, scalingMarkerByOperator[operator])
    # plt.xlabel('Time (s)')
    plt.ylabel('# of tasks')
    # plt.title('Parallelism of Operators')
    axes = plt.gca()
    # axes.set_xlim(0, lastTime-initialTime)
    # axes.set_xticks(np.arange(0, lastTime-initialTime, 30000))
    #
    # xlabels = []
    # for x in range(0, lastTime-initialTime, 30000):
    #     xlabels += [str(int(x / 1000))]
    # axes.set_xticklabels(xlabels)
    axes.set_xlim(startTime * 1000, (startTime + 3600) * 1000)
    axes.set_xticks(np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000))
    axes.set_xticklabels([int((x - startTime * 1000) / 60000) for x in
                          np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000)])

    axes.set_ylim(0, 65)
    axes.set_yticks(np.arange(0, 65, 5))
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    # plt.savefig(outputDir + figName + ".png")
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)

def draw(rawDir, outputDir, expName, windowSize):
    result = readGroundTruthLatency(rawDir, expName, windowSize)
    averageGroundTruthLatency = result[0]
    latencyLimits = result[2]
    parallelismPerJob = result[3]
    avgUtilizationPerJob = result[4]
    totalArrivalRatePerJob = result[5]

    # Find limit
    limitTriedTimes = {}
    limitFailTimes = {}
    print(latencyLimits)
    for i in range(0, len(latencyLimits[1])):
        limit = latencyLimits[1][i]
        if limit not in limitTriedTimes:
            limitTriedTimes[limit] = 1
            limitFailTimes[limit] = 0
        else:
            limitTriedTimes[limit] += 1
        if i < len(latencyLimits[1]) - 1 and limit < latencyLimits[1][i + 1]:
            limitFailTimes[limit] += 1
    possibleLimits = [limit for limit in limitTriedTimes.keys() if limitTriedTimes[limit] > 1 and limitFailTimes[limit] == 0]
    bestLimit = -1
    if len(possibleLimits) > 0:
        bestLimit = min(possibleLimits)
    print("Possible limits: " + str(possibleLimits))
    # Find utilization under best limit
    bestLimitPeriods = []
    for i in range(len(latencyLimits[0])):
        x1 = 10000000
        if (i + 1 < len(latencyLimits[0])):
            x1 = latencyLimits[0][i + 1]
        if latencyLimits[1][i] == bestLimit:
            bestLimitPeriods += [[latencyLimits[0][i], x1]]
    totalUtilizationUnderBestLimitPerJob = {}
    totalTimeUnderBestLimitPerJob = {}
    for job in avgUtilizationPerJob.keys():
        totalUtilizationUnderBestLimitPerJob[job] = 0
        totalTimeUnderBestLimitPerJob[job] = 0
        for index in avgUtilizationPerJob[job]:
            if any(bestLimitPeriod[0] <= index and bestLimitPeriod[1] for bestLimitPeriod in bestLimitPeriods):
                totalTimeUnderBestLimitPerJob[job] += 1
                totalUtilizationUnderBestLimitPerJob[job] += avgUtilizationPerJob[job][index]
    avgUtilizationUnderBestLimitPerJob = {}
    for job in totalUtilizationUnderBestLimitPerJob.keys():
        if(totalTimeUnderBestLimitPerJob[job] == 0):
            avgUtilizationUnderBestLimitPerJob[job] = -1
        else:
            avgUtilizationUnderBestLimitPerJob[job] = totalUtilizationUnderBestLimitPerJob[job] / totalTimeUnderBestLimitPerJob[job]
    print("Average utilization: " + str(avgUtilizationUnderBestLimitPerJob))
    f = open("../workload_result.txt", "a")
    f.write(expName + "\n")
    outputLine = str(bestLimit)
    for jobIndex in range(0, len(avgUtilizationUnderBestLimitPerJob)):
        job = jobList[jobIndex]
        outputLine += " " + str(avgUtilizationUnderBestLimitPerJob[job])
    f.write(outputLine + "\n")
    f.close()
    drawLatency(outputDir, averageGroundTruthLatency, latencyLimits)

    drawUtilization(outputDir, avgUtilizationPerJob)

    drawParallelism(outputDir, parallelismPerJob)



rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
#expName = "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-true-1"
expName = "microbench-workload-2op-3660-10000-10000-10000-5000-120-1-0-1-50-1-40000-12-1000-1-40000-4-357-1-10000-2000-500-100-true-1"
jobList = ["a84740bacf923e828852cc4966f2247c", "eabd4c11f6c6fbdf011f0f1fc42097b1", "d01047f852abd5702a0dabeedac99ff5", "d2336f79a0d60b5a4b16c8769ec82e47"]
import sys
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]
windowSize = 1000
latencyLimit = 2000
startTime = 0
isSingleOperator = False #True
with open("../workload_list.txt") as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split()
        expName = split[0]
        if(split[0].startswith("microbench")):
            draw(rawDir, outputDir + expName + "/", expName, windowSize)
