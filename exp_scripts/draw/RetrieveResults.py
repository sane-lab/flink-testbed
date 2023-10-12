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

    violatedNum = 0
    latencys = []
    for pair in groundTruthLatency:
        if(pair[0] >= initialTime + startTime * 1000 and pair[0] <= initialTime + endTime * 1000):
            if(pair[1] > latencyLimit):
                violatedNum += 1
            latencys += [pair[1]]
    percent = 1.0 - (violatedNum / float(len(latencys)))
    latencys.sort()
    latency99 = 0.0
    num99 = int(len(latencys) * 0.99)
    for i in range(0, num99):
        latency99 += latencys[i]
    latency99 /= num99

    # aggregatedGroundTruthLatency = {}
    # for pair in groundTruthLatency:
    #     index = int((pair[0] - initialTime) / windowSize)
    #     if index not in aggregatedGroundTruthLatency:
    #         aggregatedGroundTruthLatency[index] = [0, 0]
    #     aggregatedGroundTruthLatency[index][0] += pair[1]
    #     aggregatedGroundTruthLatency[index][1] += 1
    #
    # averageGroundTruthLatency = [[], []]
    # for index in sorted(aggregatedGroundTruthLatency):
    #     time = index * windowSize
    #     x = int(time)
    #     y = int(aggregatedGroundTruthLatency[index][0] / float(aggregatedGroundTruthLatency[index][1]))
    #     averageGroundTruthLatency[0] += [x]
    #     averageGroundTruthLatency[1] += [y]
    return [percent, latency99]

def readResourceAndSpike(rawDir, expName):
    initialTime = -1
    lastTime = 0
    ParallelismPerJob = {}
    scalingMarkerByOperator = {}
    scalingTimes = []
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
            if(split[0] == "GT:"):
                completedTime = int(split[2].rstrip(","))
                latency = int(split[3].rstrip(","))
                arrivedTime = completedTime - latency
                if (initialTime == -1 or initialTime > arrivedTime):
                    initialTime = arrivedTime
                if (lastTime < completedTime):
                    lastTime = completedTime
    print(lastTime)

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
            if(len(split) >= 10 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale" and split[8] == "operator:"):
                time = int(split[3])
                if(split[7] == "in"):
                    type = 1
                elif(split[7] == "out"):
                    type = 2

                lastScalingOperators = [split[9].lstrip('[').rstrip(']')]
                for operator in lastScalingOperators:
                    if (operator not in scalingMarkerByOperator):
                        scalingMarkerByOperator[operator] = []
                    scalingMarkerByOperator[operator] += [[time - initialTime, type]]
                mapping = parseMapping(split[12:])

            if(len(split) >= 8 and split[0] == "+++" and split[1] == "[CONTROL]" and split[4] == "all" and split[5] == "scaling" and split[6] == "plan" and split[7] == "deployed."):
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

            if(split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "backlog:"):
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
            if(split[0] == "+++" and split[1] == "[CONTROL]" and split[4] == "all" and split[5] == "scaling" and split[7] == "deployed."):
                scalingTimes += [int(split[10])]

    ParallelismPerJob["TOTAL"] = [[], []]
    for job in ParallelismPerJob:
        if job != "TOTAL":
            for i in range(0, len(ParallelismPerJob[job][0])):
                if i >= len(ParallelismPerJob["TOTAL"][0]):
                    ParallelismPerJob["TOTAL"][0].append(ParallelismPerJob[job][0][i])
                    ParallelismPerJob["TOTAL"][1].append(ParallelismPerJob[job][1][i])
                else:
                    ParallelismPerJob["TOTAL"][1][i] += ParallelismPerJob[job][1][i]
    maxParallelism = 0
    avgParallelism = 0.0
    job="TOTAL"
    if ParallelismPerJob[job][0][0] > startTime * 1000:
        avgParallelism += ParallelismPerJob[job][1][0] * (min(ParallelismPerJob[job][0][0], endTime) - startTime * 1000)
    for i in range(0, len(ParallelismPerJob[job][0])):
        x0 = ParallelismPerJob[job][0][i]
        y0 = ParallelismPerJob[job][1][i]
        if i + 1 >= len(ParallelismPerJob[job][0]):
            x1 = endTime * 1000
            y1 = y0
        else:
            x1 = ParallelismPerJob[job][0][i + 1]
            y1 = ParallelismPerJob[job][1][i + 1]
        if(x0 <= startTime * 1000):
            x0 = startTime * 1000
        if(x1 >= endTime * 1000):
            x1 = endTime * 1000
        avgParallelism += y0 * max(x1 - x0, 0)
        if (max(y0, y1) > maxParallelism):
            maxParallelism = max(y0, y1)
    avgParallelism /= (endTime - startTime) * 1000
    maxSpike = 0
    avgSpike = 0.0
    for spike in scalingTimes:
        if (spike > maxSpike):
            maxSpike = spike
        avgSpike += spike
    if(len(scalingTimes) > 0):
        avgSpike /= len(scalingTimes)
    return [avgParallelism, maxParallelism, maxSpike, avgSpike]

def retrieveResult(rawDir, expName, windowSize):
    latencyResult = readGroundTruthLatency(rawDir, expName, windowSize)
    resourceAndSpikeResult = readResourceAndSpike(rawDir, expName)
    return [latencyResult[0], latencyResult[1], resourceAndSpikeResult[0], resourceAndSpikeResult[1], resourceAndSpikeResult[2], resourceAndSpikeResult[3]]

rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
expName = "streamsluice-4op-300-8000-8000-10000-120-1-0-2-200-1-100-5-500-1-100-3-333-1-100-3-250-100-1000-600-100-true-2"
import sys
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]
windowSize = 1
#latencyLimit = 1000
latencyLimit = int(expName.split("-")[-5])
startTime = 30
endTime = 300
isSingleOperator = False #True
results = retrieveResult(rawDir, expName, windowSize)
print("\t".join(map(str, results)))
if len(sys.argv) > 1:
    f = open("./results.txt", "a")
    f.write(expName + "\t" + "\t".join(map(str, results)) + "\n")
    f.close()
