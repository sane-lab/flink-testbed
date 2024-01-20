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

MAXTASKPERFIG=5

def parsePerTaskValue(splits):
    taskValues = {}
    for split in splits:
        split = split.lstrip("{").rstrip("}").rstrip(",")
        words = split.split("=")
        taskName = words[0]
        value = float(words[1])
        taskValues[taskName] = value
    return taskValues

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
        plt.plot(x, y, color=color, linewidth=LINEWIDTH/2.0)

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

def readParallelism(rawDir, expName):
    initialTime = -1
    lastTime = 0
    arrivalRatePerTask = {}
    ParallelismPerJob = {}
    scalingMarkerByOperator = {}

    taskExecutors = [] #"flink-samza-taskexecutor-0-eagle-sane.out"
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

            if (split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "arrivalRate:"):
                time = int(split[3])
                # if (time > lastTime):
                #   continue
                arrivalRates = parsePerTaskValue(split[6:])
                for task in arrivalRates:
                    if task not in arrivalRatePerTask:
                        arrivalRatePerTask[task] = [[], []]
                    import math
                    if not math.isnan(arrivalRates[task]) and not math.isinf(arrivalRates[task]):
                        arrivalRatePerTask[task][0] += [time - initialTime]
                        arrivalRatePerTask[task][1] += [int(arrivalRates[task] * 1000)]

    ParallelismPerJob["TOTAL"] = [[], []]
    for job in ParallelismPerJob:
        if job != "TOTAL":
            for i in range(0, len(ParallelismPerJob[job][0])):
                if i >= len(ParallelismPerJob["TOTAL"][0]):
                    if weightedTotalParallelismFlag:
                        ParallelismPerJob["TOTAL"][0].append(
                            parallelismWeight[OPERATOR_NAMING[job]] * ParallelismPerJob[job][0][i])
                        ParallelismPerJob["TOTAL"][1].append(
                            parallelismWeight[OPERATOR_NAMING[job]] * ParallelismPerJob[job][1][i])
                    else:
                        ParallelismPerJob["TOTAL"][0].append(ParallelismPerJob[job][0][i])
                        ParallelismPerJob["TOTAL"][1].append(ParallelismPerJob[job][1][i])
                else:
                    if weightedTotalParallelismFlag:
                        ParallelismPerJob["TOTAL"][1][i] += parallelismWeight[OPERATOR_NAMING[job]] * \
                                                            ParallelismPerJob[job][1][i]
                    else:
                        ParallelismPerJob["TOTAL"][1][i] += ParallelismPerJob[job][1][i]
    print(ParallelismPerJob)

    totalArrivalRatePerJob = {}
    for task in arrivalRatePerTask:
        job = task.split("_")[0]
        n = len(arrivalRatePerTask[task][0])
        if job not in totalArrivalRatePerJob:
            totalArrivalRatePerJob[job] = {}
        for i in range(0, n):
            ax = arrivalRatePerTask[task][0][i]
            index = math.floor(ax / windowSize) * windowSize
            ay = arrivalRatePerTask[task][1][i]
            if index not in totalArrivalRatePerJob[job]:
                totalArrivalRatePerJob[job][index] = ay
            else:
                totalArrivalRatePerJob[job][index] += ay
    print(expName, ParallelismPerJob.keys())
    return [ParallelismPerJob, totalArrivalRatePerJob, initialTime]

def draw(rawDir, outputDir, exps):
    parallelismsPerJob = {}
    totalArrivalRatesPerJob = {}
    for expindex in range(0, len(exps)):
        expFile = exps[expindex][1]
        result = readParallelism(rawDir, expFile)
        parallelisms = result[0]
        totalArrivalRates = result[1]
        for job in parallelisms.keys():
            if job == "TOTAL":
                continue
            if job not in parallelismsPerJob:
                parallelismsPerJob[job] = []
                totalArrivalRatesPerJob[job] = []
            parallelismsPerJob[job] += [parallelisms[job]]
            totalArrivalRatesPerJob[job] += [totalArrivalRates[job]]
    print("Draw total figure...")

    figName = "Parallelism"
    nJobs = len(parallelismsPerJob.keys())
    jobList = ["a84740bacf923e828852cc4966f2247c", "eabd4c11f6c6fbdf011f0f1fc42097b1", "d01047f852abd5702a0dabeedac99ff5", "d2336f79a0d60b5a4b16c8769ec82e47"]
    fig, axs = plt.subplots(nJobs, 1, figsize=(24, 5 * nJobs), layout='constrained') #plt.figure(figsize=(12, 4))
    for jobIndex in range(0, nJobs):
        job = jobList[jobIndex]
        if nJobs == 1:
            ax1 = axs
        else:
            ax1 = axs[jobIndex]
        ax2 = ax1.twinx()
        legend = []
        for expindex in range(0, len(exps)):
            print("Draw exps " + exps[expindex][0] + " curve...")
            print(expindex, job, len(parallelismsPerJob[job]))  #parallelismsPerJob[job])
            Parallelism = parallelismsPerJob[job][expindex]

            legend += [exps[expindex][0]]
            line = [[], []]
            for i in range(0, len(Parallelism[0])):
                x0 = Parallelism[0][i]
                y0 = Parallelism[1][i]
                if i+1 >= len(Parallelism[0]):
                    x1 = 10000000
                    y1 = y0
                else:
                    x1 = Parallelism[0][i+1]
                    y1 = Parallelism[1][i+1]
                line[0].append(x0)
                line[0].append(x1)
                line[1].append(y0)
                line[1].append(y0)
                line[0].append(x1)
                line[0].append(x1)
                line[1].append(y0)
                line[1].append(y1)
            ax1.plot(line[0], line[1], color=exps[expindex][2], linewidth=LINEWIDTH)
        if jobIndex == 0:
            ax1.legend(legend, loc='upper right', ncol=5)
        #for operator in scalingMarkerByOperator:
        #    addScalingMarker(plt, scalingMarkerByOperator[operator])
        #plt.xlabel('Time (s)')
        ax1.set_ylabel('OP_'+str(jobIndex+1)+' Parallelism')
        #plt.title('Parallelism of Operators')
        #axes = plt.gca()
        # axes.set_xlim(0, lastTime-initialTime)
        # axes.set_xticks(np.arange(0, lastTime-initialTime, 30000))
        #
        # xlabels = []
        # for x in range(0, lastTime-initialTime, 30000):
        #     xlabels += [str(int(x / 1000))]
        # axes.set_xticklabels(xlabels)
        ax1.set_xlim(startTime * 1000, (startTime + 3600) * 1000)
        ax1.set_xticks(np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000))
        ax1.set_xticklabels([int((x - startTime * 1000) / 60000) for x in np.arange(startTime * 1000,(startTime + 3600) * 1000 + 300000, 300000)])

        if jobIndex < 2:
            ax1.set_ylim(0, 10)
            ax1.set_yticks(np.arange(0, 12, 2))
        elif jobIndex == 2:
            ax1.set_ylim(0, 20)
            ax1.set_yticks(np.arange(0, 24, 4))
        else:
            ax1.set_ylim(0, 15)
            ax1.set_yticks(np.arange(0, 18, 3))
        ax1.grid(True)


        ax = sorted(totalArrivalRatesPerJob[job][0].keys())
        #print(totalArrivalRatePerJob[job])
        ay = [totalArrivalRatesPerJob[job][0][x] / (windowSize / 100) for x in ax]
        ax2.plot(ax, ay, 'o-', color='red', markersize=MARKERSIZE/2, label="Arrival Rate")
        ax2.set_ylabel('Rate (tps)')
        if jobIndex == 0:
            ax2.set_ylim(0, 10000)
            ax2.set_yticks(np.arange(0, 12000, 2000))
        elif jobIndex == 1:
            ax2.set_ylim(0, 5000)
            ax2.set_yticks(np.arange(0, 6000, 1000))
        elif jobIndex == 2:
            ax2.set_ylim(0, 4000)
            ax2.set_yticks(np.arange(0, 4800, 800))
        else:
            ax2.set_ylim(0, 4500)
            ax2.set_yticks(np.arange(0, 5400, 900))
        legend = ["OP_" + str(jobIndex + 1) +"Arrival Rate"]
        ax2.set_xlim(startTime * 1000, (startTime + 3600) * 1000)
        ax2.set_xticks(np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000))
        ax2.set_xticklabels([int((x - startTime * 1000) / 60000) for x in
                             np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000)])
        if jobIndex == 0:
            ax2.legend(legend, loc='upper left', ncol=1)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    #plt.savefig(outputDir + figName + ".png")
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)


rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
#expName = "streamsluice-scaletest-400-600-500-5-2000-1000-100-1"
#expName = "autotune_4op-false-390-10000-12500-60-15000-60-12500-60-1-0-2-125-1-5000-2-120-1-5000-3-250-1-5000-6-500-5000-2000-1500-100-true-1"
exps = [
    #["StreamSluice", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-true-1", "blue"],
    #["StreamSwitch", "stock-sb-4hr-50ms.txt-streamswitch-streamswitch-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-true-1", "green"],
    #["Static-1", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-false-1", "gray"],
    #["Static-2", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-2-1000-1-100-3-2000-1-100-8-5000-1-100-2000-100-false-1", "orange"],
    #["Static-3", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-2-1000-1-100-4-2000-1-100-10-5000-1-100-2000-100-false-1", "brown"],

    # statesize=100
    # ["StreamSluice","stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-100-3-2000-1-100-7-5000-1-100-2000-100-true-1","blue"],
    # ["StreamSwitch","stock-sb-4hr-50ms.txt-streamswitch-streamswitch-3690-30-1000-20-2-1000-1-100-3-2000-1-100-7-5000-1-100-2000-100-true-1","green"],
    # ["DS2", "stock-sb-4hr-50ms.txt-ds2-ds2-3690-30-1000-20-2-1000-1-100-3-2000-1-100-7-5000-1-100-2000-100-true-1", "purple"],
    # ["Static-1", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-100-3-2000-1-100-7-5000-1-100-2000-100-false-1", "gray"],
    #["Static-2","stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-false-1","orange"],
    #["Static-3","stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-3-1000-1-100-4-2000-1-100-9-5000-1-100-2000-100-false-1","brown"],

    # statesize=2000
    # ["Static-1",
    #  "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-false-1",
    #  "gray", "*"],
    # ["Static-2",
    #  "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-4-1000-1-500-6-2000-1-500-12-5000-1-500-1000-100-false-1",
    #  "orange", "*"],
    # ["DS2", "stock-sb-4hr-50ms.txt-ds2-ds2-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "purple", "d"],
    # ["StreamSwitch",
    #  "stock-sb-4hr-50ms.txt-streamswitch-streamswitch-3690-30-1000-20-3-1000-1-500-4-2000-1-500-9-5000-1-500-1000-100-true-1",
    #  "green", "p"],
    # ["StreamSluice",
    #  "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "blue", "o"],

    # # Split and join
    # ["Static-1",
    #  "stock-split3hBsb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-false-1",
    #  "gray", "*"],
    # ["Static-2",
    #  "stock-split3hBsb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-4-1000-1-500-6-2000-1-500-12-5000-1-500-1000-100-false-1",
    #  "orange", "*"],
    # ["DS2", "stock-split3-sb-4hr-50ms.txt-ds2-ds2-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "purple", "d"],
    # ["StreamSwitch",
    #  "stock-split3-sb-4hr-50ms.txt-streamswitch-streamswitch-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "green", "p"],
    # ["StreamSluice",
    #  "stock-split3hBsb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "blue", "o"],

    # Cluster
    # ["Static-1",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-false-1",
    #  "gray", "*"],
    # ["Static-2",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-4-1000-1-500-6-2000-1-500-12-5000-1-500-1000-100-false-1",
    #  "orange", "*"],
    # ["DS2", "stock-server-split3-sb-4hr-50ms.txt-ds2-ds2-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "purple", "d"],
    # ["StreamSwitch",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamswitch-streamswitch-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "green", "p"],
    #  ["StreamSluice",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "blue", "o"],

    # Change rate
    ["Static-1",
     "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3990-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-false-1",
     "gray", "*"],
    # ["Static-2",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3990-30-1000-20-4-1000-1-500-6-2000-1-500-12-5000-1-500-1000-100-false-1",
    #  "orange", "*"],
    ["DS2", "stock-server-split3-sb-4hr-50ms.txt-ds2-ds2-3990-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
     "purple", "d"],
    ["StreamSwitch",
     "stock-server-split3-sb-4hr-50ms.txt-streamswitch-streamswitch-3990-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
     "green", "p"],
    ["StreamSluice",
     "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3990-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
     "blue", "o"],
]
windowSize=1000
startTime=30+300 #30
perOperatorFlag = False
weightedTotalParallelismFlag = False
parallelismWeight = {
    "OP2": 10,
    "OP3": 5,
    "OP4" : 2,
    "OP5" : 3,
}
arrivalRateFlag = True
import sys
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]
expName = [exp[1] for exp in exps if exp[0] == "StreamSluice"][0]
print(expName)
draw(rawDir, outputDir + expName + "/", exps)

