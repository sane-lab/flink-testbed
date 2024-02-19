import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

OPERATOR_NAMING = {
    "0a448493b4782967b150582570326227": "Stateful Map",
    "c21234bcbf1e8eb4c61f1927190efebd": "Splitter",
    "22359d48bcb33236cf1e31888091e54c": "Counter",
    "a84740bacf923e828852cc4966f2247c": "OP1",
    "eabd4c11f6c6fbdf011f0f1fc42097b1": "OP2",
    "d01047f852abd5702a0dabeedac99ff5": "OP3",
    "d2336f79a0d60b5a4b16c8769ec82e47": "OP4",
}


APP_NAMING = ["TF", "PA", "VA", "TA"]

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
        plt.plot(x, y, color=color, linewidth=LINEWIDTH)

def readUtilization(rawDir, expName):
    initialTime = -1
    lastTime = 0

    longestScalingTime = 0
    arrivalRatePerTask = {}
    serviceRatePerTask = {}
    backlogPerTask = {}
    parallelismsPerOperator = {}
    selectivity = {}


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
    #lastTime = initialTime + 240000
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
        lastScalingOperators = []
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if(len(split) >= 10 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale" and split[8] == "operator:"):
                time = int(split[3])
                #if (time > lastTime):
                #    continue
                if(split[7] == "in"):
                    type = 1
                elif(split[7] == "out"):
                    type = 2

                lastScalingOperators = [split[9].lstrip('[').rstrip(']')]
                for operator in lastScalingOperators:
                    if(operator not in scalingMarkerByOperator):
                        scalingMarkerByOperator[operator] = []
                    scalingMarkerByOperator[operator] += [[time - initialTime, type]]
            if(len(split) >= 8 and split[0] == "+++" and split[1] == "[CONTROL]" and split[4] == "all" and split[5] == "scaling" and split[6] == "plan" and split[7] == "deployed."):
                time = int(split[3])
                #if (time > lastTime):
                #    continue
                for operator in lastScalingOperators:
                    if(operator not in scalingMarkerByOperator):
                        scalingMarkerByOperator[operator] = []
                    scalingMarkerByOperator[operator] += [[time - initialTime, 3]]
                lastScalingOperators = []
            if(split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "backlog:"):
                time = int(split[3])
                #if (time > lastTime):
                #    continue
                backlogs = parsePerTaskValue(split[6:])
                operatorParallisms = {}
                for task in backlogs:
                    if task not in backlogPerTask:
                        backlogPerTask[task] = [[], []]
                    backlogPerTask[task][0] += [time - initialTime]
                    backlogPerTask[task][1] += [int(backlogs[task])]
                    operator = task.split('_')[0]
                    if operator not in operatorParallisms:
                        operatorParallisms[operator] = 0
                    operatorParallisms[operator] += 1
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
    #print(backlogPerTask)
    #print(arrivalRates)

    taskPerFig = [[]]

    # Sort task id on numerical order instead of alphabet order.
    orderedTasksByOperator = {}
    for task in backlogPerTask.keys():
        jobId, taskId = task.split("_")
        if(jobId not in orderedTasksByOperator):
            orderedTasksByOperator[jobId] = []
        orderedTasksByOperator[jobId].append(int(taskId))
    orderedTasks = []
    for jobId in orderedTasksByOperator.keys():
        for task in sorted(orderedTasksByOperator[jobId]):
            orderedTasks.append(jobId + "_" + str(task))

    for task in orderedTasks:
        if len(taskPerFig[-1]) >= MAXTASKPERFIG:
            taskPerFig += [[]]
        taskPerFig[-1] += [task]


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

    return [avgUtilizationPerJob, initialTime]

def draw(rawDir, outputDir, exps):
    avgUtilizationsPerJob = {}
    for expindex in range(0, len(exps)):
        expName = exps[expindex][1]
        result = readUtilization(rawDir, expName)
        avgUtilizationPerJob = result[0]
        for job in avgUtilizationPerJob.keys():
            if job not in avgUtilizationsPerJob:
                avgUtilizationsPerJob[job] = []
            avgUtilizationsPerJob[job] += [avgUtilizationPerJob[job]]
    print("Draw total figure...")
    figName = "avg_utilization"


    nJobs = len(avgUtilizationsPerJob.keys())
    jobList = ["a84740bacf923e828852cc4966f2247c", "eabd4c11f6c6fbdf011f0f1fc42097b1", "d01047f852abd5702a0dabeedac99ff5", "d2336f79a0d60b5a4b16c8769ec82e47"]
    #fig, axs = plt.subplots(nJobs, 1, figsize=(24, 5 * nJobs), layout='constrained')
    fig, axs = plt.subplots(nJobs, 1, figsize=(24, 2 * nJobs), layout='constrained')

    fig.supylabel("Utilization Per Operator")

    utilizationPerExp = {}
    for expindex in range(0, len(exps)):
        utilizationPerExp[exps[expindex][0]] = []
        for jobIndex in range(0, nJobs):
            job = jobList[jobIndex]
            utilizationInRange = [avgUtilizationsPerJob[job][expindex][x] for x in sorted((avgUtilizationsPerJob[job][expindex]).keys()) if x >= startTime * 1000 and x <= (startTime + 3600) * 1000]
            totalUtilization = sum(utilizationInRange)
            totalWindow = len(utilizationInRange)
            utilizationPerExp[exps[expindex][0]] += [totalUtilization / float(totalWindow)]
    print(utilizationPerExp)
    for jobIndex in range(0, nJobs):
        job = jobList[jobIndex]
        if nJobs == 1:
            ax1 = axs
        else:
            ax1 = axs[jobIndex]
        legend = ["50%"]
        limitx = [0, 100000000]
        limity = [0.5, 0.5]
        ax1.plot(limitx, limity, color='red', linewidth=1.5)
        for expindex in range(0, len(exps)):
            legend += [exps[expindex][0]]
            avgUtilization = avgUtilizationsPerJob[job][expindex]
            #print(avgUtilization)
            ax = sorted(avgUtilization.keys())
            ay = [avgUtilization[x] for x in ax]
            print("Draw " + exps[expindex][0] + " figure...")
            #plt.subplot(len(totalArrivalRatePerJob.keys()), 1, i+1)
            ax1.plot(ax, ay, exps[expindex][3], color=exps[expindex][2], markersize=MARKERSIZE)
        # ax1.set_ylabel("OP_" + str(jobIndex + 1) + ' Utilization')
        # ax1.set_ylim(0, 1.0)
        # ax1.set_yticks(np.arange(0, 1.2, 0.2))
        ax1.set_ylabel(APP_NAMING[jobIndex])
        ax1.set_ylim(0, 1.0)
        ax1.set_yticks(np.arange(0, 1.5, 0.5))
        ax1.set_xlim(startTime * 1000, (startTime + 3600) * 1000)
        ax1.set_xticks(np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000))
        if jobIndex == nJobs - 1:
            ax1.set_xticklabels([int((x - startTime * 1000) / 60000) for x in
                                 np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000)])
            ax1.set_xlabel("Time (minute)")
        else:
            ax1.set_xticklabels([])

        ax1.grid(True)
        if jobIndex == 0:
            ax1.legend(legend, bbox_to_anchor=(1, 1.6), loc='upper right', ncol=6, markerscale=4.)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    #plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.savefig(outputDir + figName + ".pdf", bbox_inches='tight')
    plt.close(fig)



rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
drawTaskFigureFlag = False
#expName = "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-true-1"
#expName = "streamsluice-twoOP-180-400-400-500-30-5-10-2-0.25-1500-500-10000-100-true-1"
exps = [
    #["StreamSluice", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-true-1", "blue", "o"],
    #["StreamSwitch", "stock-sb-4hr-50ms.txt-streamswitch-streamswitch-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-true-1", "green", "p"],
    #["Static-1", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-false-1", "gray", "*"],
    #["Static-2", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-2-1000-1-100-3-2000-1-100-8-5000-1-100-2000-100-false-1", "orange", "d"],
    #["Static-3", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-2000-20-2-1000-1-100-4-2000-1-100-10-5000-1-100-2000-100-false-1", "brown", "s"],
    # statesize=100
    # ["StreamSluice", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-100-3-2000-1-100-7-5000-1-100-2000-100-true-1", "blue", "o"],
    # ["StreamSwitch", "stock-sb-4hr-50ms.txt-streamswitch-streamswitch-3690-30-1000-20-2-1000-1-100-3-2000-1-100-7-5000-1-100-2000-100-true-1", "green", "p"],
    # ["DS2", "stock-sb-4hr-50ms.txt-ds2-ds2-3690-30-1000-20-2-1000-1-100-3-2000-1-100-7-5000-1-100-2000-100-true-1", "purple", "d"],
    # ["Static-1", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-2-1000-1-100-3-2000-1-100-7-5000-1-100-2000-100-false-1", "gray", "*"],
    #["Static-2", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-3-1000-1-100-5-2000-1-100-12-5000-1-100-2000-100-false-1", "orange", "s"],
    #["Static-3", "stock-sb-4hr-50ms.txt-streamsluice-streamsluice-3690-30-1000-20-3-1000-1-100-4-2000-1-100-9-5000-1-100-2000-100-false-1", "brown", "d"],

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

    # Cut first 5mins
    # ["Inadequate",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3990-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-false-1",
    #  "gray", "*"],
    # ["Adequate",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3990-30-1000-20-4-1000-1-500-6-2000-1-500-12-5000-1-500-1000-100-false-1",
    #  "orange", "*"],
    # ["DS2", "stock-server-split3-sb-4hr-50ms.txt-ds2-ds2-3990-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "purple", "d"],
    # ["StreamSwitch",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamswitch-streamswitch-3990-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "green", "p"],
    # ["Sluice",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3990-30-1000-20-2-1000-1-500-3-2000-1-500-6-5000-1-500-1000-100-true-1",
    #  "blue", "o"],

    # State10000
    ["Inadequate",
     "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3990-30-2000-20-2-1000-1-10000-3-2000-1-10000-6-5000-1-10000-1500-100-true-1",
     "gray", "*"],
    # ["Adequate",
    #  "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3990-30-1000-20-4-1000-1-500-6-2000-1-500-12-5000-1-500-1000-100-false-1",
    #  "orange", "*"],
    ["DS2",
     "stock-server-split3-sb-4hr-50ms.txt-ds2-ds2-3990-30-2000-20-2-1000-1-10000-3-2000-1-10000-6-5000-1-10000-1500-100-true-1",
     "purple", "d"],
    ["StreamSwitch",
     "stock-server-split3-sb-4hr-50ms.txt-streamswitch-streamswitch-3990-30-2000-20-2-1000-1-10000-3-2000-1-10000-6-5000-1-10000-1500-100-true-1",
     "green", "p"],
    ["Sluice",
     "stock-server-split3-sb-4hr-50ms.txt-streamsluice-streamsluice-3990-30-2000-20-2-1000-1-10000-3-2000-1-10000-6-5000-1-10000-1500-100-true-1",
     "blue", "o"],
]
startTime=30+300 #30
windowSize=1000
serviceRateFlag=True
scalingMarkerFlag = False
drawOperatorFigureFlag = False
expName = [exp[1] for exp in exps if exp[0] == "StreamSluice" or exp[0] == "Sluice"][0]
print(expName)
draw(rawDir, outputDir + expName + "/", exps)
