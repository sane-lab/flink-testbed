import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd


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
LINEWIDTH=3
width = 0.6

MAXTASKPERFIG=5

def parsePerKeyValue(splits):
    keyValuesPerOperator = {}
    operator = ''
    for split in splits:
        split = split.lstrip("{").rstrip("}").rstrip(",").rstrip("}")
        if "{" in split:
            split = split.split("=")
            operator = split[0]
            key = int(split[1].lstrip("{"))
            value = float(split[2])
            if operator not in keyValuesPerOperator:
                keyValuesPerOperator[operator] = {}
            keyValuesPerOperator[operator][key] = value
        else:
            split = split.split("=")
            key = int(split[0])
            value = float(split[1])
            keyValuesPerOperator[operator][key] = value
    return keyValuesPerOperator

def parsePerTaskValue(splits):
    taskValues = {}
    for split in splits:
        split = split.lstrip("{").rstrip("}").rstrip(",")
        words = split.split("=")
        taskName = words[0]
        value = float(words[1])
        taskValues[taskName] = value
    return taskValues

def getResults(rawDir, expName):
    initialTime = -1
    lastTime = 0
    arrivalRatePerTask = {}
    serviceRatePerTask = {}
    backlogPerTask = {}
    arrivalRatePerKey = {}
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
                latency = int(split[3].rstrip(","))
                arrivedTime = completedTime - latency
                if (initialTime == -1 or initialTime > arrivedTime):
                    initialTime = arrivedTime
                if (lastTime < completedTime):
                    lastTime = completedTime
    print(lastTime)
    # lastTime = initialTime + 240000

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
            if (split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "arrivalRate:"):
                time = int(split[3])
                if (time > lastTime):
                   continue
                arrivalRates = parsePerTaskValue(split[6:])
                for task in arrivalRates:
                    if task not in arrivalRatePerTask:
                        arrivalRatePerTask[task] = [[], []]
                    arrivalRatePerTask[task][0] += [time - initialTime]
                    arrivalRatePerTask[task][1] += [int(arrivalRates[task] * 1000)]
            if (split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "serviceRate:"):
                time = int(split[3])
                if (time > lastTime):
                    continue
                serviceRates = parsePerTaskValue(split[6:])
                for task in serviceRates:
                    if task not in serviceRatePerTask:
                        serviceRatePerTask[task] = [[], []]
                    serviceRatePerTask[task][0] += [time - initialTime]
                    serviceRatePerTask[task][1] += [int(serviceRates[task] * 1000)]
            if (split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "key" and split[5] == "arrivalRate:"):
                time = int(split[3])
                if (time > lastTime):
                    continue
                arrivalRates = parsePerKeyValue(split[6:])
                for operator in arrivalRates:
                    if operator not in arrivalRatePerKey:
                        arrivalRatePerKey[operator] = {}
                    for key in arrivalRates[operator]:
                        if key not in arrivalRatePerKey[operator]:
                            arrivalRatePerKey[operator][key] = [[], []]
                        arrivalRatePerKey[operator][key][0] += [time - initialTime]
                        arrivalRatePerKey[operator][key][1] += [int(arrivalRates[operator][key] * 1000)]

    averageTaskRateRatio = [[], []]
    maximumTaskRateRatio = [[], []]
    for task in arrivalRatePerTask:
        totalRatio = 0
        totalNum = 0
        maxRatio = 0
        for i in range(0, len(arrivalRatePerTask[task][0])):
            time = arrivalRatePerTask[task][0][i]
            arrivalRate = arrivalRatePerTask[task][1][i]
            if time != serviceRatePerTask[task][0][i]:
                print("Warning, time not matched: " + task + " " + i)
            serviceRate = serviceRatePerTask[task][1][i]
            if serviceRate <= 0.0:
                print("Warning, service rate is zero: " + task + " " + i)
            else:
                ratio = arrivalRate/serviceRate
                totalRatio += ratio
                totalNum += 1
                if ratio > maxRatio:
                    maxRatio = ratio
        averageTaskRateRatio[0] += [task]
        averageTaskRateRatio[1] += [totalRatio / totalNum]
        maximumTaskRateRatio[1] += [maxRatio - totalRatio / totalNum]

    averageKeyRate = {}
    maximumKeyRate = {}
    for operator in arrivalRatePerKey:
        averageKeyRate[operator] = [[], []]
        maximumKeyRate[operator] = [[], []]
        for key in arrivalRatePerKey[operator]:
            totalRate = 0
            totalNum = 0
            maxRate = 0
            for i in arrivalRatePerKey[operator][key][1]:
                totalRate += i
                totalNum += 1
                if i > maxRate:
                    maxRate = i
            averageKeyRate[operator][0] += [key]
            averageKeyRate[operator][1] += [totalRate/totalNum]
            maximumKeyRate[operator][1] += [maxRate - totalRate/totalNum]

    return [averageTaskRateRatio, maximumTaskRateRatio, averageKeyRate, maximumKeyRate]

def draw(rawDir, outputDir, expName, baseline):
    averageTaskRateRatio, maximumTaskRateRatio, averageKeyRate, maximumKeyRate = getResults(rawDir, expName)
    baseAverageTaskRateRatio, baseMaximumTaskRateRatio, baseAverageKeyRate, baseMaximumKeyRate = getResults(rawDir, baseline)

    print("Drawing experiment task rate")
    fig = plt.figure(figsize=(24, 18))
    legend = ["Average Rate"]
    p = plt.bar(averageTaskRateRatio[0], averageTaskRateRatio[1], width, label="Average")
    plt.bar_label(p, label_type='center', fmt='%.3f')
    legend += ["Maximum Rate"]
    p = plt.errorbar(averageTaskRateRatio[0], averageTaskRateRatio[1], yerr = maximumTaskRateRatio[1], fmt='o', markersize=MARKERSIZE, capsize=MARKERSIZE)
    plt.title('Task Arrival/Service Rate Ratio')
    plt.legend(legend, loc='upper left')
    axes = plt.gca()
    xlabels = []
    t_operator = ""
    axes.set_xticks(np.arange(0, len(averageTaskRateRatio[0]), 1))
    for x in averageTaskRateRatio[0]:
        operator, key = x.split('_')
        if operator != t_operator:
            t_operator = operator
            xlabels += [operator + '_' + key]
        else:
            xlabels += [key]
    axes.set_xticklabels(xlabels)
    axes.set_ylim(0, 2)
    axes.set_yticks(np.arange(0, 2, 0.2))
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + "task_rate_ratio.png")
    plt.close(fig)

    print("Drawing baseline task rate")
    fig = plt.figure(figsize=(24, 18))
    legend = ["Average Rate"]
    p = plt.bar(baseAverageTaskRateRatio[0], baseAverageTaskRateRatio[1], width, label="Average")
    plt.bar_label(p, label_type='center', fmt='%.3f')
    legend += ["Maximum Rate"]
    p = plt.errorbar(baseAverageTaskRateRatio[0], baseAverageTaskRateRatio[1], yerr=baseMaximumTaskRateRatio[1], fmt='o',
                     markersize=MARKERSIZE, capsize=MARKERSIZE)
    plt.title('Task Arrival/Service Rate Ratio')
    plt.legend(legend, loc='upper left')
    axes = plt.gca()
    xlabels = []
    t_operator = ""
    axes.set_xticks(np.arange(0, len(baseAverageTaskRateRatio[0]), 1))
    for x in baseAverageTaskRateRatio[0]:
        operator, key = x.split('_')
        if operator != t_operator:
            t_operator = operator
            xlabels += [operator + '_' + key]
        else:
            xlabels += [key]
    axes.set_xticklabels(xlabels)
    axes.set_ylim(0, 2)
    axes.set_yticks(np.arange(0, 2, 0.2))
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + "baseline_task_rate_ratio.png")
    plt.close(fig)

    print("Drawing experiment key arrival rate")
    fig = plt.figure(figsize=(24, 18))
    operators = list(averageKeyRate.keys())
    for i in range(0, len(operators)):
        plt.subplot(len(operators), i + 1, 1)
        legend = ["Average Rate"]
        operator = operators[i]
        p = plt.bar(averageKeyRate[operator][0], averageKeyRate[operator][1], width, label="Average")
        #plt.bar_label(p, label_type='center', fmt='%.3f')
        legend += ["Maximum Rate"]
        p = plt.errorbar(averageKeyRate[operator][0], averageKeyRate[operator][1], yerr=maximumKeyRate[operator][1], fmt='o',
                         markersize=MARKERSIZE, capsize=MARKERSIZE)
        axes = plt.gca()
        axes.set_ylim(0, 200)
        axes.set_yticks(np.arange(0, 200, 20))
    plt.title('Key Arrival')
    plt.legend(legend, loc='upper left')
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + "key_arrivalrate.png")
    plt.close(fig)

    print("Drawing baseline experiment key arrival rate")
    fig = plt.figure(figsize=(24, 18))
    operators = list(baseAverageKeyRate.keys())
    for i in range(0, len(operators)):
        plt.subplot(len(operators), i + 1, 1)
        legend = ["Average Rate"]
        operator = operators[i]
        p = plt.bar(baseAverageKeyRate[operator][0], baseAverageKeyRate[operator][1], width, label="Average")
        #plt.bar_label(p, label_type='center', fmt='%.3f')
        legend += ["Maximum Rate"]
        p = plt.errorbar(baseAverageKeyRate[operator][0], baseAverageKeyRate[operator][1], yerr=baseMaximumKeyRate[operator][1], fmt='o',
                         markersize=MARKERSIZE, capsize=MARKERSIZE)
        axes = plt.gca()
        axes.set_ylim(0, 100)
        axes.set_yticks(np.arange(0, 100, 10))
    plt.title('Key Arrival')
    plt.legend(legend, loc='upper left')
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + "baseline_key_arrivalrate.png")
    plt.close(fig)


rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
#expName = "streamsluice-scaletest-400-600-500-5-2000-1000-100-1"
expName = "streamsluice-twoOP-180-300-300-450-60-2-10-2-0.25-1000-500-10-100-true-1"
baselineName = "streamsluice-twoOP-180-300-300-450-60-2-10-2-0.25-1000-500-10-100-false-1"
draw(rawDir, outputDir + expName + "/", expName, baselineName)
