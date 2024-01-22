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
    "a84740bacf923e828852cc4966f2247c": "OP1",
    "eabd4c11f6c6fbdf011f0f1fc42097b1": "OP2",
    "d01047f852abd5702a0dabeedac99ff5": "OP3",
    "d2336f79a0d60b5a4b16c8769ec82e47": "OP4",
}

EXP_COLOR = {
    "Baseline" : "gray",
    "StreamSluice" : "blue",
    "Early": "purple",
    "Late": "orange",
    "Not_Enough": "orange",
    "Minus_One": "purple",
    "Too_Much": "green",
    "No_Balance": "olive",
    "Shuffle_Keys": "brown",
    "Not_Bottleneck": "orange",
}

EXP_MARKER = {
    "Baseline" : "-",
    "StreamSluice" : "-",
    "Early": "-",
    "Late": "-",
    "Not_Enough": "-",
    "Minus_One": "-",
    "Too_Much": "-",
    "No_Balance": "-",
    "Shuffle_Keys": "-",
    "Not_Bottleneck": "-",
}

SCALE_MARKER = {
    "Baseline" : "o-",
    "StreamSluice" : "o",
    "Early": "*",
    "Late": "v",
    "Not_Enough": "*",
    "Minus_One": "d",
    "Too_Much": "v",
    "No_Balance": "*",
    "Shuffle_Keys": "s",
    "Not_Bottleneck": "X",
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
MARKERSIZE=6

def addLatencyLimitMarker(plt):
    x = [0, 10000000]
    y = [latencyLimit, latencyLimit]
    plt.plot(x, y, color='red', linewidth=1.5)
def addScalingMarker(exp, plt, scalingMarkerByOperator, curve):
    for operator in scalingMarkerByOperator:
        for scaling in scalingMarkerByOperator[operator]:
            time = scaling[0]
            type = scaling[1]
            if type == 2:
                color = EXP_COLOR[exp]
                marker = 'D' #SCALE_MARKER[exp]
            #elif type == 1:
            #    color = "green"
            #else:
            #    color = "gray"
                index = 0
                for i in range(0, len(curve[0]) - 1):
                    if time >= curve[0][i] and time < curve[0][i + 1]:
                        index = i
                        break
                if index == len(curve[0]) - 1:
                    x = [curve[0][index]]
                    y = [curve[1][index]]
                else:
                    x = time
                    y = ((time - curve[0][index]) / float(curve[0][index + 1] - curve[0][index])) * (curve[1][index + 1] - curve[1][index]) + curve[1][index]

                plt.plot(x, y, color=color, marker=marker, markersize=9)

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
    scalingMarkerByOperator = {}

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
    ParallelismPerJob = {}
    windowArrivalRate = {}
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
            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale" and split[
                8] == "operator:"):
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
            if (split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "arrivalRate:"):
                time = int(split[3])
                # if (time > lastTime):
                #   continue
                arrivalRates = parsePerTaskValue(split[6:])
                currentArrivalRate = 0
                for task in arrivalRates:
                    if task.startswith("a84740bacf923e828852cc4966f2247c"):
                        currentArrivalRate += int(arrivalRates[task] * 1000)
                #totalArrivalRate[0] += [time - initialTime]
                if (time - initialTime) not in windowArrivalRate:
                    windowArrivalRate[time - initialTime] = 0
                windowArrivalRate[time - initialTime] += currentArrivalRate
            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale" and split[8] == "operator:"):
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

            if (len(split) >= 8 and split[0] == "+++" and split[1] == "[CONTROL]" and split[4] == "all" and split[5] == "scaling" and split[6] == "plan" and split[7] == "deployed."):
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
    ParallelismPerJob["TOTAL"] = [[], []]
    for job in ParallelismPerJob:
        if job != "TOTAL":
            for i in range(0, len(ParallelismPerJob[job][0])):
                if i >= len(ParallelismPerJob["TOTAL"][0]):
                    ParallelismPerJob["TOTAL"][0].append(ParallelismPerJob[job][0][i])
                    ParallelismPerJob["TOTAL"][1].append(ParallelismPerJob[job][1][i])
                else:
                    ParallelismPerJob["TOTAL"][1][i] += ParallelismPerJob[job][1][i]

    aggregatedGroundTruthLatency = {}
    for pair in groundTruthLatency:
        index = int((pair[0] - initialTime) / windowSize)
        if index not in aggregatedGroundTruthLatency:
            aggregatedGroundTruthLatency[index] = []
        aggregatedGroundTruthLatency[index] += [pair[1]]

    aggregatedArrivalRate = {}
    for time in windowArrivalRate.keys():
        index = int((time) / 100) #windowSize)
        if index not in windowArrivalRate:
            aggregatedArrivalRate[index] = [0, 0]
        aggregatedArrivalRate[index][0] += 1
        aggregatedArrivalRate[index][1] += windowArrivalRate[time]

    averageGroundTruthLatency = [[], []]
    for index in sorted(aggregatedGroundTruthLatency):
        time = index * windowSize
        x = int(time)
        sortedLatency = sorted(aggregatedGroundTruthLatency[index])
        size = len(sortedLatency)
        target = min(math.ceil(size * 0.99), size) - 1
        y = sortedLatency[target]
        averageGroundTruthLatency[0] += [x]
        averageGroundTruthLatency[1] += [y]
    totalArrivalRate = [[], []]
    for index in sorted(aggregatedArrivalRate):
        time = index * 100 #windowSize
        x = int(time)
        y = aggregatedArrivalRate[index][1]/aggregatedArrivalRate[index][0]
        totalArrivalRate[0] += [x]
        totalArrivalRate[1] += [y]
   # print(totalArrivalRate)

    return [averageGroundTruthLatency, scalingMarkerByOperator, totalArrivalRate, ParallelismPerJob["TOTAL"]]

def retrieveResults(rawDir, exps, windowSize):
    groundTruthLatencys = []
    scaleMarkers = []
    totalArrivalRate = []
    totalParallelism = []
    for i in range(0, len(exps)):
        controller = exps[i][0]
        print("Read ground truth for " + controller)
        expName = exps[i][1]
        result = readGroundTruthLatency(rawDir, expName, windowSize)
        groundTruthLatencys += [result[0]]
        scaleMarkers += [result[1]]
        totalParallelism += [result[3]]
        if controller == "Baseline":
            totalArrivalRate = result[2]
    return [groundTruthLatencys, scaleMarkers, totalArrivalRate, totalParallelism]

def draw(rawDir, outputDir, exps, windowSize, figType):
    groundTruthLatencys, scaleMarkers, totalArrivalRate, totalParallelism = retrieveResults(rawDir, exps, windowSize)

    fig = plt.figure(figsize=(12, 9))
    print("Draw ground truth curve...")
    legend = []
    for i in range(0, len(exps)):
        controller = exps[i][0]
        print("Draw ground truth for " + controller)
        legend += [controller]
        plt.plot(groundTruthLatencys[i][0], groundTruthLatencys[i][1], EXP_MARKER[controller], color=EXP_COLOR[controller], markersize=MARKERSIZE)

    for i in range(0, len(exps)):
        controller = exps[i][0]
        addScalingMarker(controller, plt, scaleMarkers[i], groundTruthLatencys[i])
    addLatencyLimitMarker(plt)
    plt.plot()
    plt.legend(legend) #,loc='upper left')
    plt.xlabel('Time (s)')
    plt.ylabel('Latency (ms)')
    plt.title('Latency Curves')
    axes = plt.gca()
    axes.set_xlim(starTime * 1000, endTime * 1000)
    axes.set_xticks(np.arange(starTime * 1000, endTime * 1000 + 5000, 5000))
    axes.set_xticklabels([int((x - starTime * 1000) / 1000) for x in np.arange(starTime * 1000, endTime * 1000 + 5000, 5000)])

    #    xlabels = []
#    for x in range(0, endTime * 1000, 30000): #averageGroundTruthLatency[0][-1], 10000):
#        xlabels += [str(int(x / 1000))]
#    axes.set_xticklabels(xlabels)
    axes.set_yscale('log')
    axes.set_ylim(100, 10000)
    axes.set_yticks([100, 500, 1000, 5000, 10000])  # np.arange(0, 1000, 200))
    axes.get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figType + "_latency.png")
    plt.close(fig)

    fig = plt.figure(figsize=(16, 9))

    print("Draw total arrival rates")
    legend = ["Total Arrival Rate"]
    plt.plot(totalArrivalRate[0], totalArrivalRate[1], 'o', color='red', markersize=MARKERSIZE)
    plt.xlabel('Time (s)')
    plt.ylabel('Rate (tps)')
    plt.title('Total Arrival Rate')
    axes = plt.gca()
    axes.set_xlim(starTime * 1000, endTime * 1000)
    axes.set_xticks(np.arange(starTime * 1000, endTime * 1000 + 5000, 5000))
    axes.set_xticklabels([int((x - starTime * 1000) / 1000) for x in np.arange(starTime * 1000, endTime * 1000 + 5000, 5000)])
    plt.legend(legend)

    axes.set_ylim(RateRange[0], RateRange[1])
    axes.set_yticks(np.arange(4000, 6500, 500))
    #axes.set_yticks(np.arange(RateRange[0], RateRange[1], (RateRange[1] - RateRange[0]) / 5))
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figType + "_arrival.png")
    plt.close(fig)

    fig = plt.figure(figsize=(16, 9))
    print("Draw total parallelism")
    legend = []
    for i in range(0, len(exps)):
        controller = exps[i][0]
        print("Draw total parallelism for " + controller)
        legend += [controller]
        line = [[], []]
        #print(totalParallelism[i])
        for j in range(0, len(totalParallelism[i][0])):
            x0 = totalParallelism[i][0][j]
            y0 = totalParallelism[i][1][j]
            if j + 1 >= len(totalParallelism[i][0]):
                x1 = 10000000
                y1 = y0
            else:
                x1 = totalParallelism[i][0][j + 1]
                y1 = totalParallelism[i][1][j + 1]
            line[0].append(x0)
            line[0].append(x1)
            line[1].append(y0)
            line[1].append(y0)
            line[0].append(x1)
            line[0].append(x1)
            line[1].append(y0)
            line[1].append(y1)
        plt.plot(line[0], line[1], EXP_MARKER[controller], color=EXP_COLOR[controller], markersize=MARKERSIZE, linewidth=2)
    plt.plot()
    plt.legend(legend)  # ,loc='upper left')
    plt.xlabel('Time (s)')
    plt.ylabel('Parallelism (# of Task)')
    plt.title('Total Parallelism Curve')
    axes = plt.gca()
    axes.set_xlim(starTime * 1000, endTime * 1000)
    axes.set_xticks(np.arange(starTime * 1000, endTime * 1000 + 5000, 5000))
    axes.set_xticklabels([int((x - starTime * 1000) / 1000) for x in np.arange(starTime * 1000, endTime * 1000 + 5000, 5000)])
    plt.legend(legend)
    axes.set_ylim(10, 15)
    axes.set_yticks(np.arange(10, 16, 1))
    # axes.set_yticks(np.arange(RateRange[0], RateRange[1], (RateRange[1] - RateRange[0]) / 5))
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figType + "_parallelism.png")
    plt.close(fig)

def drawAll(resultsPerCurve, outputDir, curves):
    CURVENUM = len(resultsPerCurve.keys())
    fig, axs = plt.subplots(3, CURVENUM, figsize=(6 * CURVENUM, 21), layout='constrained') # (24, 6)
    #fig.suptitle('Arrival Rate/Latency/Parallelism under Different Workload')
    cindex = 0
    for curve in sorted(resultsPerCurve.keys()):
        cindex += 1
        if CURVENUM == 1:
            ax1 = axs[0]
        else:
            ax1 = axs[0, cindex - 1]

        print(cindex)
        print(curve)
        groundTruthLatencys, scaleMarkers, totalArrivalRate, totalParallelism = resultsPerCurve[curve]
        exps = curves[curve][1] #curves[curve][0]


        print("Draw total arrival rates")
        ax1.plot(totalArrivalRate[0], totalArrivalRate[1], 'o', color='red', markersize=MARKERSIZE)
        ax1.set_xlabel('Time (s)')
        ax1.set_ylabel('Arrival Rate (tps)')
        ax1.set_xlim(starTime * 1000, endTime * 1000)
        ax1.set_xticks(np.arange(starTime * 1000, endTime * 1000 + 5000, 5000))
        ax1.set_xticklabels(
            [int((x - starTime * 1000) / 1000) for x in np.arange(starTime * 1000, endTime * 1000 + 5000, 5000)])
        ax1.set_ylim(RateRange[0], RateRange[1])
        ax1.set_yticks(np.arange(4000, 6500, 500))
        ax1.grid(True)
    #fig.title('Total Arrival Rate')
    #fig.legend(legend)
    # axes.set_yticks(np.arange(RateRange[0], RateRange[1], (RateRange[1] - RateRange[0]) / 5))
    # import os
    # if not os.path.exists(outputDir):
    #     os.makedirs(outputDir)
    # plt.savefig(outputDir + "all_arrival.png")
    # plt.close(fig)

        print("Draw ground truth curve...")
        if CURVENUM == 1:
            ax1 = axs[1]
        else:
            ax1 = axs[1, cindex - 1]

        for i in range(0, len(exps)):
            controller = exps[i][0]
            print("Draw ground truth for " + controller)
            if controller == "StreamSluice":
                lineWidth = 4
            else:
                lineWidth = 2
            if cindex == 1:
                ax1.plot(groundTruthLatencys[i][0], groundTruthLatencys[i][1], EXP_MARKER[controller], color=EXP_COLOR[controller], markersize=MARKERSIZE, linewidth=lineWidth, label=controller)
            else:
                ax1.plot(groundTruthLatencys[i][0], groundTruthLatencys[i][1], EXP_MARKER[controller],color=EXP_COLOR[controller], markersize=MARKERSIZE, linewidth=lineWidth)
        for i in range(0, len(exps)):
            controller = exps[i][0]
            addScalingMarker(controller, ax1, scaleMarkers[i], groundTruthLatencys[i])
        addLatencyLimitMarker(ax1)
        #plt.legend(legend) #,loc='upper left')
        #ax1.set_xlabel('Time (s)')
        if(cindex == 1):
            ax1.set_ylabel('Latency (ms)')
        #ax1.title('Latency Curves')
        ax1.set_xlim(starTime * 1000, endTime * 1000)
        ax1.set_xticks(np.arange(starTime * 1000, endTime * 1000 + 5000, 5000))
        ax1.set_xticklabels([int((x - starTime * 1000) / 1000) for x in np.arange(starTime * 1000, endTime * 1000 + 5000, 5000)])

        #    xlabels = []
        #    for x in range(0, endTime * 1000, 30000): #averageGroundTruthLatency[0][-1], 10000):
        #        xlabels += [str(int(x / 1000))]
        #    axes.set_xticklabels(xlabels)
        ax1.set_ylim(0, 4000)
        ax1.set_yticks(np.arange(0, 4500, 500))
        # ax1.set_yscale('log')
        # ax1.set_ylim(100, 5000)
        # ax1.set_yticks([100, 500, 1000, 2000, 5000])  # np.arange(0, 1000, 200))
        # ax1.get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        ax1.grid(True)

        print("Draw total parallelism")
        if CURVENUM == 1:
            ax1 = axs[2]
        else:
            ax1 = axs[2, cindex - 1]

        legend = []
        for i in range(0, len(exps)):
            controller = exps[i][0]
            print("Draw total parallelism for " + controller)
            line = [[], []]
            #print(totalParallelism[i])
            for j in range(0, len(totalParallelism[i][0])):
                x0 = totalParallelism[i][0][j]
                y0 = totalParallelism[i][1][j]
                if j + 1 >= len(totalParallelism[i][0]):
                    x1 = 10000000
                    y1 = y0
                else:
                    x1 = totalParallelism[i][0][j + 1]
                    y1 = totalParallelism[i][1][j + 1]
                line[0].append(x0)
                line[0].append(x1)
                line[1].append(y0)
                line[1].append(y0)
                line[0].append(x1)
                line[0].append(x1)
                line[1].append(y0)
                line[1].append(y1)
            if controller == "StreamSluice":
                lineWidth = 4
            else:
                lineWidth = 2
            ax1.plot(line[0], line[1], EXP_MARKER[controller], color=EXP_COLOR[controller], markersize=MARKERSIZE, linewidth=lineWidth)
        ax1.set_xlabel('Time (s)')
        if (cindex == 1):
            ax1.set_ylabel('# of Task')
        #ax1.title('Total Parallelism Curve')
        ax1.set_xlim(starTime * 1000, endTime * 1000)
        ax1.set_xticks(np.arange(starTime * 1000, endTime * 1000 + 5000, 5000))
        ax1.set_xticklabels([int((x - starTime * 1000) / 1000) for x in np.arange(starTime * 1000, endTime * 1000 + 5000, 5000)])
        ax1.set_ylim(10, 20)
        ax1.set_yticks(np.arange(10, 21, 2))
        # axes.set_yticks(np.arange(RateRange[0], RateRange[1], (RateRange[1] - RateRange[0]) / 5))
        ax1.grid(True)
    lines = []
    labels = []
    for ax in fig.axes:
        Line, Label = ax.get_legend_handles_labels()
        # print(Label)
        lines.extend(Line)
        labels.extend(Label)
    fig.legend(lines, labels, bbox_to_anchor=(0.5, 1.15), loc='upper center', ncol=5)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + "AllinOne_how.pdf", bbox_inches='tight')
    plt.close(fig)


rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/part3/"
curves = {}
with open("../howList.txt") as f:
    lines = f.readlines()
    for i in range(0, len(lines)):

        line = lines[i]
        split = line.rstrip().split()
        if len(split) == 1 and (split[0].startswith("1") or split[0].startswith("2") or split[0].startswith("3") or split[0].startswith("4") or split[0].startswith("5")):
            exps_whether = []
            exps_how = []
            curves[split[0]] = [exps_whether, exps_how]
            continue
        if len(split) == 0:
            continue
        name = split[0]
        path = split[1]
        if name.startswith("Whether"):
            exps_whether += [["_".join(name.split("_")[1:]), path]]
        elif name.startswith("How"):
            exps_how += [["_".join(name.split("_")[1:]), path]]
        else:
            exps_whether += [[name, path]]
            exps_how += [[name, path]]

windowSize = 50 #500
latencyLimit = 2000
starTime = 25
endTime = 45
RateRange = [3500, 6500]
isSingleOperator = False
resultsPerCurve = {}
for curve in sorted(curves.keys()):
    print("draw curve " + curve)
    exps_whether = curves[curve][0]
    exps_how = curves[curve][1]
    #draw(rawDir, outputDir + curve + "/", exps_whether, windowSize, "whether")
    #resultsPerCurve[curve] = retrieveResults(rawDir, exps_whether, windowSize)
    resultsPerCurve[curve] = retrieveResults(rawDir, exps_how, windowSize)
drawAll(resultsPerCurve, outputDir + "/", curves)