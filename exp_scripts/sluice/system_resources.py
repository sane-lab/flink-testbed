import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def supylabel2(fig, s, **kwargs):
    defaults = {
        "x": 0.98,
        "y": 0.5,
        "horizontalalignment": "center",
        "verticalalignment": "center",
        "rotation": "vertical",
        "rotation_mode": "anchor",
        "size": plt.rcParams["figure.labelsize"],  # matplotlib >= 3.6
        "weight": plt.rcParams["figure.labelweight"],  # matplotlib >= 3.6
    }
    kwargs["s"] = s
    # kwargs = defaults | kwargs  # python >= 3.9
    kwargs = {**defaults, **kwargs}
    fig.text(**kwargs)

OPERATOR_NAMING = {
    "0a448493b4782967b150582570326227": "Stateful Map",
    "c21234bcbf1e8eb4c61f1927190efebd": "Splitter",
    "22359d48bcb33236cf1e31888091e54c": "Counter",
    "a84740bacf923e828852cc4966f2247c": "OP2",
    "eabd4c11f6c6fbdf011f0f1fc42097b1": "OP3",
    "d01047f852abd5702a0dabeedac99ff5": "OP4",
    "d2336f79a0d60b5a4b16c8769ec82e47": "OP5",
    "36fcfcb61a35d065e60ee34fccb0541a": "OP6",
    "c395b989724fa728d0a2640c6ccdb8a1": "OP7",
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

#APP_NAMING = ["TF", "PA", "VA", "TA"]
APP_NAMING = ["PP", "PA", "VF", "VA", "Join", "AN"]

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
    scalings = []

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
                    if (arrivedTime < 0):
                        print("!!!! " + str(i) + "  " + line)
                    if (initialTime == -1 or initialTime > arrivedTime):
                        initialTime = arrivedTime
                    if (lastTime < completedTime):
                        lastTime = completedTime
    print("init time=" + str(initialTime) + " last time=" + str(lastTime))

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
                scalings.append(time - initialTime)

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
    return [ParallelismPerJob, totalArrivalRatePerJob, initialTime, scalings]

def draw(rawDir, outputDir, exps):
    parallelismsPerJob = {}
    totalArrivalRatesPerJob = {}
    totalParallelismPerExps = {}
    for expindex in range(0, len(exps)):
        expFile = exps[expindex][1]
        result = readParallelism(rawDir, expFile)
        parallelisms = result[0]
        totalArrivalRates = result[1]
        scalings = result[3]
        for job in parallelisms.keys():
            if job == "TOTAL":
                totalParallelismPerExps[expindex] = parallelisms[job]
                overall_resource[app][expindex] = []
                for i in range(0, len(parallelisms[job][1])):
                    l = 0
                    r = 0
                    if (i + 1 < len(parallelisms[job][0])):
                        r = parallelisms[job][0][i + 1]
                    l = max(parallelisms[job][0][i], startTime * 1000)
                    r = min(r, (startTime + exp_length) * 1000)
                    if (l < r):
                        for j in range(0, r - l):
                            overall_resource[app][expindex] += [parallelisms[job][1][i]]
                continue
            if job not in parallelismsPerJob:
                parallelismsPerJob[job] = []
                totalArrivalRatesPerJob[job] = []
            parallelismsPerJob[job] += [parallelisms[job]]
            totalArrivalRatesPerJob[job] += [totalArrivalRates[job]]
    print("Draw total figure...")
    print("TOTAL parallelism: " + str(totalParallelismPerExps))

    figName = "Parallelism"
    nJobs = len(parallelismsPerJob.keys())
    jobList = ["a84740bacf923e828852cc4966f2247c", "eabd4c11f6c6fbdf011f0f1fc42097b1", "d01047f852abd5702a0dabeedac99ff5", "d2336f79a0d60b5a4b16c8769ec82e47", "feccfb8648621345be01b71938abfb72"]
    fig, axs = plt.subplots(1, 1, figsize=(12, 5), layout='constrained')

    # Add super label
    #fig.supylabel('# of Slots')
    #supylabel2(fig, "Arrival Rate (tps)")
    fig.tight_layout(rect=[0.02, 0, 0.953, 1])
    axs.grid(True)
    ax1 = axs
    ax2 = ax1.twinx()
    ax1.set_ylabel("# of Slots")
    ax2.set_ylabel("Arrival Rate (tps)")

    job = jobList[0]
    ax = sorted(totalArrivalRatesPerJob[job][0].keys())
    ay = [totalArrivalRatesPerJob[job][0][x] / (windowSize / 100) for x in ax]
    ax2.plot(ax, ay, '-', color='red', markersize=MARKERSIZE / 2, label="Arrival Rate")
    #ax2.set_ylabel('Rate (tps)')
    ax2.set_ylim(0, 30000)
    ax2.set_yticks(np.arange(0, 35000, 5000))
    # legend = ["OP_" + str(jobIndex + 1) +"Arrival Rate"]
    legend = ["Arrival Rate"]
    # ax2.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
    # ax2.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 300000, 300000))
    # ax2.set_xticklabels([int((x - startTime * 1000) / 60000) for x in
    #                      np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000)])
    ax2.legend(legend, loc='upper right', bbox_to_anchor=(1.1, 1.3), ncol=1)



    legend = []
    scalingPoints = [[], []]
    for expindex in range(0, len(exps)):
        if(exps[expindex][0] == "Static"):
            continue
        print("Draw exps " + exps[expindex][0] + " curve...")
        totalParallelism = 0
        Parallelism = totalParallelismPerExps[expindex]
        # print(job + " " + str(expindex) + " " + str(Parallelism))
        legend += [exps[expindex][0]]
        line = [[], []]
        for i in range(0, len(Parallelism[0])):
            x0 = Parallelism[0][i]
            y0 = Parallelism[1][i]
            if i + 1 >= len(Parallelism[0]):
                x1 = 10000000
                y1 = y0
            else:
                x1 = Parallelism[0][i + 1]
                y1 = Parallelism[1][i + 1]
            l = max(x0, startTime * 1000)
            r = min(x1, (startTime + exp_length) * 1000)
            if(exps[expindex][0] == 'Sluice' and l < r):
                totalParallelism += (r - l) * y0
                for scalingTime in scalings:
                    if scalingTime >= l and scalingTime <= r:
                        scalingPoints[0] += [scalingTime]
                        scalingPoints[1] += [y0]
            line[0].append(x0)
            line[0].append(x1)
            line[1].append(y0)
            line[1].append(y0)
            line[0].append(x1)
            line[0].append(x1)
            line[1].append(y0)
            line[1].append(y1)
        if exps[expindex][0] == 'Sluice':
            linewidth = LINEWIDTH
        else:
            linewidth = LINEWIDTH / 2.0
        ax1.plot(line[0], line[1], color=exps[expindex][2], linewidth=linewidth)
        print("Average parallelism " + exps[expindex][0] + " : " + str(totalParallelism / (exp_length * 1000)))
    ax1.plot(scalingPoints[0], scalingPoints[1], 'o', color="orange", mfc='none', markersize=MARKERSIZE * 2, label="Scaling")
    ax1.legend(legend, loc='upper left', bbox_to_anchor=(-0.1, 1.3), ncol=3, markerscale=4.)
    # ax1.set_ylabel('OP_'+str(jobIndex+1)+' Parallelism')
    ax1.set_ylim(4, 17) #17)
    ax1.set_yticks(np.arange(4, 18, 1)) #18, 1))

    ax1.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
    ax1.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 60000, 60000))
    ax1.set_xticklabels([int((x - startTime * 1000) / 1000) for x in
                         np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 60000, 60000)])
    ax1.set_xlabel("Time (s)")



    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    # plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
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



rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
#expName = "streamsluice-scaletest-400-600-500-5-2000-1000-100-1"
#expName = "autotune_4op-false-390-10000-12500-60-15000-60-12500-60-1-0-2-125-1-5000-2-120-1-5000-3-250-1-5000-6-500-5000-2000-1500-100-true-1"
exps = {
    # "Stock": [
    #     ["DS2",
    #      "stock_analysis-ds2-ds2-3990-30-1000-20-2-500-6-5000-3-1000-4-3000-1-5-4000-2000-100-true-3-true-1",
    #      "purple", "d"],
    #     ["StreamSwitch",
    #      #"stock_analysis-streamswitch-streamswitch-2190-30-1000-20-2-500-6-5000-3-1000-4-3000-1-5-4000-2000-100-true-3-true-2",
    #      "stock_analysis-streamswitch-streamswitch-3990-30-1000-20-2-500-6-5000-3-1000-4-3000-1-5-4000-2000-100-true-3-true-2",
    #      "green", "p"],
    #     ["Sluice",
    #       "stock_analysis-streamsluice-streamsluice-3990-30-1000-20-2-500-6-5000-3-1000-4-3000-1-5-4000-2000-100-true-3-true-1",
    #       "blue", "o"],
    # ],
    # "Tweet": [
    #     ["DS2",
    #      "tweet_alert-ds2-ds2-2190-30-1800-1-30-5000-10-1000-1-50-1-100-2000-100-true-3-true-1",
    #      "purple", "d"],
    #     ["StreamSwitch",
    #      "tweet_alert-streamswitch-streamswitch-2190-30-1800-1-30-5000-10-1000-1-50-1-100-2000-100-true-3-true-1",
    #      "green", "p"],
    #     ["Sluice",
    #       "tweet_alert-streamsluice-streamsluice-2190-30-1800-1-30-5000-10-1000-1-50-1-100-2000-100-true-3-true-2",
    #       "blue", "o"],
    # ],
    "Linear_Road": [
        ["Static",
         #"system-streamsluice-ds2-true-true-false-when-mixed-1split2join1-760-6000-3000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-6-510-5000-1000-3000-100-1-false-1",
         "system-true-streamsluice-ds2-false-true-true-false-when-linear-3op_line-390-10000-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-false-1",
         "green", "o"],
        # ["Earlier",
        #  "systemsensitivity-streamsluice_earlier-streamsluice-when-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
        #  "green", "o"],
        # ["Later",
        #  "systemsensitivity-streamsluice_later-streamsluice-when-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
        #  "orange", "o"],
        # ["Sluice",
        #  "systemsensitivity-streamsluice-streamsluice-when-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
        #  "blue", "o"],
        # ["Earlier",
        #  "systemsensitivity-streamsluice_earlier-streamsluice-when-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2000-3000-100-10-true-1",
        #  "green", "o"],
        # ["Later",
        #  "systemsensitivity-streamsluice_later-streamsluice-when-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2000-3000-100-10-true-1",
        #  "orange", "o"],
        # ["Sluice",
        #  "systemsensitivity-streamsluice-streamsluice-when-1split2join1-400-6000-3000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-6-510-5000-2000-3000-100-10-true-1",
        #  "blue", "o"],
        ["Sluice",
         #"systemsensitivity-streamsluice-streamsluice-when-1split2join1-400-6000-3000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-6-510-5000-2000-3000-100-10-true-1",
         #"system-streamsluice-streamsluice-true-true-false-when-mixed-1split2join1-760-6000-3000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-6-510-5000-2000-3000-100-1-true-1",
         "system-true-streamsluice-ds2-false-true-true-false-when-linear-3op_line-390-10000-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-false-1",
         "blue", "o"],

        # ["Static",
        #  "systemsensitivity-streamsluice-streamsluice-how-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-false-1",
        #  "purple", "o"],
        # ["Not_Bottleneck",
        #  "systemsensitivity-streamsluice-streamsluice_not_bottleneck-how-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
        #  "orange", "o"],
        # ["No_Balance",
        #  "systemsensitivity-streamsluice-streamsluice_no_balance-how-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
        #  "purple", "o"],
        # ["More",
        #  "systemsensitivity-streamsluice-streamsluice_more-how-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
        #  "green", "o"],
        # ["Less",
        #  "systemsensitivity-streamsluice-streamsluice_less-how-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
        #  "orange", "o"],
        # ["Sluice",
        #  "systemsensitivity-streamsluice-streamsluice-how-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
        #  "blue", "o"],
        # ["Not_Bottleneck",
        #  "systemsensitivity-streamsluice-streamsluice_not_bottleneck-how-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
        #  "orange", "o"],
        # ["No_Balance",
        #  "systemsensitivity-streamsluice-streamsluice_no_balance-how-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
        #  "purple", "o"],
        # ["More",
        #  "systemsensitivity-streamsluice-streamsluice_more-how-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
        #  "green", "o"],
        # ["Less",
        #  "systemsensitivity-streamsluice-streamsluice_less-how-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
        #  "orange", "o"],
        # ["Sluice",
        #  "systemsensitivity-streamsluice-streamsluice-how-1split2join1-400-6000-3000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-6-510-5000-2000-3000-100-10-true-1",
        #  "blue", "o"],
    ],
}
windowSize=1000
startTime=60 #30+300 #30
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

slot_ylim_app = {
    "Stock": 45,
    "Tweet": 30,
    "Linear_Road": 100, #27,
}
arrivalrate_ylim_app = {
    "Stock": 10000,
    "Tweet": 10000,
    "Linear_Road": 10000,
}
overall_resource = {}
trickFlag = True
for app in exps.keys():
    expName = [exp[1] for exp in exps[app] if exp[0] == "StreamSluice" or exp[0] == "Sluice"][0]
    exp_length = 300 #300 #900 #480 #480 #360 #1800
    startTime = 60 # + 300
    print(expName)
    overall_resource[app] = {}
    draw(rawDir, outputDir + expName + "/", exps[app])
#drawOverallResource(outputDir + expName + "/", overall_resource)