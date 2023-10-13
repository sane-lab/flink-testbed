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
        plt.plot(x, y, color=color, linewidth=LINEWIDTH)

def draw(rawDir, outputDir, expName):

    initialTime = -1
    lastTime = 0

    longestScalingTime = 0
    arrivalRatePerTask = {}
    serviceRatePerTask = {}
    backlogPerTask = {}
    parallelismsPerOperator = {}
    selectivity = {}


    scalingMarkerByOperator = {}

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

    print(scalingMarkerByOperator)
    if drawTaskFigureFlag:
        for i in range(0, len(taskPerFig)):
            print("Draw " + str(i) + "-th figure...")
            figName = "ratesPerTask_" + str(i) + ".png"
            fig = plt.figure(figsize=(24, 30))
            print(taskPerFig[i])
            for j in range(0, len(taskPerFig[i])):
                task = taskPerFig[i][j]
                print("Draw task " + task + " figure...")
                ax1 = plt.subplot(MAXTASKPERFIG, 1, j + 1)
                ax2 = ax1.twinx()
                print("Draw arrival rates")
                ax1.plot(arrivalRatePerTask[task][0], arrivalRatePerTask[task][1], '*', color='red', markersize=MARKERSIZE, label="Arrival Rate")
                print("Draw service rates")
                ax1.plot(serviceRatePerTask[task][0], serviceRatePerTask[task][1], '*', color='blue', markersize=MARKERSIZE, label="Service Rate")
                operator = task.split('_')[0]
                if(operator in scalingMarkerByOperator):
                    addScalingMarker(plt, scalingMarkerByOperator[operator])
                ax1.set_xlabel('Time (s)')
                ax1.set_ylabel('Rate (tps)')
                plt.title('Rates and Backlog of ' + task)
                ax1.set_xlim(0, lastTime-initialTime)
                ax1.set_xticks(np.arange(0, lastTime-initialTime, 10000))
                xlabels = []
                for x in range(0, lastTime-initialTime, 10000):
                    xlabels += [str(int(x / 1000))]
                ax1.set_xticklabels(xlabels)
                job = task.split("_")[0]
                ylim = rateYMax[OPERATOR_NAMING[job]]
                ax1.set_ylim(1, ylim)
                ax1.set_yticks(np.arange(0, ylim, ylim / 10))
                plt.grid(True)

                print("Draw backlog")
                ax2.plot(backlogPerTask[task][0], backlogPerTask[task][1], '*', color='grey', markersize=MARKERSIZE, label="Backlog")
                lines1, labels1 = ax1.get_legend_handles_labels()
                lines2, labels2 = ax2.get_legend_handles_labels()
                ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
                ax2.set_xlabel('Time (s)')
                ax2.set_ylabel('# of Tuples')
                #ax2.set_yscale('log')
                # ax2.set_yticks([1, 100, 1000, 10000, 100000])
                job = task.split("_")[0]
                ylim = backlogYMax[OPERATOR_NAMING[job]]
                ax2.set_ylim(1, ylim)
                ax2.set_yticks(np.arange(0, ylim, ylim / 10))

            import os
            if not os.path.exists(outputDir):
                os.makedirs(outputDir)
            plt.savefig(outputDir + figName)
            plt.close(fig)

    totalArrivalRatePerJob = {}
    totalServiceRatePerJob = {}
    for task in arrivalRatePerTask:
        job = task.split("_")[0]
        n = len(arrivalRatePerTask[task][0])
        if job not in totalServiceRatePerJob:
            totalServiceRatePerJob[job] = {}
            totalArrivalRatePerJob[job] = {}
        for i in range(0, n):
            ax = arrivalRatePerTask[task][0][i]
            ay = arrivalRatePerTask[task][1][i]
            sx = serviceRatePerTask[task][0][i]
            sy = serviceRatePerTask[task][1][i]
            if ax not in totalArrivalRatePerJob[job]:
                totalArrivalRatePerJob[job][ax] = ay
            else:
                totalArrivalRatePerJob[job][ax] += ay
            if sx not in totalServiceRatePerJob[job]:
                totalServiceRatePerJob[job][sx] = sy
            else:
                totalServiceRatePerJob[job][sx] += sy
    print("Draw total figure...")
    figName = "total_rates.png"
    fig = plt.figure(figsize=(24, 18))
    for i in range(0, len(totalArrivalRatePerJob.keys())):
        job = sorted(totalArrivalRatePerJob.keys())[i]
        ax = sorted(totalArrivalRatePerJob[job].keys())
        ay = [totalArrivalRatePerJob[job][x] for x in ax]
        sx = sorted(totalServiceRatePerJob[job].keys())
        sy = [totalServiceRatePerJob[job][x] for x in sx]

        print("Draw job " + job + " figure...")
        plt.subplot(len(totalArrivalRatePerJob.keys()), 1, i+1)

        print("Draw total arrival rates")
        legend = ["Arrival Rate"]
        plt.plot(ax, ay, '*', color='red', markersize=MARKERSIZE)
        print("Draw total service rates")
        legend += ["Service Rate"]
        plt.plot(sx, sy, '*', color='blue', markersize=MARKERSIZE)
        plt.legend(legend, loc='upper left')
        #for operator in scalingMarkerByOperator:
        if(job in scalingMarkerByOperator):
            addScalingMarker(plt, scalingMarkerByOperator[job])
        plt.xlabel('Time (s)')
        plt.ylabel('Rate (tps)')
        plt.title('Rates of ' + OPERATOR_NAMING[job])
        axes = plt.gca()
        axes.set_xlim(0, lastTime - initialTime)
        axes.set_xticks(np.arange(0, lastTime - initialTime, 20000))

        xlabels = []
        for x in range(0, lastTime - initialTime, 20000):
            xlabels += [str(int(x / 1000))]
        axes.set_xticklabels(xlabels)
        ylim = totalYMax[OPERATOR_NAMING[job]]
        axes.set_ylim(0, ylim)
        axes.set_yticks(np.arange(0, ylim, ylim/10))
        plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figName)
    plt.close(fig)



rateYMax = {
    "Stateful Map": 200,
    "Splitter": 500,
    "Counter": 200,
    "OP2": 10000,
    "OP3": 5000,
    "OP4": 5000,
    "OP5": 10000,
}
backlogYMax = {
    "Stateful Map": 200,
    "Splitter": 1000,
    "Counter": 1000,
    "OP2": 10000,
    "OP3": 10000,
    "OP4": 10000,
    "OP5": 10000,
}
totalYMax = {
    "Stateful Map": 1000,
    "Splitter": 1000,
    "Counter": 2000,
    "OP2": 20000,
    "OP3": 20000,
    "OP4": 20000,
    "OP5": 20000,
}
rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
drawTaskFigureFlag = True #False
expName = "streamsluice-4op-300-8000-8000-10000-120-1-0.5-2-200-1-100-5-500-1-100-3-333-1-100-3-250-100-1000-600-100-true-1"
#expName = "streamsluice-twoOP-180-400-400-500-30-5-10-2-0.25-1500-500-10000-100-true-1"
import sys
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]
draw(rawDir, outputDir + expName + "/", expName)
