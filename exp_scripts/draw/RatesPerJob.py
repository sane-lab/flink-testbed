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

def draw(streamSluiceOutputPath, groundTruthPath, outputDir, windowSize):

    initialTime = -1
    lastTime = 0

    longestScalingTime = 0
    arrivalRatePerTask = {}
    serviceRatePerTask = {}
    backlogPerTask = {}
    selectivity = {}

    scalingMarker = []

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
                if (lastTime < completedTime):
                    lastTime = completedTime
    print(lastTime)
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
            if(split[0] == "++++++" and split[1] == "Time:" and split[4] == "Model" and split[5] == "decides" and split[7] == "scale"):
                time = int(split[2])
                if(split[8] == "in."):
                    type = 1
                elif(split[8] == "out."):
                    type = 2
                scalingMarker += [[time - initialTime, type]]
            if(split[0] == "++++++" and split[1] == "Time:" and split[3] == "all" and split[5] == "plan" and split[6] == "deployed."):
                time = int(split[2])
                scalingMarker += [[time - initialTime, 3]]

            if(split[0] == "++++++" and split[1] == "Time:" and split[3] == "backlog:"):
                time = int(split[2])
                backlogs = parsePerTaskValue(split[4:])
                for task in backlogs:
                    if task not in backlogPerTask:
                        backlogPerTask[task] = [[], []]
                    backlogPerTask[task][0] += [time - initialTime]
                    backlogPerTask[task][1] += [int(backlogs[task])]
            if(split[0] == "++++++" and split[1] == "Time:" and split[3] == "arrivalRate:"):
                time = int(split[2])
                arrivalRates = parsePerTaskValue(split[4:])
                for task in arrivalRates:
                    if task not in arrivalRatePerTask:
                        arrivalRatePerTask[task] = [[], []]
                    arrivalRatePerTask[task][0] += [time - initialTime]
                    arrivalRatePerTask[task][1] += [int(arrivalRates[task] * 1000)]
            if (split[0] == "++++++" and split[1] == "Time:" and split[3] == "serviceRate:"):
                time = int(split[2])
                serviceRates = parsePerTaskValue(split[4:])
                for task in serviceRates:
                    if task not in serviceRatePerTask:
                        serviceRatePerTask[task] = [[], []]
                    serviceRatePerTask[task][0] += [time - initialTime]
                    serviceRatePerTask[task][1] += [int(serviceRates[task] * 1000)]
    print(backlogPerTask)
    print(arrivalRates)

    taskPerFig = [[]]
    for task in sorted(backlogPerTask.keys()):
        if len(taskPerFig[-1]) >= MAXTASKPERFIG:
            taskPerFig += [[]]
        taskPerFig[-1] += [task]

    for i in range(0, len(taskPerFig)):
        print("Draw " + str(i) + "-th figure...")
        figName = "ratesPerTask_" + str(i) + ".png"
        fig = plt.figure(figsize=(24, 30))
        print(taskPerFig[i])
        for j in range(0, len(taskPerFig[i])):
            task = taskPerFig[i][j]
            print("Draw task " + task + " figure...")
            ax1 = plt.subplot(MAXTASKPERFIG, 1, j + 1)
            print("Draw arrival rates")
            legend = ["Arrival Rate"]
            plt.plot(arrivalRatePerTask[task][0], arrivalRatePerTask[task][1], '*', color='red', markersize=MARKERSIZE)
            print("Draw service rates")
            legend += ["Service Rate"]
            plt.plot(serviceRatePerTask[task][0], serviceRatePerTask[task][1], '*', color='blue', markersize=MARKERSIZE)
            plt.legend(legend, loc='upper left')
            addScalingMarker(plt, scalingMarker)
            plt.xlabel('Time (s)')
            plt.ylabel('Rate (tps)')
            plt.title('Rates and Backlog of ' + task)
            axes = plt.gca()
            axes.set_xlim(0, lastTime-initialTime)
            axes.set_xticks(np.arange(0, lastTime-initialTime, 10000))

            xlabels = []
            for x in range(0, lastTime-initialTime, 10000):
                xlabels += [str(int(x / 1000))]
            axes.set_xticklabels(xlabels)
            axes.set_ylim(0, 1000)
            axes.set_yticks(np.arange(0, 1000, 200))
            plt.grid(True)
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
        plt.subplot(2, 1, i+1)

        print("Draw total arrival rates")
        legend = ["Arrival Rate"]
        plt.plot(ax, ay, '*', color='red', markersize=MARKERSIZE)
        print("Draw total service rates")
        legend += ["Service Rate"]
        plt.plot(sx, sy, '*', color='blue', markersize=MARKERSIZE)
        plt.legend(legend, loc='upper left')
        addScalingMarker(plt, scalingMarker)
        plt.xlabel('Time (s)')
        plt.ylabel('Rate (tps)')
        plt.title('Rates and Backlog of ' + job)
        axes = plt.gca()
        axes.set_xlim(0, lastTime - initialTime)
        axes.set_xticks(np.arange(0, lastTime - initialTime, 10000))

        xlabels = []
        for x in range(0, lastTime - initialTime, 10000):
            xlabels += [str(int(x / 1000))]
        axes.set_xticklabels(xlabels)
        axes.set_ylim(0, 10000)
        axes.set_yticks(np.arange(0, 10000, 2000))
        plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figName)
    plt.close(fig)



streamSluiceOutputPath = "/home/swrrt11/Workspace/flinks/flink-extended/build-target/log/flink-swrrt11-standalonesession-0-dl.out"
groundTruthPath = "/home/swrrt11/Workspace/flinks/flink-extended/build-target/log/flink-swrrt11-taskexecutor-0-dl.out"
outputDir = "/home/swrrt11/Workspace/StreamSluice/Experiments/test/"
windowSize = 100
draw(streamSluiceOutputPath, groundTruthPath, outputDir, windowSize)
