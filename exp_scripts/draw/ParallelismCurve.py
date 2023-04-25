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
def draw(streamSluiceOutputPath, groundTruthPath, outputDir, windowSize):

    initialTime = -1
    lastTime = 0
    ParallelismPerJob = {}
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

                if split[9] == "New" and split[10] == "mapping:":
                    mapping = parseMapping(split[11:])

            if(split[0] == "++++++" and split[1] == "Time:" and split[3] == "all" and split[5] == "plan" and split[6] == "deployed."):
                time = int(split[2])
                scalingMarker += [[time - initialTime, 3]]
                for job in mapping:
                    ParallelismPerJob[job][0].append(time - initialTime)
                    ParallelismPerJob[job][1].append(len(mapping[job].keys()))

            if(split[0] == "++++++" and split[1] == "Time:" and split[3] == "backlog:"):
                time = int(split[2])
                backlogs = parsePerTaskValue(split[4:])
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
    ParallelismPerJob["TOTAL"] = [[], []]
    for job in ParallelismPerJob:
        if job != "TOTAL":
            for i in range(0, len(ParallelismPerJob[job][0])):
                if i >= len(ParallelismPerJob["TOTAL"][0]):
                    ParallelismPerJob["TOTAL"][0].append(ParallelismPerJob[job][0][i])
                    ParallelismPerJob["TOTAL"][1].append(ParallelismPerJob[job][1][i])
                else:
                    ParallelismPerJob["TOTAL"][1][i] += ParallelismPerJob[job][1][i]
    print(ParallelismPerJob)

    figName = "Parallelism.png"


    fig = plt.figure(figsize=(24, 18))
    colors = {}
    colors["TOTAL"] = "red"
    colors["c21234bcbf1e8eb4c61f1927190efebd"] = "blue"
    colors["22359d48bcb33236cf1e31888091e54c"] = "green"
    legend = []
    for job in ParallelismPerJob:
        print("Draw Job " + job + " curve...")
        legend += ["Parallelism of job " + job]
        line = [[], []]
        for i in range(0, len(ParallelismPerJob[job][0])):
            x0 = ParallelismPerJob[job][0][i]
            y0 = ParallelismPerJob[job][1][i]
            if i+1 >= len(ParallelismPerJob[job][0]):
                x1 = 10000000
                y1 = y0
            else:
                x1 = ParallelismPerJob[job][0][i+1]
                y1 = ParallelismPerJob[job][1][i+1]
            line[0].append(x0)
            line[0].append(x1)
            line[1].append(y0)
            line[1].append(y0)
            line[0].append(x1)
            line[0].append(x1)
            line[1].append(y0)
            line[1].append(y1)
        plt.plot(line[0], line[1], color=colors[job], linewidth=LINEWIDTH)
    plt.legend(legend, loc='upper left')
    #addScalingMarker(plt, scalingMarker)
    plt.xlabel('Time (s)')
    plt.ylabel('# of tasks')
    plt.title('Parallelism of ' + job)
    axes = plt.gca()
    axes.set_xlim(0, lastTime-initialTime)
    axes.set_xticks(np.arange(0, lastTime-initialTime, 10000))

    xlabels = []
    for x in range(0, lastTime-initialTime, 10000):
        xlabels += [str(int(x / 1000))]
    axes.set_xticklabels(xlabels)
    axes.set_ylim(0, 50)
    axes.set_yticks(np.arange(0, 32, 4))
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
