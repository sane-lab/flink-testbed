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

EXP_COLOR = {
    "Baseline" : "gray",
    "Streamsluice" : "blue",
    "Threshold_25": "red",
    "Threshold_50": "orange",
    "Threshold_75": "purple",
    "Threshold_100": "green",
    "Streamsluice_earlier": "red",
    "Streamsluice_later": "orange",
    "Streamsluice_40": "purple",
    "Streamsluice_50": "green",
    "Streamsluice_latency_only": "red",
    "Streamsluice_trend_only": "orange",
    "DS2": "purple",
    "DRS": "green",
    "Dhalion": "brown"
}
EXP_MARKER = {
    "Baseline" : "o-",
    "Streamsluice" : "o-",
    "Threshold_25": "*-",
    "Threshold_50": "v-",
    "Threshold_75": "s-",
    "Threshold_100": "X-",
    "Streamsluice_earlier": "*-",
    "Streamsluice_later": "v-",
    "Streamsluice_40": "s-",
    "Streamsluice_50": "X-",
    "Streamsluice_latency_only": "*-",
    "Streamsluice_trend_only": "v-",
    "DS2" : "s-",
    "DRS": "X-",
    "Dhalion": "D-"
}

SCALE_MARKER = {
    "Baseline" : "o", #"o",
    "Streamsluice" : "o",
    "Threshold_25": "*",
    "Threshold_50": "v",
    "Threshold_75": "s",
    "Threshold_100": "X",
    "Streamsluice_earlier": "*",
    "Streamsluice_later": "v",
    "Streamsluice_40": "s",
    "Streamsluice_50": "X",
    "Streamsluice_latency_only": "*",
    "Streamsluice_trend_only": "v",
    "DS2" : "s",
    "DRS": "X",
    "Dhalion": "D"
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
                marker = SCALE_MARKER[exp]
            #elif type == 1:
            #    color = "green"
            #else:
            #    color = "gray"
                index = 0
                for i in range(0, len(curve[0]) - 1):
                    if time >= curve[0][i] and time < curve[0][i + 1]:
                        index = i
                        break
                x = [time]
                y = [curve[1][index]]
                plt.plot(x, y, color=color, marker=marker, markersize=MARKERSIZE * 2)

def readGroundTruthLatency(rawDir, expName, windowSize):
    initialTime = -1

    groundTruthLatencyPerTuple = {}
    groundTruthLatency = []
    scalingMarkerByOperator = {}

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


    aggregatedGroundTruthLatency = {}
    for pair in groundTruthLatency:
        index = int((pair[0] - initialTime) / windowSize)
        if index not in aggregatedGroundTruthLatency:
            aggregatedGroundTruthLatency[index] = [0, 0]
        aggregatedGroundTruthLatency[index][0] += pair[1]
        aggregatedGroundTruthLatency[index][1] += 1

    averageGroundTruthLatency = [[], []]
    for index in sorted(aggregatedGroundTruthLatency):
        time = index * windowSize
        x = int(time)
        y = int(aggregatedGroundTruthLatency[index][0] / float(aggregatedGroundTruthLatency[index][1]))
        averageGroundTruthLatency[0] += [x]
        averageGroundTruthLatency[1] += [y]
    return [averageGroundTruthLatency, scalingMarkerByOperator]


def draw(rawDir, outputDir, exps, windowSize):

    groundTruthLatencys = []
    scaleMarkers = []
    for i in range(0, len(exps)):
        controller = exps[i][0]
        print("Read ground truth for " + controller)
        expName = exps[i][1]
        result = readGroundTruthLatency(rawDir, expName, windowSize)
        groundTruthLatencys += [result[0]]
        scaleMarkers += [result[1]]
    fig = plt.figure(figsize=(24, 18))
    print("Draw ground truth curve...")
    legend = []
    for i in range(0, len(exps)):
        controller = exps[i][0]
        print("Draw ground truth for " + controller)
        legend += [controller + " Truth Latency"]
        plt.plot(groundTruthLatencys[i][0], groundTruthLatencys[i][1], EXP_MARKER[controller], color=EXP_COLOR[controller], markersize=MARKERSIZE)

    for i in range(0, len(exps)):
        controller = exps[i][0]
        addScalingMarker(controller, plt, scaleMarkers[i], groundTruthLatencys[i])
    addLatencyLimitMarker(plt)
    plt.plot()
    plt.legend(legend, loc='upper left')
    plt.xlabel('Time (s)')
    plt.ylabel('Latency (ms)')
    plt.title('Latency Curves')
    axes = plt.gca()
    axes.set_xlim(0, endTime * 1000)#averageGroundTruthLatency[0][-1])
    axes.set_xticks(np.arange(0, endTime * 1000, 30000))# averageGroundTruthLatency[0][-1], 10000))

    xlabels = []
    for x in range(0, endTime * 1000, 30000): #averageGroundTruthLatency[0][-1], 10000):
        xlabels += [str(int(x / 1000))]
    axes.set_xticklabels(xlabels)
    # axes.set_yscale('log')
    axes.set_ylim(0, 1200)
    axes.set_yticks(np.arange(0, 1200, 200))
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + 'different_controller_ground_truth_latency_curves.png')
    plt.close(fig)


rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
exps = [
    ["Baseline", "streamsluice-scaleout-streamsluice-streamsluice-120-400-600-500-120-5-0-1000-500-10000-100-false-1"],
    ["Streamsluice", "streamsluice-scaleout-streamsluice-streamsluice-120-400-600-500-120-5-0-1000-500-10000-100-true-1"],
    ["Threshold_25", "streamsluice-scaleout-streamsluice_threshold25-streamsluice-120-400-600-500-120-5-0-1000-500-10000-100-true-1"],
    ["Threshold_50", "streamsluice-scaleout-streamsluice_threshold50-streamsluice-120-400-600-500-120-5-0-1000-500-10000-100-true-1"],
    ["Threshold_75", "streamsluice-scaleout-streamsluice_threshold75-streamsluice-120-400-600-500-120-5-0-1000-500-10000-100-true-1"],
    ["Threshold_100", "streamsluice-scaleout-streamsluice_threshold100-streamsluice-120-400-600-500-120-5-0-1000-500-10000-100-true-1"],
    #["Streamsluice_earlier", "streamsluice-scaleout-streamsluice_earlier-streamsluice-120-400-600-500-120-5-0-1000-500-1000-100-true-1"],
    #["Streamsluice_later", "streamsluice-scaleout-streamsluice_later-streamsluice-120-400-600-500-120-5-0-1000-500-1000-100-true-1"],
    #["Streamsluice_40", "streamsluice-scaleout-streamsluice_40-streamsluice-120-400-600-500-120-5-0-1000-500-1000-100-true-1"],
    #["Streamsluice_50", "streamsluice-scaleout-streamsluice_50-streamsluice-120-400-600-500-120-5-0-1000-500-1000-100-true-1"],
    #["Streamsluice_latency_only", "streamsluice-scaleout-streamsluice_trend_only-streamsluice-120-400-600-500-120-5-0-1000-500-1000-100-true-1"],
    #["Streamsluice_trend_only", "streamsluice-scaleout-streamsluice_latency_only-streamsluice-120-400-600-500-120-5-0-1000-500-1000-100-true-1"],
    #["DS2", "streamsluice-scaleout-ds2-streamsluice-120-400-600-500-120-5-0-1000-500-1000-100-true-1"],
    #["DRS", "streamsluice-scaleout-drs-streamsluice-120-400-600-500-120-5-0-1000-500-1000-100-true-1"],
    #["Dhalion", "streamsluice-scaleout-ds2-streamsluice-120-400-600-500-120-5-0-1000-500-1000-100-true-1"],
]
windowSize = 1000
latencyLimit = 1000
endTime = 120
isSingleOperator = True
draw(rawDir, outputDir + exps[1][1] + "/", exps, windowSize)
