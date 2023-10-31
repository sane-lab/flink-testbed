import math
import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def retrieveLatencySpikeInfoFromFile(path):
    import os
    streamsluiceOutput = "flink-samza-standalonesession-0-eagle-sane.out"
    import os
    for file in os.listdir(path + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("standalonesession") == 1:
                streamsluiceOutput = file
    streamSluiceOutputPath = path+ "/" + streamsluiceOutput
    print("Reading streamsluice output:" + streamSluiceOutputPath)
    if (not os.path.isfile(streamSluiceOutputPath)):
        print("File not exisis")
        return ["error", path]

    counter = 0
    scale_index = 0
    scalingInfos = []
    with open(streamSluiceOutputPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if (len(split) >= 7 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "decides" and split[8] == "scale" and split[9] == "random"):
                scale_index += 1
                scalingInfo = []
            if (len(split) >= 7 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale." and split[8] == "operator:"):
                scalingInfo += [split[9], int(split[13]), int(split[17]), split[21]]
            if (len(split) >= 7 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale." and split[8] == "ete:"):
                scalingInfo += [float(split[9])]
            if (len(split) >= 7 and split[0] == "+++" and split[1] == "[CONTROL]" and split[4] == "all" and split[7] == "deployed."):
                scalingInfo += [int(split[10])]
                scalingInfos += [scalingInfo]
                scalingInfo = []
    return scalingInfos

def drawLatencySpikeOnEteLatency(scalingInfos):
    fig = plt.figure(figsize=(12, 9))
    legend = []
    for setting in scalingInfos.keys():
        xs = [scalingInfo[4] for scalingInfo in scalingInfos[setting]]
        ys = [scalingInfo[5] for scalingInfo in scalingInfos[setting]]
        print("Draw scaling spike vs latency curve for statesize" + setting)
        legend += ["StateSize" + setting]
        plt.plot(xs, ys, 'D', markersize=1)
        plt.plot()
    plt.legend(legend, loc='upper left')
    plt.xlabel('Latency (ms)')
    plt.ylabel('Latency Spike (ms)')
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + 'spike_vs_latency.png', bbox_inches='tight')
    #plt.savefig(outputDir + 'spike_vs_latency.pdf', bbox_inches='tight')
    plt.close(fig)

scalingInfos = {}
import os
rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/spikes/"
#rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/spikes/"
errorFiles = []
for fdir in os.listdir(rawDir):
    if fdir.startswith("data_") and not fdir.endswith("_old"):
        for dir in os.listdir(rawDir + fdir + '/'):
            if dir.startswith("random_"):
                state_size = dir.split("-")[-2]
                movekey_num = dir.split("_")[2]
                setting_name = state_size + "_K" + movekey_num
                if (movekey_num == "20"): # if(state_size == "50000"): #
                    result = retrieveLatencySpikeInfoFromFile(rawDir + fdir + '/' + dir)
                    if len(result) == 2 and result[0] == "error":
                        errorFiles += [result[1]]
                        continue
                    if setting_name  not in scalingInfos:
                        scalingInfos[setting_name] = []
                    scalingInfos[setting_name] += result

print(errorFiles)
#print(scalingInfos.values())
# 0: operator name, 1: moved keys, 2: new tasks, 3: removed tasks, 4: ete l, 5: spike
drawLatencySpikeOnEteLatency(scalingInfos)