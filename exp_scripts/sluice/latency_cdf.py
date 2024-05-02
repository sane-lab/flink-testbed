import sys
import numpy as np
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt

workload_index = {
    0: "Stock",
    1: "Twitter",
    2: "Linear_Road",
    3: "Spam_Detection",
}

workload_limit = {
    "Stock": 2000,
    "Twitter": 4000,
    "Linear_Road": 4000,
}

controller_index = {
    0: "DS2",
    1: "StreamSwitch",
    2: "Sluice",
}

controller_color = {
    0: "#fdae61",
    1: "#abdda4",
    2: "#2b83ba",
}

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

def extractLatency(raw_dir: str, exp: str):
    initialTime = -1
    lastTime = -1

    groundTruthLatencyPerTuple = {}
    groundTruthLatency = []

    taskExecutors = []  # "flink-samza-taskexecutor-0-eagle-sane.out"
    import os
    for file in os.listdir(raw_dir + exp + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("taskexecutor") == 1:
                taskExecutors += [file]
    fileInitialTimes = {}
    for taskExecutor in taskExecutors:
        groundTruthPath = raw_dir + exp + "/" + taskExecutor
        print("Reading ground truth file:" + groundTruthPath)
        fileInitialTime = - 1
        counter = 0
        with open(groundTruthPath) as f:
            lines = f.readlines()
            for i in range(0, len(lines)):
                line = lines[i]
                split = line.rstrip().split(",")
                counter += 1
                if (counter % 5000 == 0):
                    print("Processed to line:" + str(counter))
                if (split[0].startswith("GT:")):
                    completedTime = int(split[1].replace(" ", ""))
                    latency = int(split[2].replace(" ", ""))
                    arrivedTime = completedTime - latency
                    if (fileInitialTime == -1 or fileInitialTime > arrivedTime):
                        fileInitialTime = arrivedTime
                    if (completedTime > lastTime):
                        lastTime = completedTime
                    tupleId = split[3].rstrip()
                    if tupleId not in groundTruthLatencyPerTuple:
                        groundTruthLatencyPerTuple[tupleId] = [arrivedTime, latency]
                    elif groundTruthLatencyPerTuple[tupleId][1] < latency:
                        groundTruthLatencyPerTuple[tupleId][1] = latency
        fileInitialTimes[taskExecutor] = fileInitialTime
        if (initialTime == -1 or initialTime > fileInitialTime):
            initialTime = fileInitialTime
    print("FF: " + str(fileInitialTimes))
    for value in groundTruthLatencyPerTuple.values():
        groundTruthLatency += [value]

    streamsluiceOutput = "flink-samza-standalonesession-0-eagle-sane.out"
    import os
    for file in os.listdir(raw_dir + exp + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("standalonesession") == 1:
                streamsluiceOutput = file
    streamSluiceOutputPath = raw_dir + exp + "/" + streamsluiceOutput
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
                if (lastTime < estimateTime):
                    lastTime = estimateTime
    sorted_latency = []
    for pair in groundTruthLatency:
        index = int((pair[0] - initialTime) / windowSize)
        if index >= start_time * 1000 / windowSize and index <= end_time * 1000 / windowSize:
            sorted_latency += [pair[1]]
    sorted_latency.sort()
    return sorted_latency

def drawCDF(workload_label:str, latencys: dict):
    figName = "CDF_" + workload_label + ".jpg"
    fig, axs = plt.subplots(1, 1, figsize=(8, 4), layout='constrained')  # (24, 9)
    for controller in latencys.keys():
        latency = latencys[controller]
        plt.ecdf(latency, complementary=True, color=controller_color[controller], label=controller_index[controller])
    plt.legend()
    plt.xscale("log")
    plt.xlim(1, 100000)
    plt.ylim(0.001, 1.0)
    plt.yscale("log")
    plt.gca().set_yticks([1.0, 0.5, 0.2, 0.1, 0.05, 0.01, 0.001])
    plt.gca().invert_yaxis()
    plt.gca().set_yticklabels(1 - plt.gca().get_yticks())
    plt.plot([workload_limit[workload_label], workload_limit[workload_label]], [0.0, 1.0], color="red")
    plt.grid(True)
    plt.savefig(output_directory+figName)
output_directory = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/new/"
raw_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
exps = {
    "Stock": {
        0: "stock_analysis-ds2-ds2-3990-30-1000-20-2-500-6-5000-3-1000-4-3000-1-5-4000-2000-100-true-3-true-1",
        1: "stock_analysis-streamswitch-streamswitch-3990-30-1000-20-2-500-6-5000-3-1000-4-3000-1-5-4000-2000-100-true-3-true-5",
        2: "stock_analysis-streamsluice-streamsluice-3990-30-1000-20-2-500-6-5000-3-1000-4-3000-1-5-4000-2000-100-true-3-true-1",
    },
    "Twitter": {
        2: "tweet_alert-streamsluice-streamsluice-3990-30-1500-1-45-5000-25-3333-1-100-30-4000-4000-100-true-3-true-8",
    },
    "Linear_Road": {
        2: "linear_road-streamsluice-streamsluice-3990-30-1000-300-15-500-1-1000-15-500-1-100-15-500-55-2000-1-100---4000-100-true-3-true-2",
    },
}

start_time = 330
end_time = start_time + 3600
windowSize = 1000
for workload in workload_limit.keys():
    latencys = {}
    for controller in exps[workload]:
        latencys[controller] = extractLatency(raw_dir, exps[workload][controller])
    drawCDF(workload, latencys)
