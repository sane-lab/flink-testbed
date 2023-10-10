import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd


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

def getResults(rawDir, expName):
    initialTime = -1
    lastTime = 0
    arrivalRatePerKey = {}
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

    return [arrivalRatePerKey,initialTime, lastTime]

def draw(rawDir, outputDir, expName):
    arrivalRatePerKeyPerOperator,initialTime, lastTime = getResults(rawDir, expName)
    arrivalRatePerKey = arrivalRatePerKeyPerOperator[firstOP]
    for i in range(0, 8):
        print("Draw " + str(i) + "-th figure...")
        figName = "keyArrival_" + str(i) + ".png"
        fig = plt.figure(figsize=(16, 48))
        for j in range(0, 16):
            key = i * 16 + j
            print("Draw key " + str(key) + " figure...")
            ax1 = plt.subplot(16, 1, j + 1)
            print("Draw arrival rates")
            ax1.plot(arrivalRatePerKey[key][0], arrivalRatePerKey[key][1], '*', color='red', markersize=MARKERSIZE,
                     label="Arrival Rate")
            ax1.set_xlabel('Time (s)')
            ax1.set_ylabel('Rate (tps)')
            plt.title('Arrival Rates of ' + str(key))
            ax1.set_xlim(0, lastTime - initialTime)
            ax1.set_xticks(np.arange(0, lastTime - initialTime, 10000))
            xlabels = []
            for x in range(0, lastTime - initialTime, 10000):
                xlabels += [str(int(x / 1000))]
            ax1.set_xticklabels(xlabels)
            ylim = 100
            ax1.set_ylim(1, ylim)
            ax1.set_yticks(np.arange(0, ylim, ylim / 5))
            plt.grid(True)
        import os
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)
        plt.savefig(outputDir + figName)
        plt.close(fig)

rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
#expName = "streamsluice-scaletest-400-600-500-5-2000-1000-100-1"
expName = "streamsluice-4op-120-8000-12000-10000-240-1-0-2-200-1-100-5-500-1-100-3-333-1-100-3-250-100-1000-500-100-true-1"
firstOP = "a84740bacf923e828852cc4966f2247c"
draw(rawDir, outputDir + expName + "/", expName)
