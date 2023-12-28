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
    "all": "All",
}

def draw(rawDir, outputDir, expName):
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
    trueScalingTimes = []
    estimatedScalingTimes = []
    with open(streamSluiceOutputPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if (len(split) >= 5 and split[0] == "+++" and split[1] == "[MODEL]" and split[2] == "estimated" and split[3] == "spike:"):
                estimatedScalingTimes += [float(split[4])]
                trueScalingTimes += [float(split[7])]

    estimatedScalingTimes = estimatedScalingTimes[30:]
    trueScalingTimes = trueScalingTimes[30:]
    fig, axs = plt.subplots(1, 1, figsize=(15, 6), layout='constrained')
    ax1 = axs
    print("Draw estimated/true scaling time")
    x = np.arange(0, len(estimatedScalingTimes))
    legend = ["Ground Truth", "Estimated"]
    ax1.plot(x, trueScalingTimes, 'd', color="gray", markersize=4)
    ax1.plot(x, estimatedScalingTimes, 'o', color="blue", markersize=4)
    for index in range(0, len(estimatedScalingTimes)):
        x = [index, index]
        y = [trueScalingTimes[index], estimatedScalingTimes[index]]
        ax1.plot(x, y, '-', color="blue", linewidth=1)
    # width = 0.4
    # offset = 0
    # rects = ax1.bar(x + offset, spikesPerOperator[operator][0], width, label="Estimated Spike")
    # ax1.bar_label(rects, padding=3)
    # offset = width
    # rects = ax1.bar(x + offset, spikesPerOperator[operator][1], width, label="True Spike")
    # ax1.bar_label(rects, padding=3)
    ax1.set_ylabel('Scaling Time (ms)')
    ax1.set_xlabel('Scaling Index')
    ax1.set_title("Ground-Truth/Estimated Scaling Time")
    ax1.legend(legend, loc='upper left', ncol=2)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + "scaling_spike.png", bbox_inches='tight')
    plt.close(fig)


rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
import sys
expName = "microbench-workload-2op-3660-10000-10000-10000-5000-120-1-0-1-50-1-10000-12-1000-1-10000-4-357-1-10000-1000-500-100-true-1"
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]
draw(rawDir, outputDir + expName + "/", expName)