import math
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

def addLatencyLimitMarker(plt):
    x = [0, 10000000]
    y = [latencyLimit, latencyLimit]
    plt.plot(x, y, label="Limit", color='red', linewidth=1.5)
def addLatencyLimitWithSpikeMarker(plt):
    x = [0, 10000000]
    y = [latencyLimit + spike, latencyLimit + spike]
    plt.plot(x, y, color='orange', linewidth=1.5)
def readGroundTruthLatency(rawDir, expName, windowSize):
    initialTime = -1

    groundTruthLatencyPerTuple = {}
    groundTruthLatency = []

    taskExecutors = []  # "flink-samza-taskexecutor-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("taskexecutor") == 1:
                taskExecutors += [file]
    fileInitialTimes = {}
    for taskExecutor in taskExecutors:
        groundTruthPath = rawDir + expName + "/" + taskExecutor
        print("Reading ground truth file:" + groundTruthPath)
        fileInitialTime = - 1
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
                    if (fileInitialTime == -1 or fileInitialTime > arrivedTime):
                        fileInitialTime = arrivedTime
                    if(not isSingleOperator):
                        tupleId = split[4].rstrip()
                        if tupleId not in groundTruthLatencyPerTuple:
                            groundTruthLatencyPerTuple[tupleId] = [arrivedTime, latency]
                        elif groundTruthLatencyPerTuple[tupleId][1] < latency:
                            groundTruthLatencyPerTuple[tupleId][1] = latency
                    else:
                        groundTruthLatency += [[arrivedTime, latency]]
        if (fileInitialTime > 0):
            fileInitialTimes[taskExecutor] = fileInitialTime
            if (initialTime == -1 or initialTime > fileInitialTime):
                initialTime = fileInitialTime
    print("FF: " + str(fileInitialTimes))
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

    aggregatedGroundTruthLatency = {}
    for pair in groundTruthLatency:
        index = int((pair[0] - initialTime) / windowSize)
        if index not in aggregatedGroundTruthLatency:
            aggregatedGroundTruthLatency[index] = []
        aggregatedGroundTruthLatency[index] += [pair[1]]

    averageGroundTruthLatency = [[], [], []]
    for index in sorted(aggregatedGroundTruthLatency):
        time = index * windowSize
        x = int(time)
        if index in aggregatedGroundTruthLatency:
            sortedLatency = sorted(aggregatedGroundTruthLatency[index])
            size = len(sortedLatency)
            # P99 latency
            target = min(math.ceil(size * 0.99), size) - 1
            y = sortedLatency[target]
            averageGroundTruthLatency[0] += [x]
            averageGroundTruthLatency[1] += [y]
            y = sum(sortedLatency)/size
            averageGroundTruthLatency[2] += [y]

    return [averageGroundTruthLatency, initialTime]

def readGroundTruthLatencyByMetricsManager(rawDir, expName, windowSize):
    initialTime = -1
    taskExecutors = []  # "flink-samza-taskexecutor-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("taskexecutor") == 1:
                taskExecutors += [file]
    fileInitialTimes = {}
    groundTruthLatency = {}
    for taskExecutor in taskExecutors:
        groundTruthPath = rawDir + expName + "/" + taskExecutor
        print("Reading ground truth file:" + groundTruthPath)
        fileInitialTime = - 1
        counter = 0
        with open(groundTruthPath) as f:
            lines = f.readlines()
            for i in range(0, len(lines)):
                line = lines[i]
                split = line.rstrip().split()
                counter += 1
                if (counter % 5000 == 0):
                    print("Processed to line:" + str(counter))
                # if line.startswith("GroundTruth"):
                #     try:
                #         # Extract fields from the line
                #         keygroup = int(split[3])  # keygroup: 40 -> 40
                #         arrival_ts = int(split[5])  # arrival_ts: 1725453549506 -> 1725453549506
                #         completion_ts = int(split[7])  # completion_ts: 1725453549625 -> 1725453549625
                #         if (fileInitialTime == -1 or fileInitialTime > arrival_ts):
                #             fileInitialTime = arrival_ts
                #         # Calculate ground truth latency
                #         latency = completion_ts - arrival_ts
                #         groundTruthLatency.append([arrival_ts, latency])
                if line.startswith("tupletime"):
                    try:
                        operator_name = "op-" + split[2].split("-")[0]
                        kg_index = 2
                        while(split[kg_index] != "keygroup:"):
                            kg_index += 1
                        # Extract fields from the line
                        keygroup = int(split[kg_index + 1])  # keygroup: 40 -> 40
                        arrival_ts = int(split[kg_index + 3])  # arrival_ts: 1725453549506 -> 1725453549506
                        deserialization_start_ts = int(split[kg_index + 5])
                        deserialization_over_ts = int(split[kg_index + 7])
                        completion_ts = int(split[kg_index + 9])  # completion_ts: 1725453549625 -> 1725453549625
                        if (fileInitialTime == -1 or fileInitialTime > arrival_ts):
                            fileInitialTime = arrival_ts
                        # Calculate ground truth latency
                        latency = completion_ts - arrival_ts
                        if operator_name not in groundTruthLatency:
                            groundTruthLatency[operator_name] = []
                        groundTruthLatency[operator_name].append([arrival_ts, latency, deserialization_start_ts - arrival_ts, deserialization_over_ts - deserialization_start_ts, completion_ts - deserialization_over_ts])

                    except Exception as e:
                        print(f"Error parsing line {i + 1}: {e}")
                        continue
        if (fileInitialTime > 0):
            fileInitialTimes[taskExecutor] = fileInitialTime
            if (initialTime == -1 or initialTime > fileInitialTime):
                initialTime = fileInitialTime
    averageGroundTruthLatency_PerOperator = {}
    for operator_name, groundTruthLatency in groundTruthLatency.items():
        aggregatedGroundTruthLatency = {}
        for pair in groundTruthLatency:
            index = int((pair[0] - initialTime) / windowSize)
            if index not in aggregatedGroundTruthLatency:
                aggregatedGroundTruthLatency[index] = []
            aggregatedGroundTruthLatency[index] += [(pair[1], pair[2], pair[3], pair[4])]

        averageGroundTruthLatency = [[], [], [], [], []]
        for index in sorted(aggregatedGroundTruthLatency):
            time = index * windowSize
            x = int(time)
            if index in aggregatedGroundTruthLatency:
                sortedLatency = sorted(aggregatedGroundTruthLatency[index])
                size = len(sortedLatency)
                # P99 latency
                target = min(math.ceil(size * 0.99), size) - 1
                y = sortedLatency[target][0]
                averageGroundTruthLatency[0] += [x]
                averageGroundTruthLatency[1] += [y]
                averageGroundTruthLatency[2] += [sortedLatency[target][1]]
                averageGroundTruthLatency[3] += [sortedLatency[target][2]]
                averageGroundTruthLatency[4] += [sortedLatency[target][3]]
        averageGroundTruthLatency_PerOperator[operator_name] = averageGroundTruthLatency

    return [averageGroundTruthLatency_PerOperator, initialTime]


def readLEMLatencyAndSpike(rawDir, expName) -> [list[int], list[float], list[float]]:

    lem_latency = [[], [], []]

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
            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[MODEL]" and split[6] == "cur_ete_l:" and split[
                8] == "n_epoch_l:"):
                time = int(split[3])
                estimated_l = float(split[7])
                estimated_spike = float(split[13]) - float(split[7])
                lem_latency[0] += [time]
                lem_latency[1] += [estimated_l]
                lem_latency[2] += [estimated_spike]
    return lem_latency

def draw(rawDir, outputDir, exps, windowSize):
    averageGroundTruthLatencies = []
    averageGroundTruthLatencies_FromMetricsManager_PerOperator = []
    lem_latencies = []
    initial_times = []
    for i in range(0, len(exps)):
        expFile = exps[i][1]
        result = readGroundTruthLatency(rawDir, expFile, windowSize)
        averageGroundTruthLatencies += [result[0]]
        initial_times += [result[1]]
        result = readGroundTruthLatencyByMetricsManager(rawDir, expFile, windowSize)
        averageGroundTruthLatencies_FromMetricsManager_PerOperator += [result[0]]
        result = readLEMLatencyAndSpike(rawDir, expFile)
        result[0] = [x - initial_times[i] for x in result[0]]
        lem_latencies += [result]
    # print("+++ " + str(averageGroundTruthLatencies))



    successRatePerExps = {}
    for i in range(0, len(exps)):
        totalSuccess = len([x for x in range(0, len(averageGroundTruthLatencies[i][0])) if
                              averageGroundTruthLatencies[i][0][x] >= startTime * 1000 and
                              averageGroundTruthLatencies[i][0][x] <= (startTime + 1800) * 1000 and averageGroundTruthLatencies[i][1][x] <= latencyLimit])
        totalWindows = len([x for x in range(0, len(averageGroundTruthLatencies[i][0])) if
                              averageGroundTruthLatencies[i][0][x] >= startTime * 1000 and
                              averageGroundTruthLatencies[i][0][x] <= (startTime + 1800) * 1000])
        successRatePerExps[exps[i][0]] = totalSuccess / float(totalWindows)

        groundtruth_P99_latency_in_range = [averageGroundTruthLatencies[i][1][x] for x in range(0, len(averageGroundTruthLatencies[i][0])) if
                              averageGroundTruthLatencies[i][0][x] >= startTime * 1000 and averageGroundTruthLatencies[i][0][x] <= (startTime + avg_latency_calculateTime) * 1000]
        lem_latency_in_range = [lem_latencies[i][1][x] for x in range(0, len(lem_latencies[i][0])) if
                              lem_latencies[i][0][x] >= startTime * 1000 and lem_latencies[i][0][x] <= (startTime + avg_latency_calculateTime) * 1000]
        print("in range ground truth P99 latency max:" + str(max(groundtruth_P99_latency_in_range)) + " avg: " + str(sum(groundtruth_P99_latency_in_range)/len(groundtruth_P99_latency_in_range)))
        print("in range lem latency max:" + str(max(lem_latency_in_range)) + " avg: " + str(
            sum(lem_latency_in_range) / len(lem_latency_in_range)))
        for operator_name, averageGroundTruthLatencies_FromMetricsManager in averageGroundTruthLatencies_FromMetricsManager_PerOperator[i].items():
            groundtruth_P99_MM_latency_in_range = [averageGroundTruthLatencies_FromMetricsManager[1][x] for x in
                                               range(0, len(averageGroundTruthLatencies_FromMetricsManager[0])) if
                                               averageGroundTruthLatencies_FromMetricsManager[0][
                                                   x] >= startTime * 1000 and
                                               averageGroundTruthLatencies_FromMetricsManager[0][x] <= (
                                                       startTime + avg_latency_calculateTime) * 1000]
            groundtruth_P99_before_deserialization_in_range = [averageGroundTruthLatencies_FromMetricsManager[2][x] for x
                                                           in
                                                           range(0, len(
                                                               averageGroundTruthLatencies_FromMetricsManager[0])) if
                                                           averageGroundTruthLatencies_FromMetricsManager[0][
                                                               x] >= startTime * 1000 and
                                                           averageGroundTruthLatencies_FromMetricsManager[0][x] <= (
                                                                   startTime + avg_latency_calculateTime) * 1000]
            print("in range operator " + operator_name + " MM latency max:" + str(max(groundtruth_P99_MM_latency_in_range)) + " avg: " + str(
                sum(groundtruth_P99_MM_latency_in_range) / len(groundtruth_P99_MM_latency_in_range)))
            print("in range operator " + operator_name + " (Arrival - DeserializeStart) max:" + str(
                max(groundtruth_P99_before_deserialization_in_range)) + " avg: " + str(
                sum(groundtruth_P99_before_deserialization_in_range) / len(
                    groundtruth_P99_before_deserialization_in_range)))

    print("1800 seconds success rate")
    print(successRatePerExps)
    #print(averageGroundTruthLatencies)
    #fig = plt.figure(figsize=(24, 3))
    fig, ax = plt.subplots(figsize=(12, 5))
    print("Draw ground truth curve...")
    for i in range(0, len(exps)):
        averageGroundTruthLatency = averageGroundTruthLatencies[i]

        sample_factor = 1 #5
        sampledLatency = [[], [], []]
        sampledLatency[0] = [averageGroundTruthLatency[0][i] for i in range(0, len(averageGroundTruthLatency[0]), sample_factor)]
        sampledLatency[1] = [max([averageGroundTruthLatency[1][y] for y in range(x, min(x + sample_factor, len(averageGroundTruthLatency[1])))]) for x in range(0, len(averageGroundTruthLatency[0]), sample_factor)]
        sampledLatency[2] = [max([averageGroundTruthLatency[2][y] for y in
                                  range(x, min(x + sample_factor, len(averageGroundTruthLatency[2])))]) for x in
                             range(0, len(averageGroundTruthLatency[0]), sample_factor)]

        #plt.plot(averageGroundTruthLatency[0], averageGroundTruthLatency[1], 'o-', color=exps[i][2], markersize=2, linewidth=2)
        if exps[i][0] == 'Sluice':
            linewidth = 3
        else:
            linewidth = 3 / 2.0
        plt.plot(sampledLatency[0], sampledLatency[1], '-', color=exps[i][2], markersize=4,
                 linewidth=linewidth * 2, label="Ground Truth P99") #exps[i][0])
        # plt.plot(averageGroundTruthLatencies_FromMetricsManager[i][0], averageGroundTruthLatencies_FromMetricsManager[i][1], '-', color="orange", markersize=4,
        #         linewidth=1, label="Ground Truth P99 Metrics Manager")
        averageGroundTruthLatencies_FromMetricsManager = averageGroundTruthLatencies_FromMetricsManager_PerOperator[i]["op-2"]
        print(averageGroundTruthLatencies_FromMetricsManager_PerOperator[i])
        y = np.vstack(
            [averageGroundTruthLatencies_FromMetricsManager[2], averageGroundTruthLatencies_FromMetricsManager[3],
             averageGroundTruthLatencies_FromMetricsManager[4]])
        ax.stackplot(averageGroundTruthLatencies_FromMetricsManager[0],
                     y,
                     colors=['orange', 'purple', 'green'],
                     labels=["Arrival - Deserialize Start", "Deserialize", "Processing"])
        # plt.plot(averageGroundTruthLatencies_FromMetricsManager[i][0],
        #          averageGroundTruthLatencies_FromMetricsManager[i][2], '-', color="orange", markersize=4,
        #          linewidth=1.5, label="P99 (Deserialize start - Arrival)")
        # plt.plot(averageGroundTruthLatencies_FromMetricsManager[i][0],
        #          averageGroundTruthLatencies_FromMetricsManager[i][3], '-', color="brown", markersize=4,
        #          linewidth=1.5, label="P99 (Deserialize)")
        # plt.plot(averageGroundTruthLatencies_FromMetricsManager[i][0],
        #          averageGroundTruthLatencies_FromMetricsManager[i][4], '-', color="purple", markersize=4,
        #          linewidth=1.5, label="P99 (Processing)")
        if (show_avg_flag):
            plt.plot(sampledLatency[0], sampledLatency[2], '-', color="orange", markersize=4,
                     linewidth=linewidth, label="Ground Truth Average")
        #plt.plot(lem_latencies[i][0], lem_latencies[i][1], 'o', color="green", markersize=2, linewidth=linewidth, label='Estimated Latency')
        # plt.plot([x - initial_times[i] for x in lem_latencies[i][0]], [lem_latencies[i][1][x] + lem_latencies[i][2][x] for x in range(0, len(lem_latencies[i][1]))], 'd', color="gray", markersize=2, linewidth=linewidth)
    addLatencyLimitMarker(plt)
    # legend += ["Limit + Spike"]
    # addLatencyLimitWithSpikeMarker(plt)
    # plt.legend(legend, bbox_to_anchor=(0.45, 1.3), loc='upper center', ncol=4, markerscale=4.)  # When
    # plt.legend(legend, bbox_to_anchor=(0.45, 1.3), loc='upper center', ncol=3, markerscale=4.)  # How1
    plt.legend(bbox_to_anchor=(0.45, 1.4), loc='upper center', ncol=3, markerscale=4.) # How2
    #plt.xlabel('Time (min)')
    plt.ylabel('Latency (ms)')
    #plt.title('Latency Curves')
    #axes.set_ylim(0, 5000)
    axes = plt.gca()
    axes.set_xlim(startTime * 1000, (startTime + expLength) * 1000)
    axes.set_xticks(np.arange(startTime * 1000, (startTime + expLength) * 1000 + 60000, 60000))
    axes.set_xticklabels([int((x - startTime * 1000) / 1000) for x in np.arange(startTime * 1000, (startTime + expLength) * 1000 + 60000, 60000)])
    #axes.set_yticks(np.arange(0, 6000, 1000))
    # axes.set_ylim(0, 5000)
    # axes.set_yticks(np.arange(0, 6250, 1250))
    # axes.set_ylim(0, 8000)
    # axes.set_yticks(np.arange(0, 8500, 500))
    axes.set_ylim(-1, 300)
    axes.set_yticks(np.arange(0, 325, 25))
    if trickFlag:
        axes.set_yticklabels([int(x / 1250 * 1000) for x in np.arange(0, 6250, 1250)])
    # axes.set_yscale('log')
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    #plt.savefig(outputDir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.savefig(outputDir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.close(fig)


    # Print per operator
    for i in range(0, len(exps)):
        operator_num = len(averageGroundTruthLatencies_FromMetricsManager_PerOperator[i].keys())
        fig, axs = plt.subplots(1, operator_num, figsize=(10 * operator_num, 5))
        index = 0
        for operator, averageGroundTruthLatencies_FromMetricsManager in averageGroundTruthLatencies_FromMetricsManager_PerOperator[i].items():
            if operator_num > 1:
                ax = axs[index]
            else:
                ax = axs
            y = np.vstack(
                [averageGroundTruthLatencies_FromMetricsManager[2], averageGroundTruthLatencies_FromMetricsManager[3],
                 averageGroundTruthLatencies_FromMetricsManager[4]])
            ax.stackplot(averageGroundTruthLatencies_FromMetricsManager[0],
                         y,
                         colors=['orange', 'purple', 'green'],
                         labels=["Arrival - Deserialize Start", "Deserialize", "Processing"])
            axes = ax
            axes.set_xlim(startTime * 1000, (startTime + expLength) * 1000)
            axes.set_xticks(np.arange(startTime * 1000, (startTime + expLength) * 1000 + 60000, 60000))
            axes.set_xticklabels([int((x - startTime * 1000) / 1000) for x in
                                  np.arange(startTime * 1000, (startTime + expLength) * 1000 + 60000, 60000)])
            axes.set_ylim(-1, 300)
            axes.set_yticks(np.arange(0, 325, 25))
            ax.set_ylabel(operator + ' latency (ms)')
            ax.grid(True)
            if index == 0:
                ax.legend()
            index += 1
        import os
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)
        # plt.savefig(outputDir + 'ground_truth_latency_curves.png', bbox_inches='tight')
        plt.savefig(outputDir + 'operator_latency_component.png', bbox_inches='tight')
        plt.close(fig)
rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
exps = [
    # ["Earlier",
    #  "systemsensitivity-streamsluice_earlier-streamsluice-when-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
    #  "green", "o"],
    # ["Later",
    #  "systemsensitivity-streamsluice_later-streamsluice-when-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
    #  "orange", "o"],
    # ["Sluice",
    #  "systemsensitivity-streamsluice-streamsluice-when-1split2join1-400-6000-3000-4000-1-0-2-300-1-10000-2-300-1-10000-2-300-1-10000-6-510-10000-2500-3000-100-10-true-1",
    #  "blue", "o"],
    ["GroundTruth",
      #"systemsensitivity-streamsluice-streamsluice-when-1split2join1-400-6000-3000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-6-510-5000-2000-3000-100-10-true-1",
     "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-1-333-5000-1000-3000-100-1-false-1",
      "blue", "o"],


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
]


import sys
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]

overall_latency = {}

windowSize = 500 #500
latencyLimit = 50000
spike = 2500 #1500
#latencyLimit = 2500 #1000
startTime = 30 #+300 #30
expLength = 120 #480 #480 #360
show_avg_flag = False

avg_latency_calculateTime = expLength

isSingleOperator = False #True
expName = exps[0][1]
print(expName)
trickFlag = False #True
draw(rawDir, outputDir + expName + "/", exps, windowSize)

