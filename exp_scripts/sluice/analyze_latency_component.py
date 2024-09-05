import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import math

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
                    tupleId = split[4].rstrip()
                    if tupleId not in groundTruthLatencyPerTuple:
                        groundTruthLatencyPerTuple[tupleId] = [arrivedTime, latency]
                    elif groundTruthLatencyPerTuple[tupleId][1] < latency:
                        groundTruthLatencyPerTuple[tupleId][1] = latency

        if (fileInitialTime > 0):
            fileInitialTimes[taskExecutor] = fileInitialTime
            if (initialTime == -1 or initialTime > fileInitialTime):
                initialTime = fileInitialTime
    print("FF: " + str(fileInitialTimes))
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



# Assuming we have the following data structure:
# Each n has 5 repeated experiments, and each experiment has 4 values:
# (avg_ground_truth_latency, max_ground_truth_latency, avg_arrival_to_deserialization, max_arrival_to_deserialization)

n_values = [1, 2, 4, 8, 16]

experiements_per_n = {
    1: [
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-1-222-5000-1000-3000-100-1-false-2",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-1-222-5000-1000-3000-100-1-false-3",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-1-222-5000-1000-3000-100-1-false-4",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-1-222-5000-1000-3000-100-1-false-5",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-1-222-5000-1000-3000-100-1-false-6",
    ],
    2: [
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-2-222-5000-1000-3000-100-1-false-1",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-2-222-5000-1000-3000-100-1-false-3",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-2-222-5000-1000-3000-100-1-false-4",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-2-222-5000-1000-3000-100-1-false-5",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-2-222-5000-1000-3000-100-1-false-6",
    ],
    4: [
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-4-222-5000-1000-3000-100-1-false-1",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-4-222-5000-1000-3000-100-1-false-3",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-4-222-5000-1000-3000-100-1-false-4",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-4-222-5000-1000-3000-100-1-false-5",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-4-222-5000-1000-3000-100-1-false-6",
    ],
    8: [
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-8-222-5000-1000-3000-100-1-false-1",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-8-222-5000-1000-3000-100-1-false-3",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-8-222-5000-1000-3000-100-1-false-4",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-8-222-5000-1000-3000-100-1-false-5",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-8-222-5000-1000-3000-100-1-false-6",
    ],
    16: [
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-16-222-5000-1000-3000-100-1-false-1",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-16-222-5000-1000-3000-100-1-false-3",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-16-222-5000-1000-3000-100-1-false-4",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-16-222-5000-1000-3000-100-1-false-5",
        "system-streamsluice-ds2-true-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-16-222-5000-1000-3000-100-1-false-6",
    ],
}

avg_ground_truth_latency = np.zeros(shape=(len(n_values), 5))
max_ground_truth_latency = np.zeros(shape=(len(n_values), 5))
avg_arrival_to_deserialization = np.zeros(shape=(len(n_values), 5))
max_arrival_to_deserialization = np.zeros(shape=(len(n_values), 5))

startTime = 30 #+300 #30
expLength = 120 #480 #480 #360

rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
for index in range(0, len(n_values)):
    for repeat in range(0, len(experiements_per_n[n_values[index]])):
        result = readGroundTruthLatencyByMetricsManager(rawDir, experiements_per_n[n_values[index]][repeat], 500)
        averageGroundTruthLatencies_FromMetricsManager_PerOperator = result[0]
        for operator_name, averageGroundTruthLatencies_FromMetricsManager in averageGroundTruthLatencies_FromMetricsManager_PerOperator.items():
            groundtruth_P99_MM_latency_in_range = [averageGroundTruthLatencies_FromMetricsManager[1][x] for x in
                                                   range(0, len(averageGroundTruthLatencies_FromMetricsManager[0])) if
                                                   averageGroundTruthLatencies_FromMetricsManager[0][
                                                       x] >= startTime * 1000 and
                                                   averageGroundTruthLatencies_FromMetricsManager[0][x] <= (
                                                           startTime + expLength) * 1000]
            groundtruth_P99_before_deserialization_in_range = [averageGroundTruthLatencies_FromMetricsManager[2][x] for
                                                               x
                                                               in
                                                               range(0, len(
                                                                   averageGroundTruthLatencies_FromMetricsManager[0]))
                                                               if
                                                               averageGroundTruthLatencies_FromMetricsManager[0][
                                                                   x] >= startTime * 1000 and
                                                               averageGroundTruthLatencies_FromMetricsManager[0][x] <= (
                                                                       startTime + expLength) * 1000]
            max_ground_truth_latency[index][repeat] = max(groundtruth_P99_MM_latency_in_range)
            avg_ground_truth_latency[index][repeat] = sum(groundtruth_P99_MM_latency_in_range) / len(groundtruth_P99_MM_latency_in_range)
            max_arrival_to_deserialization[index][repeat] = max(groundtruth_P99_before_deserialization_in_range)
            avg_arrival_to_deserialization[index][repeat] = sum(groundtruth_P99_before_deserialization_in_range) / len(
                    groundtruth_P99_before_deserialization_in_range)
            print(max_ground_truth_latency)
outputDir += experiements_per_n[n_values[0]][0] + "/"
# 1. Box Plot for Ground Truth Latency (Avg and Max)
fig, ax = plt.subplots(1, 2, figsize=(14, 6))

# Average Ground Truth Latency Box Plot
ax[0].boxplot([avg_ground_truth_latency[i] for i in range(len(n_values))], positions=n_values)
ax[0].set_title("Average Ground Truth Latency")
ax[0].set_xlabel("Number of Tasks (n)")
ax[0].set_ylabel("Latency (ms)")

# Max Ground Truth Latency Box Plot
ax[1].boxplot([max_ground_truth_latency[i] for i in range(len(n_values))], positions=n_values)
ax[1].set_title("Max Ground Truth Latency")
ax[1].set_xlabel("Number of Tasks (n)")
ax[1].set_ylabel("Latency (ms)")

plt.tight_layout()
import os
if not os.path.exists(outputDir):
    os.makedirs(outputDir)
# plt.savefig(outputDir + 'ground_truth_latency_curves.png', bbox_inches='tight')
plt.savefig(outputDir + '1.png', bbox_inches='tight')
plt.close(fig)

# 2. Box Plot for (Arrival - Deserialization) (Avg and Max)
fig, ax = plt.subplots(1, 2, figsize=(14, 6))

# Average (Arrival - Deserialization) Latency Box Plot
ax[0].boxplot([avg_arrival_to_deserialization[i] for i in range(len(n_values))], positions=n_values)
ax[0].set_title("Average (Arrival - Deserialization) Latency")
ax[0].set_xlabel("Number of Tasks (n)")
ax[0].set_ylabel("Latency (ms)")

# Max (Arrival - Deserialization) Latency Box Plot
ax[1].boxplot([max_arrival_to_deserialization[i] for i in range(len(n_values))], positions=n_values)
ax[1].set_title("Max (Arrival - Deserialization) Latency")
ax[1].set_xlabel("Number of Tasks (n)")
ax[1].set_ylabel("Latency (ms)")

plt.tight_layout()

import os
if not os.path.exists(outputDir):
    os.makedirs(outputDir)
# plt.savefig(outputDir + 'ground_truth_latency_curves.png', bbox_inches='tight')
plt.savefig(outputDir + '2.png', bbox_inches='tight')
plt.close(fig)

# 3. Line Plot for Trends Across `n`
fig, ax = plt.subplots(2, 1, figsize=(10, 12))

# Line plot for average and max ground truth latency
ax[0].errorbar(n_values, np.mean(avg_ground_truth_latency, axis=1), yerr=np.std(avg_ground_truth_latency, axis=1), label="Avg Ground Truth Latency", fmt='-o', color='blue')
ax[0].errorbar(n_values, np.mean(max_ground_truth_latency, axis=1), yerr=np.std(max_ground_truth_latency, axis=1), label="Max Ground Truth Latency", fmt='-o', color='red')
ax[0].set_title("Ground Truth Latency (Avg and Max) with Number of Tasks")
ax[0].set_xlabel("Number of Tasks (n)")
ax[0].set_ylabel("Latency (ms)")
ax[0].legend()

# Line plot for average and max (Arrival - Deserialization) latency
ax[1].errorbar(n_values, np.mean(avg_arrival_to_deserialization, axis=1), yerr=np.std(avg_arrival_to_deserialization, axis=1), label="Avg (Arrival - Deserialization)", fmt='-o', color='green')
ax[1].errorbar(n_values, np.mean(max_arrival_to_deserialization, axis=1), yerr=np.std(max_arrival_to_deserialization, axis=1), label="Max (Arrival - Deserialization)", fmt='-o', color='orange')
ax[1].set_title("(Arrival - Deserialization) Latency (Avg and Max) with Number of Tasks")
ax[1].set_xlabel("Number of Tasks (n)")
ax[1].set_ylabel("Latency (ms)")
ax[1].legend()

plt.tight_layout()

import os
if not os.path.exists(outputDir):
    os.makedirs(outputDir)
# plt.savefig(outputDir + 'ground_truth_latency_curves.png', bbox_inches='tight')
plt.savefig(outputDir + '3.png', bbox_inches='tight')
plt.close(fig)