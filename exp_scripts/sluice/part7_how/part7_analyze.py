import math

import matplotlib
import numpy as np

matplotlib.use('Agg')
import matplotlib.pyplot as plt
def supylabel2(fig, s, **kwargs):
    defaults = {
        "x": 0.98,
        "y": 0.5,
        "horizontalalignment": "center",
        "verticalalignment": "center",
        "rotation": "vertical",
        "rotation_mode": "anchor",
        "size": plt.rcParams["figure.labelsize"],  # matplotlib >= 3.6
        "weight": plt.rcParams["figure.labelweight"],  # matplotlib >= 3.6
    }
    kwargs["s"] = s
    # kwargs = defaults | kwargs  # python >= 3.9
    kwargs = {**defaults, **kwargs}
    fig.text(**kwargs)

OPERATOR_NAMING = {
    "0a448493b4782967b150582570326227": "Stateful Map",
    "c21234bcbf1e8eb4c61f1927190efebd": "Splitter",
    "22359d48bcb33236cf1e31888091e54c": "Counter",
    "a84740bacf923e828852cc4966f2247c": "OP2",
    "eabd4c11f6c6fbdf011f0f1fc42097b1": "OP3",
    "d01047f852abd5702a0dabeedac99ff5": "OP4",
    "d2336f79a0d60b5a4b16c8769ec82e47": "OP5",
    "36fcfcb61a35d065e60ee34fccb0541a": "OP6",
    "c395b989724fa728d0a2640c6ccdb8a1": "OP7",
    "TOTAL": "TOTAL",
}
COLOR = {
    "TOTAL": "red",
    "Stateful Map": "red",
    "Splitter": "blue",
    "Counter": "green",
    "OP2": "blue",
    "OP3": "green",
    "OP4": "purple",
    "OP5": "orange",
}

#APP_NAMING = ["TF", "PA", "VA", "TA"]
APP_NAMING = ["PP", "PA", "VF", "VA", "Join", "AN"]

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
def addLatencyLimitMarker(plt):
    x = [0, 10000000]
    y = [latencyLimit, latencyLimit]
    plt.plot(x, y, "--", label="Limit", color='red', linewidth=1.5)
def addLatencyLimitWithSpikeMarker(plt):
    x = [0, 10000000]
    y = [latencyLimit + spike, latencyLimit + spike]
    plt.plot(x, y, color='orange', linewidth=1.5)

def parsePerTaskValue(splits):
    taskValues = {}
    for split in splits:
        split = split.lstrip("{").rstrip("}").rstrip(",")
        words = split.split("=")
        taskName = words[0]
        value = float(words[1])
        taskValues[taskName] = value
    return taskValues

def parse_key_metrics(s):
    # Remove outer curly braces and split into individual dictionaries
    outer_dict = {}
    s = s.strip('{}')
    pairs = s.split('},')
    for pair in pairs:
        key, inner_str = pair.split('={', 1)
        inner_str = inner_str.rstrip('}')

        # Split the inner string into key-value pairs
        inner_pairs = inner_str.split(',')
        inner_dict = {int(k): float(v) for k, v in (item.split('=') for item in inner_pairs)}

        outer_dict[key] = inner_dict
    return outer_dict


def parseMapping(split):
    mapping = {}
    for word in split:
        word = word.lstrip("{").rstrip("}")
        if "=" in word:
            if word.count("=") == 1:
                x = word.split("=")
                job = x[0].split("_")[0]
                task = x[0]
                key = x[1].lstrip("[").rstrip(",").rstrip("}").rstrip("]")
            else:
                x = word.split("=")
                job = x[0].lstrip("{")
                task = x[1].lstrip("{")
                key = x[2].lstrip("[").rstrip(",").rstrip("}").rstrip("]")
            if job not in mapping:
                mapping[job] = {}
            mapping[job][task] = [key]
        else:
            key = word.rstrip(",").rstrip("}").rstrip("]")
            mapping[job][task] += [key]
    return mapping


def readParallelism(rawDir, expName):
    initialTime = -1
    lastTime = 0
    arrivalRatePerTask = {}
    ParallelismPerJob = {}
    scalingMarkerByOperator = {}
    scalings = []

    taskExecutors = [] #"flink-samza-taskexecutor-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("taskexecutor") == 1:
                taskExecutors += [file]
    for taskExecutor in taskExecutors:
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
                    if (arrivedTime < 0):
                        print("!!!! " + str(i) + "  " + line)
                    if (initialTime == -1 or initialTime > arrivedTime):
                        initialTime = arrivedTime
                    if (lastTime < completedTime):
                        lastTime = completedTime
    print("init time=" + str(initialTime) + " last time=" + str(lastTime))

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

    scalings_change_info = []
    key_arrival_per_operator = {}
    key_backlog_per_operator = {}
    current_scaling_info = []


    with open(streamSluiceOutputPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))

            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "decides" and split[8] == "scale" and split[9] == "out."):
                time = int(split[3]) - initialTime
                if time >= startTime * 1000 and time <= (startTime + expLength) * 1000:
                    current_scaling_info = [time]

            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale" and split[
                8] == "operator:"):
                time = int(split[3])
                if (split[7] == "in"):
                    type = 1
                elif (split[7] == "out"):
                    type = 2

                lastScalingOperators = [split[9].lstrip('[').rstrip(']')]
                for operator in lastScalingOperators:
                    if (operator not in scalingMarkerByOperator):
                        scalingMarkerByOperator[operator] = []
                    scalingMarkerByOperator[operator] += [[time - initialTime, type]]
                mapping = parseMapping(split[12:])
                scalings.append(time - initialTime)

                if len(current_scaling_info) == 1:
                    after_scale_mapping = mapping[lastScalingOperators[0]]
                    current_scaling_info.append(lastScalingOperators[0])
                    def convert_map(input_dict:dict[str, list[str]]):
                        output_dict = {key: list(map(int, value)) for key, value in input_dict.items()}
                        return output_dict
                    current_scaling_info.append(convert_map(before_scale_config[lastScalingOperators[0]]))
                    current_scaling_info.append(convert_map(after_scale_mapping))
                    current_scaling_info.append(key_arrival_per_operator[lastScalingOperators[0]])
                    current_scaling_info.append(key_backlog_per_operator[lastScalingOperators[0]])

                    scalings_change_info.append(current_scaling_info)
                    current_scaling_info = []

            if (len(split) >= 8 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "current" and split[7] == "config:"):
                before_scale_config = parseMapping(split[8:])

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
                for job in mapping:
                    ParallelismPerJob[job][0].append(time - initialTime)
                    ParallelismPerJob[job][1].append(len(mapping[job].keys()))


            if (split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "backlog:"):
                time = int(split[3])
                backlogs = parsePerTaskValue(split[6:])
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

            if (split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "arrivalRate:"):
                time = int(split[3])
                # if (time > lastTime):
                #   continue
                arrivalRates = parsePerTaskValue(split[6:])
                for task in arrivalRates:
                    if task not in arrivalRatePerTask:
                        arrivalRatePerTask[task] = [[], []]
                    import math
                    if not math.isnan(arrivalRates[task]) and not math.isinf(arrivalRates[task]):
                        arrivalRatePerTask[task][0] += [time - initialTime]
                        arrivalRatePerTask[task][1] += [int(arrivalRates[task] * 1000)]

            if (len(split) >= 6 and split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "key" and split[5] == "arrivalRate:"):
                time = int(split[3])
                key_arrival_per_operator = parse_key_metrics(''.join(split[6:]).strip())

            if (len(split) >= 6 and split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "key" and split[
                5] == "backlog:"):
                time = int(split[3])
                key_backlog_per_operator = parse_key_metrics(''.join(split[6:]).strip())







    ParallelismPerJob["TOTAL"] = [[], []]
    for job in ParallelismPerJob:
        if job != "TOTAL":
            for i in range(0, len(ParallelismPerJob[job][0])):
                if i >= len(ParallelismPerJob["TOTAL"][0]):
                    if weightedTotalParallelismFlag:
                        ParallelismPerJob["TOTAL"][0].append(
                            parallelismWeight[OPERATOR_NAMING[job]] * ParallelismPerJob[job][0][i])
                        ParallelismPerJob["TOTAL"][1].append(
                            parallelismWeight[OPERATOR_NAMING[job]] * ParallelismPerJob[job][1][i])
                    else:
                        ParallelismPerJob["TOTAL"][0].append(ParallelismPerJob[job][0][i])
                        ParallelismPerJob["TOTAL"][1].append(ParallelismPerJob[job][1][i])
                else:
                    if weightedTotalParallelismFlag:
                        ParallelismPerJob["TOTAL"][1][i] += parallelismWeight[OPERATOR_NAMING[job]] * \
                                                            ParallelismPerJob[job][1][i]
                    else:
                        ParallelismPerJob["TOTAL"][1][i] += ParallelismPerJob[job][1][i]
    print(ParallelismPerJob)

    totalArrivalRatePerJob = {}
    for task in arrivalRatePerTask:
        job = task.split("_")[0]
        n = len(arrivalRatePerTask[task][0])
        if job not in totalArrivalRatePerJob:
            totalArrivalRatePerJob[job] = {}
        for i in range(0, n):
            ax = arrivalRatePerTask[task][0][i]
            index = math.floor(ax / windowSize) * windowSize
            ay = arrivalRatePerTask[task][1][i]
            if index not in totalArrivalRatePerJob[job]:
                totalArrivalRatePerJob[job][index] = ay
            else:
                totalArrivalRatePerJob[job][index] += ay
    print(expName, ParallelismPerJob.keys())
    return [ParallelismPerJob, totalArrivalRatePerJob, initialTime, scalings, scalings_change_info]

def calculate_latency_limits(p99_latencies) -> [int, int]:
    # Filter the P99 latencies to only include those within the given time range
    # filtered_latencies = [latency[1] for latency in p99_latencies if start_time <= latency[0] <= end_time]
    filtered_latencies = p99_latencies
    if not filtered_latencies:
        raise ValueError("No latencies found in the specified time range.")

    # Sort the latencies in ascending order
    sorted_latencies = sorted(filtered_latencies)

    # Find the latency limit for 95% success rate (P95)
    target_p95_index = int(len(sorted_latencies) * 0.95) - 1
    latency_limit_95 = sorted_latencies[target_p95_index]

    # Find the latency limit for 99% success rate (P99)
    target_p99_index = int(len(sorted_latencies) * 0.99) - 1
    latency_limit_99 = sorted_latencies[target_p99_index]

    return latency_limit_95, latency_limit_99
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
                        process_start_ts = int(split[kg_index + 5])
                        completion_ts = int(split[kg_index + 7])  # completion_ts: 1725453549625 -> 1725453549625
                        if (fileInitialTime == -1 or fileInitialTime > arrival_ts):
                            fileInitialTime = arrival_ts
                        # Calculate ground truth latency
                        latency = completion_ts - arrival_ts
                        if operator_name not in groundTruthLatency:
                            groundTruthLatency[operator_name] = []
                        groundTruthLatency[operator_name].append([arrival_ts, latency, process_start_ts - arrival_ts, completion_ts - process_start_ts])

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
            aggregatedGroundTruthLatency[index] += [(pair[1], pair[2], pair[3])]

        averageGroundTruthLatency = [[], [], [], []]
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
        averageGroundTruthLatency_PerOperator[operator_name] = averageGroundTruthLatency
    return [averageGroundTruthLatency_PerOperator, initialTime]


def readLEMLatencyAndSpike(rawDir, expName) -> [list[int], list[float], list[float]]:

    lem_latency = [[], []]

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
            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[MODEL]" and split[6] == "cur_ete_l:" and (
                    "n_epoch_l:" in split)):
                time = int(split[3])
                estimated_l = float(split[7])
                # estimated_spike = float(split[13]) - float(split[7])
                lem_latency[0] += [time]
                lem_latency[1] += [estimated_l]
                # lem_latency[2] += [estimated_spike]
    return lem_latency

def retrieve_scaling_info(rawDir, expName):
    scaling_info = []

    streamsluiceOutput = "flink-samza-standalonesession-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("standalonesession") == 1:
                streamsluiceOutput = file
    streamSluiceOutputPath = rawDir + expName + "/" + streamsluiceOutput
    print("Reading streamsluice output:" + streamSluiceOutputPath)

    import re
    # Define regex patterns to match scaling start and complete lines
    pattern_start = r"\+\+\+ \[CONTROL\] time: (\d+) .*scale (in|out) operator:"
    pattern_complete = r"\+\+\+ \[CONTROL\] time: (\d+) .*all scaling plan deployed\. Scaling time: (\d+)"

    # Variables to store ongoing scaling events
    ongoing_scaling = None

    # Open and read the log file
    with open(streamSluiceOutputPath, 'r') as file:
        for line in file:
            # Check if it's a scaling start line
            match_start = re.search(pattern_start, line)
            if match_start:
                # Extract scaling start time and type
                scaling_time = int(match_start.group(1))
                scaling_type = match_start.group(2)
                ongoing_scaling = [scaling_time, scaling_type]

            # Check if it's a scaling complete line
            match_complete = re.search(pattern_complete, line)
            if match_complete and ongoing_scaling:
                # Extract the scaling complete time and the duration
                complete_time = int(match_complete.group(1))
                scaling_duration = int(match_complete.group(2))
                # Finalize the scaling information
                scaling_info.append([
                    ongoing_scaling[0],  # Scaling start time
                    complete_time,  # Scaling complete time
                    ongoing_scaling[1],  # Scaling type (in or out)
                ])
                # Reset ongoing scaling to avoid duplicate matches
                ongoing_scaling = None

    return scaling_info

def draw(rawDir, outputDir, exps, windowSize, ax, workload_name, xlabel_flag, ylabel_flag):
    averageGroundTruthLatencies = []
    averageGroundTruthLatencies_FromMetricsManager_PerOperator = []
    scaling_infos = []
    lem_latencies = []
    initial_times = []
    for i in range(0, len(exps)):
        expFile = exps[i][1]
        result = readGroundTruthLatency(rawDir, expFile, windowSize)
        averageGroundTruthLatencies += [result[0]]
        initial_times += [result[1]]
        if ground_truth_component_flag:
            result = readGroundTruthLatencyByMetricsManager(rawDir, expFile, windowSize)
            averageGroundTruthLatencies_FromMetricsManager_PerOperator += [result[0]]
        result = readLEMLatencyAndSpike(rawDir, expFile)
        result[0] = [x - initial_times[i] for x in result[0]]
        lem_latencies += [result]
        scaling_infos += [[[(scaling_info[0] - initial_times[i]), (scaling_info[1] - initial_times[i]), scaling_info[2]] for scaling_info in retrieve_scaling_info(rawDir, expName)]]
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
        result_limits = calculate_latency_limits(groundtruth_P99_latency_in_range)
        print("in range ground truth P99 limit: " + str(result_limits[1]) + ", P95 limit: " + str(result_limits[0]))
        print("in range lem latency max:" + str(max(lem_latency_in_range + [0])) + " avg: " + str(
            sum(lem_latency_in_range + [0]) / len(lem_latency_in_range + [0])))
        if ground_truth_component_flag:
            for operator_name, averageGroundTruthLatencies_FromMetricsManager in averageGroundTruthLatencies_FromMetricsManager_PerOperator[i].items():
                groundtruth_P99_MM_latency_in_range = [averageGroundTruthLatencies_FromMetricsManager[1][x] for x in
                                                   range(0, len(averageGroundTruthLatencies_FromMetricsManager[0])) if
                                                   averageGroundTruthLatencies_FromMetricsManager[0][
                                                       x] >= startTime * 1000 and
                                                   averageGroundTruthLatencies_FromMetricsManager[0][x] <= (
                                                           startTime + avg_latency_calculateTime) * 1000]
                groundtruth_P99_except_process_in_range = [averageGroundTruthLatencies_FromMetricsManager[2][x] for x
                                                               in
                                                               range(0, len(
                                                                   averageGroundTruthLatencies_FromMetricsManager[0])) if
                                                               averageGroundTruthLatencies_FromMetricsManager[0][
                                                                   x] >= startTime * 1000 and
                                                               averageGroundTruthLatencies_FromMetricsManager[0][x] <= (
                                                                       startTime + avg_latency_calculateTime) * 1000]
                print("in range operator " + operator_name + " MM latency max:" + str(max(groundtruth_P99_MM_latency_in_range)) + " avg: " + str(
                    sum(groundtruth_P99_MM_latency_in_range) / len(groundtruth_P99_MM_latency_in_range)))
                print("in range operator " + operator_name + " Except process max:" + str(
                    max(groundtruth_P99_except_process_in_range)) + " avg: " + str(
                    sum(groundtruth_P99_except_process_in_range) / len(
                        groundtruth_P99_except_process_in_range)))

    print("1800 seconds success rate")
    print(successRatePerExps)
    #print(averageGroundTruthLatencies)

    # fig, ax = plt.subplots(figsize=(5, 5))
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
        ax.plot(sampledLatency[0], sampledLatency[1], exps[i][3], color=exps[i][2], markersize=5,
                 linewidth=linewidth, label=exps[i][0])
        #plt.plot(lem_latencies[i][0], lem_latencies[i][1], '-', color="green", markersize=2, linewidth=linewidth, label=exps[i][0] + 'Estimated Latency')

    addLatencyLimitMarker(ax)
    # legend += ["Limit + Spike"]
    # addLatencyLimitWithSpikeMarker(plt)
    # plt.legend(legend, bbox_to_anchor=(0.45, 1.3), loc='upper center', ncol=4, markerscale=4.)  # When
    # plt.legend(legend, bbox_to_anchor=(0.45, 1.3), loc='upper center', ncol=3, markerscale=4.)  # How1

    #plt.xlabel('Time (min)')
    if ylabel_flag:
        ax.set_ylabel('Latency (ms)')
    if xlabel_flag:
        ax.set_xlabel('Time (s)')
    ax.set_xlim((startTime) * 1000, (startTime + expLength) * 1000)
    ax.set_xticks(np.arange((startTime) * 1000, (startTime + expLength) * 1000 + 5000, 5000))
    ax.set_xticklabels([int((x - startTime * 1000) / 1000) for x in np.arange((startTime)  * 1000, (startTime + expLength) * 1000 + 5000, 5000)])
    ax.set_ylim(0, 4000)
    ax.set_yticks(np.arange(0, 5000, 1000))

    if trickFlag:
        ax.set_yticklabels([int(x / 1250 * 1000) for x in np.arange(0, 6250, 1250)])
    ax.grid(True)
    ax.set_title(workload_name, y=-0.85, fontsize=35)

def draw_scaling_info(scaling_change_info, outputDir, label):
    bottleneck_operator = scaling_change_info[1]
    print("Scale out at time " + str(scaling_change_info[0]) + " Bottleneck: " + bottleneck_operator)
    print("key_arrival_rate: " + str(scaling_change_info[4]))
    mapping_before_scale = scaling_change_info[2]
    mapping_after_scale = scaling_change_info[3]
    key_arrival_rate = {x: y * 1000 for x, y in scaling_change_info[4].items()}
    key_backlog = scaling_change_info[5]

    def aggregate_key_level(key_metrics, mapping:dict[str:list[int]]):
        task_metrics = {}
        for task, keys in mapping.items():
            task_metrics[task] = sum([key_metrics[key] for key in keys])
        return task_metrics
    task_arrival_before_scale = aggregate_key_level(key_arrival_rate, mapping_before_scale)
    task_backlog_before_scale = aggregate_key_level(key_backlog, mapping_before_scale)
    task_arrival_after_scale = aggregate_key_level(key_arrival_rate, mapping_after_scale)
    task_backlog_after_scale = aggregate_key_level(key_backlog, mapping_after_scale)
    def draw_task_metrics_barchart(task_data:dict[str:float], label, metrics_name, file_name):
        import matplotlib.pyplot as plt
        # Create the figure and two bar charts
        fig_task, ax_task = plt.subplots(1, 1, figsize=(14, 6))

        # Sort tasks by arrival rate (optional for ranking)
        sorted_tasks = sorted(task_data.items(), key=lambda x: x[1], reverse=True)

        # Extract indices and values
        task_names = [task[0] for task in sorted_tasks]
        arrival_rates = [task[1] for task in sorted_tasks]
        indices = np.arange(len(task_names))

        # First bar chart
        ax_task.bar(indices, arrival_rates, color='skyblue', alpha=0.7)
        #ax_task.set_title(metrics_name + " under " + label, fontsize=14)
        ax_task.set_xlabel("Task Index", fontsize=12)
        ax_task.set_ylabel(metrics_name, fontsize=12)
        ax_task.set_xticks(indices)
        ax_task.set_ylim(0, 1000)
        #ax1.set_xticklabels([f"Rank {i + 1}" for i in indices], rotation=45)

        # Adjust layout and show plot
        if output_pdf_flag:
            fig_task.savefig(outputDir + label + "_" + file_name + ".pdf", bbox_inches='tight')
        else:
            fig_task.savefig(outputDir + label + "_" + file_name + ".png", bbox_inches='tight')

    draw_task_metrics_barchart(task_arrival_before_scale, label, "Arrival Rate (tps)", "Arrival_Rate_Before")
    draw_task_metrics_barchart(task_backlog_before_scale, label, "Backlog", "Backlog_Before")
    draw_task_metrics_barchart(task_arrival_after_scale, label, "Arrival Rate (tps)", "Arrival_Rate_After")
    draw_task_metrics_barchart(task_backlog_after_scale, label, "Backlog", "Backlog_After")


def draw_resource(rawDir, outputDir, exps, ax1, ax2, xlabel_flag, ylabel_flag):
    parallelismsPerJob = {}
    totalArrivalRatesPerJob = {}
    totalParallelismPerExps = {}
    scaling_change_infos = []
    for expindex in range(0, len(exps)):
        expFile = exps[expindex][1]
        result = readParallelism(rawDir, expFile)
        parallelisms = result[0]
        totalArrivalRates = result[1]
        scalings = result[3]
        for job in parallelisms.keys():
            if job == "TOTAL":
                totalParallelismPerExps[expindex] = parallelisms[job]
                for i in range(0, len(parallelisms[job][1])):
                    l = 0
                    r = 0
                    if (i + 1 < len(parallelisms[job][0])):
                        r = parallelisms[job][0][i + 1]
                    l = max(parallelisms[job][0][i], startTime * 1000)
                    r = min(r, (startTime + exp_length) * 1000)
                continue
            if job not in parallelismsPerJob:
                parallelismsPerJob[job] = []
                totalArrivalRatesPerJob[job] = []
            parallelismsPerJob[job] += [parallelisms[job]]
            totalArrivalRatesPerJob[job] += [totalArrivalRates[job]]

        scaling_change_infos.append(result[4])

    print("Draw total figure...")
    print("TOTAL parallelism: " + str(totalParallelismPerExps))

    figName = "Parallelism"
    nJobs = len(parallelismsPerJob.keys())
    jobList = ["a84740bacf923e828852cc4966f2247c", "eabd4c11f6c6fbdf011f0f1fc42097b1", "d01047f852abd5702a0dabeedac99ff5", "d2336f79a0d60b5a4b16c8769ec82e47", "feccfb8648621345be01b71938abfb72"]
    #fig, axs = plt.subplots(1, 1, figsize=(6, 5), layout='constrained')

    # Add super label
    #fig.supylabel('# of Slots')
    #supylabel2(fig, "Arrival Rate (tps)")
    #fig.tight_layout(rect=[0.02, 0, 0.953, 1])
    #axs.grid(True)
    #ax1 = axs
    #ax2 = ax1.twinx()
    if ylabel_flag:
        ax1.set_ylabel("# of Slots")
        ax2.set_ylabel("Arrival Rate (tps)")

    job = jobList[0]
    ax = sorted(totalArrivalRatesPerJob[job][0].keys())
    ay = [totalArrivalRatesPerJob[job][0][x] / (windowSize / 100) for x in ax]
    ax2.plot(ax, ay, '-', color='red', markersize=MARKERSIZE / 2, label="Arrival Rate")
    #ax2.set_ylabel('Rate (tps)')
    #ax2.set_ylim(3500, 6500)
    #ax2.set_yticks(np.arange(3500, 7500, 1000))
    ax2.set_ylim(500, 9500)
    ax2.set_yticks(np.arange(500, 10500, 2000))
    ax2.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
    ax2.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 5000, 5000))
    ax2.set_xticklabels([int((x - startTime * 1000) / 1000) for x in
                         np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 5000, 5000)])

    #ax2.legend(legend, loc='upper right', bbox_to_anchor=(1.1, 1.3), ncol=1)



    legend = []
    scalingPoints = [[], []]
    scale_out_points = {}
    for expindex in range(0, len(exps)):
        if(exps[expindex][0] == "Static"):
            continue
        print("Draw exps " + exps[expindex][0] + " curve...")
        totalParallelism = 0
        Parallelism = totalParallelismPerExps[expindex]
        # print(job + " " + str(expindex) + " " + str(Parallelism))
        legend += [exps[expindex][0]]
        line = [[], []]
        scale_out_points[expindex] = [[], []]

        # Interval for markers in milliseconds (2 seconds = 2000 ms)
        marker_interval = 2000
        next_marker_time = startTime * 1000 + marker_interval

        for i in range(0, len(Parallelism[0])):
            x0 = Parallelism[0][i]
            y0 = Parallelism[1][i]
            if i + 1 >= len(Parallelism[0]):
                x1 = 10000000
                y1 = y0
            else:
                x1 = Parallelism[0][i + 1]
                y1 = Parallelism[1][i + 1]
            l = max(x0, startTime * 1000)
            r = min(x1, (startTime + exp_length) * 1000)
            if(exps[expindex][0] == 'Sluice' and l < r):
                totalParallelism += (r - l) * y0
                for scalingTime in scalings:
                    if scalingTime >= l and scalingTime <= r:
                        scalingPoints[0] += [scalingTime]
                        scalingPoints[1] += [y0]
            line[0].append(x0)
            line[0].append(x1)
            line[1].append(y0)
            line[1].append(y0)
            line[0].append(x1)
            line[0].append(x1)
            line[1].append(y0)
            line[1].append(y1)

            # Add marker points every 2 seconds
            while next_marker_time >= x0 and next_marker_time <= x1 and next_marker_time <= r:
                ax1.scatter(next_marker_time, y0, color=exps[expindex][2], marker=exps[expindex][3][0], s=50, zorder=3)
                next_marker_time += marker_interval

            if y1 >= y0:
                scale_out_points[expindex][0].append(x1)
                scale_out_points[expindex][1].append(y0)
        if exps[expindex][0] == 'Sluice':
            linewidth = LINEWIDTH
        else:
            linewidth = LINEWIDTH / 2.0
        ax1.plot(line[0], line[1], exps[expindex][3], color=exps[expindex][2], markersize=5, linewidth=linewidth, label=exps[expindex][0])
        print("Average parallelism " + exps[expindex][0] + " : " + str(totalParallelism / (exp_length * 1000)))
    for expindex in range(0, len(exps)):
        if (exps[expindex][0] == "Static"):
            continue
        ax1.plot(scale_out_points[expindex][0], scale_out_points[expindex][1], exps[expindex][3][0], color=exps[expindex][2])


    ax1.set_ylim(15, 35)
    ax1.set_yticks(np.arange(15, 35, 5))

    ax1.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
    ax1.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 5000, 5000))
    ax1.set_xticklabels([int((x - startTime * 1000) / 1000) for x in
                         np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 5000, 5000)])
    if xlabel_flag:
        ax1.set_xlabel("Time (s)")
    ax1.grid(True)
    ax2.grid(True)

    for expindex in range(0, len(exps)):
        if (exps[expindex][0] == "Static"):
            continue
        draw_scaling_info(scaling_change_infos[expindex][0], outputDir, exps[expindex][0])



    # import os
    # if not os.path.exists(outputDir):
    #     os.makedirs(outputDir)
    #
    # if output_pdf_flag:
    #     plt.savefig(outputDir + figName + ".pdf", bbox_inches='tight')
    # else:
    #     plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    # plt.close(fig)


rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/part7/"
exps_per_setting = {
    "(a) No Skew": [
        ["Static",
         "part6and7-microbench-streamsluice-ds2-800-part7-linear-1split2join1-120-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-false-1",
         "black", "x--"],
        ["DS2",
         "part6and7-microbench-streamsluice-ds2_new-800-part7-linear-1split2join1-120-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2",
         "purple", "^-"],
        ["Sluice",
         "part6and7-microbench-streamsluice-streamsluice-800-part7-linear-1split2join1-120-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1",
         "blue", "o-"],
    ],
    "(b) Skewed (0.1)": [
        ["Static",
         "part7-microbench-streamsluice-ds2-800-part7-linear-1split2join1-120-4000-4000-960-linear-4000-1-1440-stair_3-80-1-1440-stair_3-1-0.1-1-20-1-5000-1-20-1-5000-1-20-1-5000-20-1000-5000-0.00-0.1-2000-3000-100-10-false-3",
         "black", "x--"],
        ["DS2",
         "part7-microbench-streamsluice-ds2_new-800-part7-linear-1split2join1-120-4000-4000-960-linear-4000-1-1440-stair_3-80-1-1440-stair_3-1-0.1-1-20-1-5000-1-20-1-5000-1-20-1-5000-20-1000-5000-0.00-0.1-2000-3000-100-10-true-1",
         "purple", "^-"],
        ["Sluice",
         "part7-microbench-streamsluice-streamsluice-800-part7-linear-1split2join1-120-4000-4000-960-linear-4000-1-1440-stair_3-80-1-1440-stair_3-1-0.1-1-20-1-5000-1-20-1-5000-1-20-1-5000-20-1000-5000-0.00-0.1-2000-3000-100-10-true-1",
         "blue", "o-"],
    ],
    # "(b) Skewed (0.2)": [
    #     ["Static",
    #      "part7-microbench-streamsluice-ds2-800-part7-linear-1split2join1-120-4000-4000-960-linear-4000-1-1440-stair_3-80-1-1440-stair_3-1-0.2-1-20-1-5000-1-20-1-5000-1-20-1-5000-20-1000-5000-0.00-0.1-2000-3000-100-10-false-3",
    #      "black", "x--"],
    #     ["DS2",
    #      "part7-microbench-streamsluice-ds2_new-800-part7-linear-1split2join1-120-4000-4000-960-linear-4000-1-1440-stair_3-80-1-1440-stair_3-1-0.2-1-20-1-5000-1-20-1-5000-1-20-1-5000-20-1000-5000-0.00-0.1-2000-3000-100-10-true-1",
    #      "purple", "^-"],
    #     ["Sluice",
    #      "part7-microbench-streamsluice-streamsluice-800-part7-linear-1split2join1-120-4000-4000-960-linear-4000-1-1440-stair_3-80-1-1440-stair_3-1-0.2-1-20-1-5000-1-20-1-5000-1-20-1-5000-20-1000-5000-0.00-0.1-2000-3000-100-10-true-1",
    #      "blue", "o-"],
    # ],
}



exps_per_settings = {
    "all": exps_per_setting,
}

startTime=60 #30+300 #30
perOperatorFlag = False
weightedTotalParallelismFlag = False
parallelismWeight = {
    "OP2": 10,
    "OP3": 5,
    "OP4" : 2,
    "OP5" : 3,
}
arrivalRateFlag = True
import sys
if len(sys.argv) > 1:
    expName = sys.argv[1].split("/")[-1]

slot_ylim_app = {
    "Stock": 45,
    "Tweet": 30,
    "Linear_Road": 100, #27,
}
arrivalrate_ylim_app = {
    "Stock": 10000,
    "Tweet": 10000,
    "Linear_Road": 10000,
}
isSingleOperator = False #True
overall_latency = {}

windowSize = 1000 #500 #500
latencyLimit = 0
spike = 2500 #1500
#latencyLimit = 2500 #1000
startTime = 55 #55
expLength = 30 #30
exp_length = expLength
show_avg_flag = False
ground_truth_component_flag = False
show_scaling_flag = False #True

avg_latency_calculateTime = expLength # 30
trickFlag = False #True

output_pdf_flag = True

for name, exps_per_setting in exps_per_settings.items():
    fig, axs = plt.subplots(3, 2, figsize=(20, 9), layout='constrained')

    index = 0
    for workload, exps in exps_per_setting.items():
        latencyLimit = int(exps[0][1].split('-')[-6])
        expName = exps[0][1]
        print(expName)


        exp_length = expLength
        ylabel_flag = False
        if index == 0:
            ylabel_flag = True
        draw(rawDir, outputDir, exps, windowSize, axs[2][index], workload, True, ylabel_flag)
        draw_resource(rawDir, outputDir, exps, axs[1][index], axs[0][index], False, ylabel_flag)
        index += 1

    handles, labels = axs[2, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.51, 1.12), ncol=7, markerscale=5)

    if output_pdf_flag:
        fig.savefig(outputDir + "one_in_all_part7_" + name + ".pdf", bbox_inches='tight')
    else:
        fig.savefig(outputDir + "one_in_all_part7_" + name + ".png", bbox_inches='tight')

