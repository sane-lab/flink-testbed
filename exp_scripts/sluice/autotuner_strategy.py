import math
import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

# Set up matplotlib font sizes
SMALL_SIZE = 25
MEDIUM_SIZE = 30
BIGGER_SIZE = 35

plt.rc('font', size=SMALL_SIZE)
plt.rc('axes', titlesize=SMALL_SIZE)
plt.rc('axes', labelsize=MEDIUM_SIZE)
plt.rc('xtick', labelsize=SMALL_SIZE)
plt.rc('ytick', labelsize=SMALL_SIZE)
plt.rc('legend', fontsize=SMALL_SIZE)
plt.rc('figure', titlesize=BIGGER_SIZE)
MARKERSIZE = 4
LINEWIDTH = 3

def read_ground_truth_latency(raw_dir, exp_name, window_size):
    initial_time = -1
    ground_truth_latency = []
    task_executors = [file for file in os.listdir(raw_dir + exp_name + "/") if
                      file.endswith(".out") and "taskexecutor" in file]

    for task_executor in task_executors:
        ground_truth_path = os.path.join(raw_dir, exp_name, task_executor)
        print("Reading ground truth file:" + ground_truth_path)
        file_initial_time = -1
        counter = 0
        with open(ground_truth_path) as f:
            lines = f.readlines()
            for i in range(0, len(lines)):
                line = lines[i]
                split = line.rstrip().split()
                counter += 1
                if (counter % 5000 == 0):
                    print("Processed to line:" + str(counter))
                if split[0] == "GT:":
                    completed_time = int(split[2].rstrip(","))
                    latency = int(split[3].rstrip(","))
                    arrived_time = completed_time - latency
                    if file_initial_time == -1 or file_initial_time > arrived_time:
                        file_initial_time = arrived_time
                    ground_truth_latency += [[arrived_time, latency]]
        if file_initial_time > 0 and (initial_time == -1 or initial_time > file_initial_time):
            initial_time = file_initial_time

    aggregated_ground_truth_latency = {}
    for pair in ground_truth_latency:
        index = int((pair[0] - initial_time) / window_size)
        if index not in aggregated_ground_truth_latency:
            aggregated_ground_truth_latency[index] = []
        aggregated_ground_truth_latency[index] += [pair[1]]

    average_ground_truth_latency = [[], [], []]
    for index in sorted(aggregated_ground_truth_latency):
        time = index * window_size
        x = int(time)
        if index in aggregated_ground_truth_latency:
            sorted_latency = sorted(aggregated_ground_truth_latency[index])
            size = len(sorted_latency)
            target = min(math.ceil(size * 0.99), size) - 1
            y = sorted_latency[target]
            average_ground_truth_latency[0] += [x]
            average_ground_truth_latency[1] += [y]
            y = sum(sorted_latency) / size
            average_ground_truth_latency[2] += [y]

    return [average_ground_truth_latency, initial_time]

def readLEMLatencyAndSpikeAndBar(rawDir, expName) -> [[list[int], list[float], list[float]], dict[int, int]]:
    lem_latency = [[], [], []]
    latency_bar = {}
    p99_bar = {}

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
            if (len(split) >= 8 and split[0] == "[AUTOTUNE]" and split[4] == "initial" and split[5] == "latency" and split[6] == "bar:"):
                time = int(split[2])
                bar = int(split[7].rstrip(','))
                latency_bar[time] = bar
            if (len(split) >= 8 and split[1] == "[AUTOTUNE]" and split[4] == "user" and split[5] == "limit" and split[6] == "is"):
                time = int(split[3])
                for index in range(7, 15):
                    if(split[index] == "bar:"):
                        bar = int(split[index + 1].rstrip(','))
                        p99 = int(split[index + 4].rstrip(','))
                        break
                latency_bar[time] = bar
                p99_bar[time] = p99
            if (len(split) >= 8 and split[1] == "[AUTOTUNE]" and split[4] == "set" and split[5] == "bar" and split[6] == "to" and split[7] == "lowerbound:"):
                time = int(split[3])
                bar = int(split[8].rstrip(','))
                latency_bar[time] = bar
                p99_bar[time] = int(split[11].rstrip(','))

    return [lem_latency, latency_bar, p99_bar]

def add_latency_limit_marker(plt, latency_limit):
    x = [0, 10000000]
    y = [latency_limit, latency_limit]
    plt.plot(x, y, "--", label="Limit", color='red', linewidth=1.5)


def add_p99_bar_curve(plt, p99_bar: dict[int, int], initial_time):
    last_time = 0
    for time in p99_bar.keys():
        if time - initial_time > last_time:
            x = [last_time, time - initial_time]
            y = [p99_bar[time], p99_bar[time]]
            print(x, y)
            plt.plot(x, y, 'd-', label="P99 latency", color='blue', linewidth=1.5)
        last_time = time - initial_time

def add_latency_bar_curve(plt, latency_bar:dict[int, int], initial_time):
    last_time = 0
    last_y = 0
    for time in latency_bar.keys():
        x = [last_time, time - initial_time]
        y = [last_y, last_y]
        plt.plot(x, y, 'o--', label="Latency Bar", color='green', linewidth=1.5)
        last_y = latency_bar[time]
        last_time = time - initial_time
    x = [last_time, 10000000]
    y = [last_y, last_y]
    plt.plot(x, y, 'o--', label="Latency Bar", color='green', linewidth=1.5)

def draw_latency_curves(raw_dir, output_dir, exp_name, window_size, start_time, exp_length, latency_limit):
    exps = [
        ["GroundTruth", exp_name, "blue", "o"]
    ]

    average_ground_truth_latencies = []
    lem_latencies = []
    latency_bar = []
    p99_bar = []
    initial_times = []
    for i in range(len(exps)):
        result = read_ground_truth_latency(raw_dir, exps[i][1], window_size)
        average_ground_truth_latencies += [result[0]]
        initial_times += [result[1]]
        result = readLEMLatencyAndSpikeAndBar(raw_dir, exps[i][1])
        lem_latencies += [result[0]]
        latency_bar += [result[1]]
        p99_bar += [result[2]]
    #print(p99_bar)
    for i in range(len(exps)):
        groundtruth_p99_latency_in_range = [average_ground_truth_latencies[i][1][x] for x in
                                            range(len(average_ground_truth_latencies[i][0])) if
                                            average_ground_truth_latencies[i][0][x] >= start_time * 1000 and
                                            average_ground_truth_latencies[i][0][x] <= (start_time + exp_length) * 1000]
        success_rate = len([x for x in groundtruth_p99_latency_in_range if x <= latency_limit])/len(groundtruth_p99_latency_in_range)
        print("Success rate: " + str(success_rate))
    # Plotting the latency curve
    fig, ax = plt.subplots(figsize=(12, 5))
    for i in range(len(exps)):
        average_ground_truth_latency = average_ground_truth_latencies[i]
        sample_factor = 1
        sampled_latency = [[], [], []]
        sampled_latency[0] = [average_ground_truth_latency[0][i] for i in
                              range(0, len(average_ground_truth_latency[0]), sample_factor)]
        sampled_latency[1] = [max([average_ground_truth_latency[1][y] for y in
                                   range(x, min(x + sample_factor, len(average_ground_truth_latency[1])))]) for x in
                              range(0, len(average_ground_truth_latency[0]), sample_factor)]
        sampled_latency[2] = [max([average_ground_truth_latency[2][y] for y in
                                   range(x, min(x + sample_factor, len(average_ground_truth_latency[2])))]) for x in
                              range(0, len(average_ground_truth_latency[0]), sample_factor)]

        plt.plot(sampled_latency[0], sampled_latency[1], '-', color=exps[i][2], markersize=4, linewidth=3,
                 label="Ground Truth P99")
        add_latency_limit_marker(plt, latency_limit)

    handles, labels = plt.gca().get_legend_handles_labels()
    new_labels, new_handles = [], []
    for handle, label in zip(handles, labels):
        if label not in new_labels:
            new_labels.append(label)
            new_handles.append(handle)
    plt.legend(new_handles, new_labels, bbox_to_anchor=(0.45, 1.4), loc='upper center', ncol=3, markerscale=4.)
    plt.ylabel('Latency (ms)')
    axes = plt.gca()
    axes.set_xlim((start_time) * 1000, (start_time + exp_length) * 1000)
    axes.set_xticks(np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + (exp_length / 10) * 1000, (exp_length / 10) * 1000))
    axes.set_xticklabels([int((x - start_time * 1000) / 1000) for x in
                          np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + (exp_length / 10) * 1000, (exp_length / 10) * 1000)])

    axes.set_ylim(0, 3000)
    axes.set_yticks(np.arange(0, 3300, 300))
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.close(fig)

    #Calculate the bar converge time
    #tune_window_success_rates = {}
    last_time = 0
    last_bar = 0
    index = 0
    first_converge_time = 0
    for time, bar in latency_bar[0].items():
        if last_bar > 0:
            # start = last_time - initial_times[0]
            # end = time - initial_times[0]
            # tune_window_groundtruth_p99_latency_in_range = [average_ground_truth_latencies[0][1][x] for x in
            #                                     range(len(average_ground_truth_latencies[0][0])) if
            #                                     average_ground_truth_latencies[0][0][x] >= start and
            #                                     average_ground_truth_latencies[0][0][x] <
            #                                                 end]
            # print(start, end)
            # tune_window_success_rate = len([x for x in tune_window_groundtruth_p99_latency_in_range if x <= latency_limit]) / len(
            #     tune_window_groundtruth_p99_latency_in_range)
            # tune_window_success_rates[last_time - initial_times[0]] = tune_window_success_rate
            if last_bar != bar:
                first_converge_time = index + 1
        index += 1
        last_time = time
        last_bar = bar
    if first_converge_time == 0:
        first_converge_time = 1
    start = last_time - initial_times[0]
    end = (start_time + exp_length) * 1000
    # tune_window_groundtruth_p99_latency_in_range = [average_ground_truth_latencies[0][1][x] for x in
    #                                                 range(len(average_ground_truth_latencies[0][0])) if
    #                                                 average_ground_truth_latencies[0][0][x] >= start and
    #                                                 average_ground_truth_latencies[0][0][x] <
    #                                                     end]
    # if len(tune_window_groundtruth_p99_latency_in_range) > 0:
    #     tune_window_success_rate = len(
    #     [x for x in tune_window_groundtruth_p99_latency_in_range if x <= latency_limit]) / len(
    #     tune_window_groundtruth_p99_latency_in_range)
    #     tune_window_success_rates[last_time - initial_times[0]] = tune_window_success_rate
    converged_bar = last_bar
    #print("tune window success rates: " + str(tune_window_success_rates))
    #first_converge_time = 0
    #index = 0
    # for time, tune_window_success_rate in tune_window_success_rates.items():
    #     if tune_window_success_rate < 0.99:
    #         first_converge_time = index + 1
    #     index += 1


    # Plotting the latency curve
    fig, ax = plt.subplots(figsize=(12, 5))
    for i in range(len(exps)):
         # lem_latencies[i][0] = [x - initial_times[0] for x in lem_latencies[i][0]]
         # plt.plot(lem_latencies[i][0], lem_latencies[i][1], '-', color=exps[i][2], markersize=4, linewidth=3,
         #          label="Estimated Latency")
        add_p99_bar_curve(plt, p99_bar[i], initial_times[i])
        add_latency_bar_curve(plt, latency_bar[i], initial_times[i])
    add_latency_limit_marker(plt, latency_limit)
    handles, labels = plt.gca().get_legend_handles_labels()
    new_labels, new_handles = [], []
    for handle, label in zip(handles, labels):
        if label not in new_labels:
            new_labels.append(label)
            new_handles.append(handle)
    plt.legend(new_handles, new_labels, bbox_to_anchor=(0.45, 1.4), loc='upper center', ncol=3, markerscale=4.)
    plt.ylabel('Latency (ms)')
    axes = plt.gca()
    axes.set_xlim((start_time) * 1000, (start_time + exp_length) * 1000)
    axes.set_xticks(np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + (exp_length / 10) * 1000, (exp_length / 10) * 1000))
    axes.set_xticklabels([int((x - start_time * 1000) / 1000) for x in
                          np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + (exp_length / 10) * 1000, (exp_length / 10) * 1000)])
    if (latency_limit < 3000):
        axes.set_ylim(0, 3000)
        axes.set_yticks(np.arange(0, 3300, 300))
    elif (latency_limit < 6000):
        axes.set_ylim(0, 10050)
        axes.set_yticks(np.arange(0, 11000, 1000))
    else:
        axes.set_ylim(0, 25000)
        axes.set_yticks(np.arange(0, 27500, 2500))
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'latency_bar.png', bbox_inches='tight')
    plt.close(fig)

    return success_rate, first_converge_time, converged_bar

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
def parsePerTaskValue(splits):
    taskValues = {}
    for split in splits:
        split = split.lstrip("{").rstrip("}").rstrip(",")
        words = split.split("=")
        taskName = words[0]
        value = float(words[1])
        taskValues[taskName] = value
    return taskValues
def readParallelism(rawDir, expName, windowSize):
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

    timestamp_to_index = {}

    with open(streamSluiceOutputPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
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

    ParallelismPerJob["TOTAL"] = [[], []]
    for job in ParallelismPerJob:
        if job != "TOTAL":
            for i in range(0, len(ParallelismPerJob[job][0])):
                if i >= len(ParallelismPerJob["TOTAL"][0]):
                    ParallelismPerJob["TOTAL"][0].append(ParallelismPerJob[job][0][i])
                    ParallelismPerJob["TOTAL"][1].append(ParallelismPerJob[job][1][i])
                else:
                    ParallelismPerJob["TOTAL"][1][i] += ParallelismPerJob[job][1][i]

    first_ax = -1
    totalArrivalRatePerJob = {}
    for task in arrivalRatePerTask:
        job = task.split("_")[0]
        n = len(arrivalRatePerTask[task][0])
        if job not in totalArrivalRatePerJob:
            totalArrivalRatePerJob[job] = {}
        for i in range(0, n):
            ax = arrivalRatePerTask[task][0][i]
            if (first_ax == -1):
                first_ax = ax
            delta_x = round((ax - first_ax) / windowSize) * windowSize
            index = ((delta_x + first_ax) // windowSize) * windowSize #math.floor(ax / windowSize) * windowSize
            ay = arrivalRatePerTask[task][1][i]
            if index not in totalArrivalRatePerJob[job]:
                totalArrivalRatePerJob[job][index] = ay
            else:
                totalArrivalRatePerJob[job][index] += ay

    print(expName, ParallelismPerJob.keys())
    return [ParallelismPerJob, totalArrivalRatePerJob, initialTime, scalings]

def draw_parallelism_curve(rawDir, outputDir, exp_name, windowSize, startTime, exp_length, draw_parallelism_flag) -> [float, [list[int], list[float]]]:
    exps = [
        ["Sluice", exp_name, "blue", "o"]
    ]
    parallelismsPerJob = {}
    totalArrivalRatesPerJob = {}
    totalParallelismPerExps = {}
    for expindex in range(0, len(exps)):
        expFile = exps[expindex][1]
        result = readParallelism(rawDir, expFile, windowSize)
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
    print("Draw total figure...")
    print("TOTAL parallelism: " + str(totalParallelismPerExps))

    figName = "Parallelism"
    nJobs = len(parallelismsPerJob.keys())
    jobList = ["a84740bacf923e828852cc4966f2247c", "eabd4c11f6c6fbdf011f0f1fc42097b1", "d01047f852abd5702a0dabeedac99ff5", "d2336f79a0d60b5a4b16c8769ec82e47", "feccfb8648621345be01b71938abfb72"]
    fig, axs = plt.subplots(1, 1, figsize=(12, 5), layout='constrained')
    # Add super label
    #fig.supylabel('# of Slots')
    #supylabel2(fig, "Arrival Rate (tps)")
    fig.tight_layout(rect=[0.02, 0, 0.953, 1])
    axs.grid(True)
    if(draw_parallelism_flag):
        ax1 = axs
        ax2 = ax1.twinx()
    else:
        ax2 = axs
        ax2.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
        ax2.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + (exp_length / 10) * 1000, (exp_length / 10) * 1000))
        ax2.set_xticklabels([int((x - startTime * 1000) / 1000) for x in
                             np.arange(startTime * 1000, (startTime + exp_length) * 1000 + (exp_length / 10) * 1000, (exp_length / 10) * 1000)])
        ax2.set_xlabel("Time (s)")


    ax2.set_ylabel("Arrival Rate (tps)")

    job = jobList[0]
    ax = sorted(totalArrivalRatesPerJob[job][0].keys())
    ay = [totalArrivalRatesPerJob[job][0][x] / (windowSize / 100) for x in ax]
    arrival_curves = [ax, ay]
    ax2.plot(ax, ay, '-', color='red', markersize=MARKERSIZE / 2, label="Arrival Rate")
    #ax2.set_ylabel('Rate (tps)')
    ax2.set_ylim(0, 30000)
    ax2.set_yticks(np.arange(0, 35000, 5000))
    # legend = ["OP_" + str(jobIndex + 1) +"Arrival Rate"]
    legend = ["Arrival Rate"]
    # ax2.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
    # ax2.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 300000, 300000))
    # ax2.set_xticklabels([int((x - startTime * 1000) / 60000) for x in
    #                      np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000)])
    ax2.legend(legend, loc='upper right', bbox_to_anchor=(1.1, 1.3), ncol=1)
    average_parallelism = 0.0
    if(draw_parallelism_flag):
        ax1.set_ylabel("# of Slots")
        legend = []
        scalingPoints = [[], []]
        for expindex in range(0, len(exps)):
            if(exps[expindex][0] == "Static"):
                continue
            print("Draw exps " + exps[expindex][0] + " curve...")
            totalParallelism = 0
            Parallelism = totalParallelismPerExps[expindex]
            # print(job + " " + str(expindex) + " " + str(Parallelism))
            legend += [exps[expindex][0]]
            line = [[], []]
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
            if exps[expindex][0] == 'Sluice':
                linewidth = LINEWIDTH
            else:
                linewidth = LINEWIDTH / 2.0
            ax1.plot(line[0], line[1], color=exps[expindex][2], linewidth=linewidth, label='# of Slots')
            average_parallelism = totalParallelism / (exp_length * 1000)
            print("Average parallelism " + exps[expindex][0] + " : " + str(totalParallelism / (exp_length * 1000)))
        ax1.plot(scalingPoints[0], scalingPoints[1], 'o', color="orange", mfc='none', markersize=MARKERSIZE * 2, label="Scaling")
        ax1.legend(legend, loc='upper left', bbox_to_anchor=(-0.1, 1.3), ncol=3, markerscale=4.)
        # ax1.set_ylabel('OP_'+str(jobIndex+1)+' Parallelism')
        ax1.set_ylim(4, 32) #17)
        ax1.set_yticks(np.arange(4, 34, 2)) #18, 1))

        ax1.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
        ax1.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + (exp_length / 10) * 1000, (exp_length / 10) * 1000))
        ax1.set_xticklabels([int((x - startTime * 1000) / 1000) for x in
                             np.arange(startTime * 1000, (startTime + exp_length) * 1000 + (exp_length / 10) * 1000, (exp_length / 10) * 1000)])
        ax1.set_xlabel("Time (s)")

    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    # plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)
    return average_parallelism, arrival_curves


# Function to plot success rates
def plot_success_rate(user_limits, success_rate_per_label, output_dir, setting_index):
    labels = list(success_rate_per_label.keys())
    bar_width = 0.2
    x = np.arange(len(user_limits))

    fig, axs = plt.subplots(figsize=(12, 5))

    # Plot success rates
    for idx, label in enumerate(labels):
        plt.bar(x + idx * bar_width, success_rate_per_label[label], bar_width, label=label)

    plt.xlabel('User Limits')
    plt.ylabel('Success Rate')
    plt.xticks(x + bar_width * (len(labels) - 1) / 2, user_limits)
    plt.ylim(0.95, 1.01)
    plt.yticks(np.arange(0.95, 1.01, 0.01))
    plt.title('Success Rates by User Limits and Strategy')
    plt.legend()
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'success_rate_by_strategy_' + str(setting_index) + '.png', bbox_inches='tight')
    plt.close(fig)

def plot_converge_time(user_limits, first_converge_time_per_label, output_dir, setting_index):
    labels = list(first_converge_time_per_label.keys())
    bar_width = 0.2
    x = np.arange(len(user_limits))

    fig, axs = plt.subplots(figsize=(12, 5))

    # Plot avg parallelism
    for idx, label in enumerate(labels):
        plt.bar(x + idx * bar_width, first_converge_time_per_label[label], bar_width, label=label)

    plt.xlabel('User Limits')
    plt.ylabel('# of Tunes to Converge')
    plt.yticks(np.arange(0, 20, 5))
    plt.xticks(x + bar_width * (len(labels) - 1) / 2, user_limits)
    plt.title('Number of Tunes to Converge by User Limits and Strategy')
    plt.legend()
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'converge_vs_strategy_' + str(setting_index) + '.png', bbox_inches='tight')
    plt.close(fig)

def plot_converged_bar(user_limits, converged_bar_per_label, output_dir, setting_index):
    labels = list(converged_bar_per_label.keys())
    bar_width = 0.2
    x = np.arange(len(user_limits))

    fig, axs = plt.subplots(figsize=(12, 5))

    # Plot avg parallelism
    for idx, label in enumerate(labels):
        plt.bar(x + idx * bar_width, converged_bar_per_label[label], bar_width, label=label)

    plt.xlabel('User Limits')
    plt.ylabel('Converged Bar')
    #plt.yticks(np.arange(0, 20, 5))
    plt.xticks(x + bar_width * (len(labels) - 1) / 2, user_limits)
    plt.title('Converged Bar by User Limits and Strategy')
    plt.legend()
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'converged_bar_vs_strategy_' + str(setting_index) + '.png', bbox_inches='tight')
    plt.close(fig)


# Function to plot avg parallelism
def plot_avg_parallelism(user_limits, avg_parallelism_per_label, output_dir, setting_index):
    labels = list(avg_parallelism_per_label.keys())
    bar_width = 0.2
    x = np.arange(len(user_limits))

    fig, axs = plt.subplots(figsize=(12, 5))

    # Plot avg parallelism
    for idx, label in enumerate(labels):
        plt.bar(x + idx * bar_width, avg_parallelism_per_label[label], bar_width, label=label)

    plt.xlabel('User Limits')
    plt.ylabel('Avg Parallelism')
    plt.xticks(x + bar_width * (len(labels) - 1) / 2, user_limits)
    plt.title('Avg Parallelism by User Limits and Strategy')
    plt.legend()
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'resource_vs_strategy_' + str(setting_index) + '.png', bbox_inches='tight')
    plt.close(fig)


def main():
    raw_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
    output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
    overall_output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/autotuner/"
    window_size = 100
    start_time = 60 #30 #60
    exp_length = 1800 #1200 #600
    exps_per_label = {
        # "option_1": [
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-1-0.5-1-2.0-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-250-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-1-0.5-1-2.0-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-1-0.5-1-2.0-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-1-0.5-1-2.0-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-1-0.5-1-2.0-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-4000-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-30-100-200-1-0.5-1-2.0-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-125-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-30-100-200-1-0.5-1-2.0-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-250-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-30-100-200-1-0.5-1-2.0-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-30-100-200-1-0.5-1-2.0-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-30-100-200-1-0.5-1-2.0-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     #"autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-250-3000-100-1-true-1",
        #     #"autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
        #     #"autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-true-1",
        #     # "autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-2000-3000-100-1-true-1",
        #     #"autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-4000-3000-100-1-true-1",
        # ],
        # "option_2": [
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-1-0.5-2-0.8-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-250-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-1-0.5-2-0.8-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-1-0.5-2-0.8-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-1-0.5-2-0.8-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-1-0.5-2-0.8-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-4000-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-30-100-200-1-0.5-2-0.8-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-125-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-30-100-200-1-0.5-2-0.8-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-250-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-30-100-200-1-0.5-2-0.8-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-30-100-200-1-0.5-2-0.8-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-30-100-200-1-0.5-2-0.8-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     #"autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-250-3000-100-1-true-1",
        #     #"autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
        #     #"autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-true-1",
        #     #"autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-2000-3000-100-1-true-1",
        #     #"autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-4000-3000-100-1-true-1",
        # ],
        # "bisection-with-increase": [
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-2-0.2-1-2.0-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-250-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-2-0.2-1-2.0-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-2-0.2-1-2.0-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-2-0.2-1-2.0-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-2-0.2-1-2.0-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-4000-3000-100-1-true-1",
        #     "autotune-setting2-true-streamsluice-streamsluice-30-100-200-2-0.2-1-2.0-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-125-3000-100-1-true-1",
        #     "autotune-setting2-true-streamsluice-streamsluice-30-100-200-2-0.2-1-2.0-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-250-3000-100-1-true-1",
        #     "autotune-setting2-true-streamsluice-streamsluice-30-100-200-2-0.2-1-2.0-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #     "autotune-setting2-true-streamsluice-streamsluice-30-100-200-2-0.2-1-2.0-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #     "autotune-setting2-true-streamsluice-streamsluice-30-100-200-2-0.2-1-2.0-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     # "autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-250-3000-100-1-true-1",
        #     # "autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
        #     # "autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-true-1",
        #     # "autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-2000-3000-100-1-true-1",
        #     # "autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-4000-3000-100-1-true-1",
        # ],
        # "gradient_descent-with-increase": [
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-2-0.2-2-0.8-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-250-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-2-0.2-2-0.8-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-2-0.2-2-0.8-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-2-0.2-2-0.8-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-30-100-300-2-0.2-2-0.8-2-when-sine-1split2join1-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-4000-3000-100-1-true-1",
        #     "autotune-setting2-true-streamsluice-streamsluice-30-100-200-2-0.2-2-0.8-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-125-3000-100-1-true-1",
        #     "autotune-setting2-true-streamsluice-streamsluice-30-100-200-2-0.2-2-0.8-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-250-3000-100-1-true-1",
        #     "autotune-setting2-true-streamsluice-streamsluice-30-100-200-2-0.2-2-0.8-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #     "autotune-setting2-true-streamsluice-streamsluice-30-100-200-2-0.2-2-0.8-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #     "autotune-setting2-true-streamsluice-streamsluice-30-100-200-2-0.2-2-0.8-2-when-linear-2op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     # "autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-250-3000-100-1-true-1",
        #     # "autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
        #     # "autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-true-1",
        #     # "autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-2000-3000-100-1-true-1",
        #     # "autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-4000-3000-100-1-true-1",
        # ],
        "bisection-no-increase": [
            # "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            # "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
            # "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",

            # "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-1-2.0-1-when-linear-2op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-250-3000-100-1-true-1",
            # "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-1-2.0-1-when-linear-2op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            # "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-1-2.0-1-when-linear-2op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",

            # "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-1-2.0-1-when-gradient-4op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
            # "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-1-2.0-1-when-gradient-4op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-true-1",
            # "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-1-2.0-1-when-gradient-4op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1",

            # Setting 4
            # "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            # "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
            # "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
            # "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",

            # Setting 5
            "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-20-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-20-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
            "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-20-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
            "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-20-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",

            # "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-1-2.0-1-when-linear-2op_line-660-13750-30-6250-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-250-3000-100-1-true-1",
            # "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-1-2.0-1-when-linear-2op_line-660-13750-30-6250-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            # "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-1-2.0-1-when-linear-2op_line-660-13750-30-6250-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
            # "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-1-2.0-1-when-linear-2op_line-660-13750-30-6250-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
            #
            # "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-1-2.0-1-when-gradient-4op_line-660-13750-30-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
            # "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-1-2.0-1-when-gradient-4op_line-660-13750-30-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-true-1",
            # "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-1-2.0-1-when-gradient-4op_line-660-13750-30-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1",
            # "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-1-2.0-1-when-gradient-4op_line-660-13750-30-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-2000-3000-100-1-true-1"
        ],
        # "gradient_descent-no-increase": [
        #     # "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-2-1.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-2-1.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #     # "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-2-1.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-2-1.0-1-when-linear-2op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-250-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-2-1.0-1-when-linear-2op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #     # "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-2-1.0-1-when-linear-2op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #     "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-2-1.0-1-when-gradient-4op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
        #     "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-2-1.0-1-when-gradient-4op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-true-1",
        #     "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-2-1.0-1-when-gradient-4op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1"
        # ],
    }
    success_rate_per_label = {}
    avg_parallelism_per_label = {}
    first_converge_time_per_label = {}
    converged_bar_per_label = {}
    # Setting 1:
    # success_rate_per_label = {'option_1': [0.9909969989996665, 0.9995000833194467, 1.0, 1.0],
    #                           'option_2': [0.9958340276620563, 0.9998333611064822, 1.0, 1.0],
    #                           'option_3': [0.9879959986662221, 0.990834860856524, 0.9765039160139977,
    #                                        0.9726712214630895],
    #                           'option_4': [0.9888314719119853, 0.9900016663889352, 0.9673387768705216,
    #                                        0.9575070821529745]}
    # avg_parallelism_per_label = {
    #     'option_1': [24.465108333333333, 15.820303333333333, 15.662833333333333, 15.517728333333332],
    #     'option_2': [26.873041666666666, 15.73877, 15.52051, 15.566878333333333],
    #     'option_3': [25.733346666666666, 15.560146666666666, 15.285025, 15.010518333333334],
    #     'option_4': [24.085583333333332, 15.399241666666667, 15.169036666666667, 14.863578333333333]}

    # Setting 2:
    # success_rate_per_label = {'option_1': [0.1183136143976004, 0.9951674720879853, 0.9991668055324112, 1.0, 1.0],
    #  'option_2': [0.42626228961839696, 0.9981669721713048, 1.0, 1.0, 1.0],
    #  'option_3': [0.2837860356607232, 0.9966672221296451, 0.9981669721713048, 0.9936677220463256, 0.9840026662222963],
    #  'option_4': [0.3514414264289285, 0.9941676387268789, 0.9948341943009499, 0.9778370271621396, 0.9691718046992168]}
    # avg_parallelism_per_label = {'option_1': [15.876626666666667, 16.53053166666667, 7.879413333333333, 7.50901, 7.47283],
    #  'option_2': [30.470266666666667, 10.005566666666667, 7.946825, 7.5119316666666665, 7.4872966666666665],
    #  'option_3': [18.0, 23.246483333333334, 7.451758333333333, 7.239298333333333, 7.18298],
    #  'option_4': [18.0, 13.12991, 7.265076666666666, 7.228191666666667, 7.113145]}

    exps_per_label_per_setting = {
        # 1: {
        #     "bisection-no-increase": [
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #     ],
        # },
        # 2: {
        #     "bisection-no-increase": [
        #         "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-1-2.0-1-when-linear-2op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-250-3000-100-1-true-1",
        #         "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-1-2.0-1-when-linear-2op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #         "autotune-setting2-true-streamsluice-streamsluice-60-100-200-2-0.2-1-2.0-1-when-linear-2op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-17-1000-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #     ],
        # },
        # 3: {
        #     "bisection-no-increase": [
        #         "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-1-2.0-1-when-gradient-4op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
        #         "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-1-2.0-1-when-gradient-4op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-true-1",
        #         "autotune-setting3-true-streamsluice-streamsluice-60-100-400-2-0.2-1-2.0-1-when-gradient-4op_line-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1",
        #     ],
        # },
        # 4: {
        #     "bisection-no-increase": [
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     ],
        # },
        # 5: {
        #     "bisection-no-increase": [
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-20-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-20-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-20-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-20-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     ],
        # },
        # 6: {
        #     "bisection-no-increase": [
        #         # "autotune-setting6-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-500-20000-500-3000-100-1-true-1",
        #         # "autotune-setting6-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-500-20000-1000-3000-100-1-true-1",
        #         # "autotune-setting6-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-500-20000-1500-3000-100-1-true-1",
        #         # "autotune-setting6-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-500-20000-2000-3000-100-1-true-1",
        #         # "autotune-setting6-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-500-20000-2500-3000-100-1-true-1",
        #         "autotune-setting6-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-500-20000-5000-3000-100-1-true-1",
        #         "autotune-setting6-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-500-20000-7500-3000-100-1-true-1",
        #         "autotune-setting6-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-13750-30-6250-10000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-500-20000-10000-3000-100-1-true-1",
        #     ],
        # },
        # 7: {
        #     "bisection-no-increase": [
        #         "autotune-setting7-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-changing_amplitude-sine-1split2join1-1260-14000-1200-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #         "autotune-setting7-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-changing_amplitude-sine-1split2join1-1260-14000-1200-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting7-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-changing_amplitude-sine-1split2join1-1260-14000-1200-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting7-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-changing_amplitude-sine-1split2join1-1260-14000-1200-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     ],
        # },
        # 8: {
        #     "bisection-no-increase": [
        #         "autotune-setting8-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-changing_amplitude-sine-1split2join1-1260-14000-600-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #         "autotune-setting8-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-changing_amplitude-sine-1split2join1-1260-14000-600-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting8-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-changing_amplitude-sine-1split2join1-1260-14000-600-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting8-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-changing_amplitude-sine-1split2join1-1260-14000-600-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     ],
        # },
        # 9: {
        #     "bisection-no-increase": [
        #         "autotune-setting9-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-changing_period-sine-1split2join1-1260-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #         "autotune-setting9-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-changing_period-sine-1split2join1-1260-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting9-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-changing_period-sine-1split2join1-1260-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting9-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-changing_period-sine-1split2join1-1260-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     ],
        # },
        # 10: {
        #     "bisection-no-increase": [
        #         "autotune-setting10-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-sine_with_spike-sine-1split2join1-660-12500-60-17500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #         "autotune-setting10-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-sine_with_spike-sine-1split2join1-660-12500-60-17500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting10-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-sine_with_spike-sine-1split2join1-660-12500-60-17500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting10-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-sine_with_spike-sine-1split2join1-660-12500-60-17500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     ],
        # },
        # 11: {
        #     "bisection-no-increase": [
        #         "autotune-setting11-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-sine_with_spike-sine-1split2join1-660-12500-60-15000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #         "autotune-setting11-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-sine_with_spike-sine-1split2join1-660-12500-60-15000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting11-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-sine_with_spike-sine-1split2join1-660-12500-60-15000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting11-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-sine_with_spike-sine-1split2join1-660-12500-60-15000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     ],
        # },
        # 12: {
        #     "bisection-no-increase": [
        #         "autotune-setting12-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-sine_shift-sine-1split2join1-1260-12500-60-15000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #         "autotune-setting12-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-sine_shift-sine-1split2join1-1260-12500-60-15000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting12-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-sine_shift-sine-1split2join1-1260-12500-60-15000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting12-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-sine_shift-sine-1split2join1-1260-12500-60-15000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     ],
        # },
        # 13: {
        #     "bisection-no-increase": [
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-5000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-10000-3000-100-1-true-1",
        #     ],
        # },
        # 14: {
        #     "bisection-no-increase": [
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-12500-900-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-12500-900-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-12500-900-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-12500-900-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-5000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-12500-900-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-10000-3000-100-1-true-1",
        #     ],
        # },
        # 15: {
        #     "bisection-no-increase": [
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-12500-180-15000-7500-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-12500-180-15000-7500-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-12500-180-15000-7500-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-12500-180-15000-7500-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-5000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-12500-180-15000-7500-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-10000-3000-100-1-true-1"
        #     ],
        # },
        # 16: {
        #     "bisection-no-increase": [
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-10000-1-20-1-10000-1-20-1-10000-17-500-10000-1000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-10000-1-20-1-10000-1-20-1-10000-17-500-10000-1500-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-10000-1-20-1-10000-1-20-1-10000-17-500-10000-2000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-10000-1-20-1-10000-1-20-1-10000-17-500-10000-5000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-10000-1-20-1-10000-1-20-1-10000-17-500-10000-10000-3000-100-1-true-1",
        #     ],
        # },
        # 17: {
        #     "bisection-no-increase": [
        #         "autotune-setting14-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-3-sine_two_phase-sine-1split2join1-1260-12500-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #         "autotune-setting14-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-3-sine_two_phase-sine-1split2join1-1260-12500-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting14-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-3-sine_two_phase-sine-1split2join1-1260-12500-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting14-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-3-sine_two_phase-sine-1split2join1-1260-12500-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #     ],
        # },
        # 18: {
        #     "bisection-no-increase": [
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-3-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-3-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-3-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #     ],
        # }
        # 19: {
        #     "bisection-no-increase": [
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-3-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-3-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-3-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-3-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-5000-3000-100-1-true-1",
        #         "autotune-setting13-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-3-linear_phase_change-sine-1split2join1-1860-10000-360-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-10000-3000-100-1-true-1",
        #     ],
        # }
        20: {
            "bisection-no-increase": [
                # "autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
                # "autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
                # "autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
                # "autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
                # "autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-3000-3000-100-1-true-1",
                # "autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-4000-3000-100-1-true-1",
                # "autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-5000-3000-100-1-true-1",
                # "autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-10000-3000-100-1-true-1",
                # "autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-20000-3000-100-1-true-1",
                # "autotune-setting15-true-streamsluice-streamsluice-90-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-3000-3000-100-1-true-1",
                # "autotune-setting15-true-streamsluice-streamsluice-90-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-4000-3000-100-1-true-1",
                #"autotune-setting15-true-streamsluice-streamsluice-90-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-5000-3000-100-1-true-1",
                #"autotune-setting15-true-streamsluice-streamsluice-90-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-10000-3000-100-1-true-1",
                #"autotune-setting13-true-streamsluice-streamsluice-90-100-300-1-2.0-3-0.25-linear_phase_change-sine-1split2join1-1860-12500-900-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-2000-3000-100-1-true-1",
                #"autotune-setting13-true-streamsluice-streamsluice-90-100-300-1-2.0-3-0.25-linear_phase_change-sine-1split2join1-1860-12500-900-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-5000-3000-100-1-true-1",
                "autotune-setting13-true-streamsluice-streamsluice-90-100-300-1-2.0-3-0.25-linear_phase_change-sine-1split2join1-1860-12500-900-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-10000-3000-100-1-true-1",
                #"autotune-setting13-true-streamsluice-streamsluice-90-100-300-1-2.0-3-0.25-linear_phase_change-sine-1split2join1-1860-12500-900-15000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-20000-3000-100-1-true-1",
            ],
        }
    }
    for setting_index, exps_per_label in exps_per_label_per_setting.items():
        success_rate_per_label = {}
        avg_parallelism_per_label = {}
        first_converge_time_per_label = {}
        converged_bar_per_label = {}
        user_limit_per_label = {}
        for label, exps in exps_per_label.items():
            success_rate_per_label[label] = []
            avg_parallelism_per_label[label] = []
            user_limit_per_label[label] = []
            first_converge_time_per_label[label] = []
            converged_bar_per_label[label] = []
            for exp_name in exps:
                latency_bar = int(exp_name.split('-')[-6])
                success_rate, first_converge_time, converged_bar = draw_latency_curves(raw_dir, output_dir + exp_name + '/', exp_name,
                                                                              window_size,
                                                                              start_time, exp_length, latency_bar)
                avg_parallelism, trash = draw_parallelism_curve(raw_dir, output_dir + exp_name + '/', exp_name, window_size,
                                                                start_time, exp_length, True)
                user_limit_per_label[label] += [latency_bar]
                success_rate_per_label[label] += [success_rate]
                avg_parallelism_per_label[label] += [avg_parallelism]
                first_converge_time_per_label[label] += [first_converge_time]
                converged_bar_per_label[label] += [converged_bar]
        print(success_rate_per_label)
        print(avg_parallelism_per_label)
        print(first_converge_time_per_label)
        user_limits = user_limit_per_label["bisection-no-increase"]
        plot_success_rate(user_limits, success_rate_per_label, overall_output_dir, setting_index)
        plot_avg_parallelism(user_limits, avg_parallelism_per_label, overall_output_dir, setting_index)
        plot_converge_time(user_limits, first_converge_time_per_label, overall_output_dir, setting_index)
        plot_converged_bar(user_limits, converged_bar_per_label, overall_output_dir, setting_index)

if __name__ == "__main__":
    main()

