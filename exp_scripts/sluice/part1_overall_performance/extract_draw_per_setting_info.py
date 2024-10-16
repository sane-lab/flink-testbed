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
            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[MODEL]" and split[6] == "cur_ete_l:" and (
                    split[
                        8] == "n_epoch_l:" or split[11] == "n_epoch_l:")):
                time = int(split[3])
                estimated_l = float(split[7])
                # estimated_spike = float(split[13]) - float(split[7])
                lem_latency[0] += [time]
                lem_latency[1] += [estimated_l]
                # lem_latency[2] += [estimated_spike]
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

def draw_latency_curves(raw_dir, output_dir, exp_name, window_size, start_time, exp_length, latency_limit, draw_lem_latency_flag):
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
        result[0][0] = [x - initial_times[i] for x in result[0][0]]
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

        def compute_weighted_success_rate(average_ground_truth_latencies, start_time, exp_length, latency_limit,
                                          window_size=30):
            total_windows = int(
                exp_length / window_size)  # Total number of windows based on the experiment length and window size
            overall_wsr = 0
            total_weight = 0

            for i in range(len(average_ground_truth_latencies)):
                # Split the workload into windows
                window_wsr = []
                for window_idx in range(total_windows):
                    window_start = start_time + window_idx * window_size
                    window_end = window_start + window_size

                    # Get groundtruth latencies within the current window
                    latencies_in_window = [average_ground_truth_latencies[i][1][x] for x in
                                           range(len(average_ground_truth_latencies[i][0]))
                                           if average_ground_truth_latencies[i][0][x] >= window_start * 1000 and
                                           average_ground_truth_latencies[i][0][x] <= window_end * 1000]

                    # Calculate success rate in this window (WSR_i)
                    if len(latencies_in_window) > 0:
                        time_under_limit_in_window = len([x for x in latencies_in_window if x <= latency_limit])
                        wsr_i = time_under_limit_in_window / len(latencies_in_window)
                    else:
                        wsr_i = 0  # No data in the window, treat it as 0 success rate

                    # Calculate the weight for this window: w_i = 1 + log(i + 1)
                    weight = 1 + math.log(window_idx + 1)

                    # Accumulate weighted success rate and weight
                    overall_wsr += weight * wsr_i
                    total_weight += weight

                    # Optionally, store window-wise WSR for debugging or further analysis
                    window_wsr.append((wsr_i, weight))

                # Final Weighted Success Rate (WSR) for the workload
                if total_weight > 0:
                    final_wsr = overall_wsr / total_weight
                else:
                    final_wsr = 0

                print(f"Weighted Success Rate for workload {i}: {final_wsr}")

            return final_wsr
        weighted_success_rate = compute_weighted_success_rate(average_ground_truth_latencies, start_time, exp_length, latency_limit, 30)
        print("Success rate: " + str(success_rate))
        print("Weighted success rate: " + str(weighted_success_rate))
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
        if(draw_lem_latency_flag):
            plt.plot(lem_latencies[i][0], lem_latencies[i][1], '-', color="green", markersize=2, linewidth=2,
                 label='Estimated Latency')
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

    axes.set_ylim(0, 5000)
    axes.set_yticks(np.arange(0, 5500, 500))
    # axes.set_ylim(0, 10000)
    # axes.set_yticks(np.arange(0, 11000, 1000))
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

    return success_rate, weighted_success_rate, first_converge_time, converged_bar

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
    # ax2.set_ylim(0, 30000)
    # ax2.set_yticks(np.arange(0, 35000, 5000))
    if max(ay) <= 2000:
        ax2.set_ylim(500, 2000)
        ax2.set_yticks(np.arange(500, 2100, 200))
    else:
        ax2.set_ylim(1000, 4000)
        ax2.set_yticks(np.arange(1000, 4500, 500))
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
        #ax1.set_ylim(10, 60) #(4, 32) #17)
        #ax1.set_yticks(np.arange(10, 70, 5)) # (4, 34, 2)) #18, 1))
        ax1.set_ylim(0, 45) #(4, 32) #17)
        ax1.set_yticks(np.arange(0, 45, 5)) # (4, 34, 2)) #18, 1))

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
def plot_success_rate_bar(user_limits, success_rate_per_label, output_dir, workload_name: str):
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
    plt.savefig(output_dir + 'success_rate_by_strategy_' + str(workload_name) + '.png', bbox_inches='tight')
    plt.close(fig)

# Function to plot avg parallelism
def plot_avg_parallelism_bar(user_limits, avg_parallelism_per_label, output_dir, workload_name:str):
    labels = list(avg_parallelism_per_label.keys())
    bar_width = 0.2
    x = np.arange(len(user_limits))

    fig, axs = plt.subplots(figsize=(12, 5))

    # Plot avg parallelism
    for idx, label in enumerate(labels):
        plt.bar(x + idx * bar_width, avg_parallelism_per_label[label], bar_width, label=("$\\alpha$="+label))

    plt.xlabel('User Limits')
    plt.ylabel('Avg Parallelism')
    plt.xticks(x + bar_width * (len(labels) - 1) / 2, user_limits)
    plt.title('Avg Parallelism by User Limits and Strategy')
    plt.legend()
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'resource_vs_strategy_' + str(workload_name) + '.png', bbox_inches='tight')
    plt.close(fig)


# Function to plot success rate as curves
def plot_success_rate_curve(user_limits_per_label, success_rate_per_label, output_dir, workload_name: str):
    labels = list(success_rate_per_label.keys())

    fig, axs = plt.subplots(figsize=(12, 5))

    # Plot success rate curves
    for label in labels:
        plt.plot(user_limits_per_label[label], success_rate_per_label[label], marker='o', label=("$\\alpha$="+label))

    plt.xlabel('User Limits')
    plt.ylabel('Success Rate')
    plt.ylim(0.95, 1.01)
    plt.yticks(np.arange(0.95, 1.01, 0.01))
    plt.xticks(user_limits_per_label[label])
    plt.title('Success Rates by User Limits and Strategy')
    plt.legend()
    plt.grid(True)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'success_rate_curve_' + str(workload_name) + '.png', bbox_inches='tight')
    plt.close(fig)

def plot_weighted_success_rate_curve(user_limits_per_label, weighted_success_rate_per_label, output_dir, workload_name: str):
    labels = list(weighted_success_rate_per_label.keys())

    fig, axs = plt.subplots(figsize=(12, 5))

    # Plot success rate curves
    for label in labels:
        plt.plot(user_limits_per_label[label], weighted_success_rate_per_label[label], marker='o', label=("$\\alpha$="+label))

    plt.xlabel('User Limits')
    plt.ylabel('Weighted Success Rate')
    plt.ylim(0.95, 1.01)
    plt.yticks(np.arange(0.95, 1.01, 0.01))
    plt.xticks(user_limits_per_label[label])
    plt.title('Weighted Success Rates by User Limits and Strategy')
    plt.legend()
    plt.grid(True)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'weighted_success_rate_curve_' + str(workload_name) + '.png', bbox_inches='tight')
    plt.close(fig)


# Function to plot average parallelism as curves
def plot_avg_parallelism_curve(user_limits_per_label, avg_parallelism_per_label, output_dir, workload_name: str):
    labels = list(avg_parallelism_per_label.keys())

    fig, axs = plt.subplots(figsize=(12, 5))

    # Plot average parallelism curves
    for label in labels:
        plt.plot(user_limits_per_label[label], avg_parallelism_per_label[label], marker='o', label=label)

    plt.xticks(user_limits_per_label[label])
    plt.xlabel('User Limits')
    plt.ylabel('Avg Parallelism')
    plt.title('Avg Parallelism by User Limits and Strategy')
    plt.legend()
    plt.grid(True)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'avg_parallelism_curve_' + str(workload_name) + '.png', bbox_inches='tight')
    plt.close(fig)

def main():
    raw_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
    output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
    overall_output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/part1/"
    window_size = 100
    draw_lem_latency_flag = True
    exps_per_label_per_setting = {
        # "Stock": {
        #     "0.1": [
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-750-100-0.1-true-true-2",
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1000-100-0.1-true-true-2",
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1500-100-0.1-true-true-2",
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-2000-100-0.1-true-true-2",
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-2500-100-0.1-true-true-2",
        #     ],
        #     "0.2": [
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-750-100-0.2-true-true-2",
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1000-100-0.2-true-true-2",
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1500-100-0.2-true-true-2",
        #         #"stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-2000-100-0.2-true-true-2",
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-2500-100-0.2-true-true-2",
        #     ],
        #     "0.4": [
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-750-100-0.4-true-true-2",
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1000-100-0.4-true-true-2",
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1500-100-0.4-true-true-2",
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-2000-100-0.4-true-true-2",
        #         "stock-streamsluice-streamsluice-1-720-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-2500-100-0.4-true-true-2",
        #     ],
        # },
        # "Twitter": {
        #     "0.1": [
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-750-100-true-0.1-2",
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-1000-100-true-0.1-2",
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-1500-100-true-0.1-2",
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-2000-100-true-0.1-2",
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-2500-100-true-0.1-2",
        #     ],
        #     "0.2": [
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-750-100-true-0.2-2",
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-1000-100-true-0.2-2",
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-1500-100-true-0.2-2",
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-2000-100-true-0.2-2",
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-2500-100-true-0.2-2",
        #     ],
        #     "0.4": [
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-750-100-true-0.4-2",
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-1000-100-true-0.4-2",
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-1500-100-true-0.4-2",
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-2000-100-true-0.4-2",
        #         "tweet-streamsluice-streamsluice-1-720-90-1800-1-28-5000-10-1000-1-50-1-50-2500-100-true-0.4-2",
        #     ],
        # },
        "Linear-Road": {
            "0.1": [
            #    "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-1000-0.1-100-1-0-0.0-true-3000-1",
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-2000-0.1-100-1-0-0.0-true-3000-1",
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-3000-0.1-100-1-0-0.0-true-3000-1",
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-4000-0.1-100-1-0-0.0-true-3000-1",
            #    "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-5000-0.1-100-1-0-0.0-true-3000-1",
            ],
            "0.2": [
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-1000-0.2-100-1-0-0.0-true-3000-1",
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-2000-0.2-100-1-0-0.0-true-3000-1",
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-3000-0.2-100-1-0-0.0-true-3000-1",
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-4000-0.2-100-1-0-0.0-true-3000-1",
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-5000-0.2-100-1-0-0.0-true-3000-1",
            ],
            "0.4": [
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-1000-0.4-100-1-0-0.0-true-3000-1",
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-2000-0.4-100-1-0-0.0-true-3000-1",
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-3000-0.4-100-1-0-0.0-true-3000-1",
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-4000-0.4-100-1-0-0.0-true-3000-1",
                "lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-5000-0.4-100-1-0-0.0-true-3000-1",
            ],
        }
    }
    for workload_name, exps_per_label in exps_per_label_per_setting.items():
        success_rate_per_label = {}
        avg_parallelism_per_label = {}
        user_limit_per_label = {}
        weighted_success_rate_per_label = {}
        for label, exps in exps_per_label.items():
            success_rate_per_label[label] = []
            avg_parallelism_per_label[label] = []
            user_limit_per_label[label] = []
            weighted_success_rate_per_label[label] = []

            for exp_name in exps:
                if exp_name.startswith("lr"):
                    latency_bar = int(exp_name.split('-')[-9])
                    start_time = 180
                    exp_length = 600
                elif exp_name.startswith("tweet"):
                    latency_bar = int(exp_name.split('-')[-5])
                    start_time = 120
                    exp_length = 600
                else:
                    latency_bar = int(exp_name.split('-')[-6])
                    start_time = 120
                    exp_length = 600
                success_rate, weighted_success_rate, first_converge_time, converged_bar = draw_latency_curves(raw_dir, output_dir + exp_name + '/', exp_name,
                                                                              window_size,
                                                                              start_time, exp_length, latency_bar, draw_lem_latency_flag)
                avg_parallelism, trash = draw_parallelism_curve(raw_dir, output_dir + exp_name + '/', exp_name, window_size,
                                                                start_time, exp_length, True)
                user_limit_per_label[label] += [latency_bar]
                success_rate_per_label[label] += [success_rate]
                weighted_success_rate_per_label[label] += [weighted_success_rate]
                avg_parallelism_per_label[label] += [avg_parallelism]
        print(success_rate_per_label)
        print(weighted_success_rate_per_label)
        print(avg_parallelism_per_label)
        #user_limits = user_limit_per_label["0.1"]
        #plot_success_rate_bar(user_limits, weighted_success_rate_per_label, overall_output_dir, workload_name)
        plot_success_rate_curve(user_limit_per_label, success_rate_per_label, overall_output_dir, workload_name)
        plot_weighted_success_rate_curve(user_limit_per_label, weighted_success_rate_per_label, overall_output_dir, workload_name)
        #plot_avg_parallelism_bar(user_limits, avg_parallelism_per_label, overall_output_dir, workload_name)
        plot_avg_parallelism_curve(user_limit_per_label, avg_parallelism_per_label, overall_output_dir, workload_name)

if __name__ == "__main__":
    main()

