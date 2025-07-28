"""
Enhanced Flink Part8 Overhead Analysis Script

This script analyzes CPU overhead using TaskManagerRunner-focused monitoring data.
It supports both the new enhanced monitoring format and legacy format for backward compatibility.

NEW FEATURES (TaskManagerRunner-focused):
- Reads taskmanager_cycles_*.txt files for PRIMARY CPU overhead analysis
- Parses enhanced monitor logs with [PRIMARY] and [secondary] process labels
- Calculates CPU cycle-based overhead (most accurate for research)
- Provides IPC (Instructions Per Cycle) analysis
- Comprehensive CSV output with detailed metrics

QUICK USAGE:
    # For a single workload analysis:
    from part8_extract_latency_and_overhead import calculate_overhead
    
    baseline_dir = "/path/to/experiment-without-sluice/"
    sluice_dir = "/path/to/experiment-with-sluice/"
    output_dir = "/path/to/results/"
    
    calculate_overhead(baseline_dir, sluice_dir, output_dir)

FILES ANALYZED:
- taskmanager_cycles_*.txt (PRIMARY CPU cycle data - middle 20 minutes only)
- monitor_*.out (Enhanced monitoring logs)
- total_cycles_*.txt (Secondary reference data)

KEY FEATURE:
- Analyzes only the MIDDLE 20 MINUTES of each experiment for stable comparison
- Avoids startup/warmup and shutdown effects
- Ensures better alignment between baseline and Sluice experiments

OUTPUT:
- enhanced_overhead_analysis.csv (Comprehensive results)
"""

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
                    "n_epoch_l:" in split)):
                time = int(split[3])
                estimated_l = float(split[7])
                # estimated_spike = float(split[13]) - float(split[7])
                lem_latency[0] += [time]
                lem_latency[1] += [estimated_l]
                # lem_latency[2] += [estimated_spike]
            if (len(split) >= 8 and split[0] == "[AUTOTUNE]" and split[4] == "initial" and split[5] == "latency" and
                    split[6] == "bar:"):
                time = int(split[2])
                bar = int(split[7].rstrip(','))
                latency_bar[time] = bar
            if (len(split) >= 8 and split[1] == "[AUTOTUNE]" and split[4] == "user" and split[5] == "limit" and split[
                6] == "is"):
                time = int(split[3])
                for index in range(7, 15):
                    if (split[index] == "bar:"):
                        bar = int(split[index + 1].rstrip(','))
                        p99 = int(split[index + 4].rstrip(','))
                        break
                latency_bar[time] = bar
                p99_bar[time] = p99
            if (len(split) >= 8 and split[1] == "[AUTOTUNE]" and split[4] == "set" and split[5] == "bar" and split[
                6] == "to" and split[7] == "lowerbound:"):
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


def add_latency_bar_curve(plt, latency_bar: dict[int, int], initial_time):
    last_time = 0
    last_y = 0
    for time in latency_bar.keys():
        x = [last_time, time - initial_time]
        y = [last_y, last_y]
        plt.plot(x, y, 'o--', label="Latency Bound", color='orange', linewidth=1.5)
        last_y = latency_bar[time]
        last_time = time - initial_time
    x = [last_time, 10000000]
    y = [last_y, last_y]
    plt.plot(x, y, 'o--', label="Latency Bound", color='orange', linewidth=1.5)


def readLEMLatencyAndSpikeAndBarAndScalingMarker(rawDir, expName) -> [[list[int], list[float], list[float]], dict[int, int]]:
    lem_latency = [[], [], []]
    latency_bar = {}
    p99_bar = {}
    scalings = [[], []]

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
            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale" and split[
                8] == "operator:"):
                time = int(split[3])
                scalings[0].append(time)
                scalings[1].append(0)

            if (len(split) >= 8 and split[0] == "+++" and split[1] == "[CONTROL]" and split[4] == "all" and split[
                5] == "scaling" and split[6] == "plan" and split[7] == "deployed."):
                time = int(split[3])
                scalings[0].append(time)
                scalings[1].append(1)

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

    return [lem_latency, latency_bar, p99_bar, scalings]

def draw_latency_curves(raw_dir, output_dir, exp_name, window_size, start_time, exp_length, latency_limit,
                        draw_lem_latency_flag):
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
        result = readLEMLatencyAndSpikeAndBarAndScalingMarker(raw_dir, exps[i][1])
        result[0][0] = [x - initial_times[i] for x in result[0][0]]
        result[3][0] = [x - initial_times[i] for x in result[3][0]]
        lem_latencies += [result[0]]
        latency_bar += [result[1]]
        p99_bar += [result[2]]
    # print(p99_bar)
    for i in range(len(exps)):
        groundtruth_p99_latency_in_range = [average_ground_truth_latencies[i][1][x] for x in
                                            range(len(average_ground_truth_latencies[i][0])) if
                                            average_ground_truth_latencies[i][0][x] >= start_time * 1000 and
                                            average_ground_truth_latencies[i][0][x] <= (start_time + exp_length) * 1000]
        success_rate = len([x for x in groundtruth_p99_latency_in_range if x <= latency_limit]) / len(
            groundtruth_p99_latency_in_range)
        avg_ground_truth_latency_in_range = sum(groundtruth_p99_latency_in_range)/len(groundtruth_p99_latency_in_range)

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

        weighted_success_rate = compute_weighted_success_rate(average_ground_truth_latencies, start_time, exp_length,
                                                              latency_limit, 30)
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

        plt.plot(sampled_latency[0], sampled_latency[1], '-', color="blue", markersize=4, linewidth=3,
                 label="Ground Truth P99")
        if (draw_lem_latency_flag):
            plt.plot(lem_latencies[i][0], lem_latencies[i][1], '-', color="green", markersize=2, linewidth=2,
                     label='Estimated Latency')
            add_latency_bar_curve(plt, latency_bar[i], initial_times[i])
        add_latency_limit_marker(plt, latency_limit)
        # add_scaling_marker(plt, scalings[i])

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
    axes.set_xticks(np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + (exp_length / 10) * 1000,
                              (exp_length / 10) * 1000))
    axes.set_xticklabels([int((x - start_time * 1000) / 1000) for x in
                          np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + (exp_length / 10) * 1000,
                                    (exp_length / 10) * 1000)])

    if max(sampled_latency[1]) <= 2000:
        axes.set_ylim(0, 2000)
        axes.set_yticks(np.arange(0, 2200, 200))
    else:
        axes.set_ylim(0, 5000)
        axes.set_yticks(np.arange(0, 5500, 500))
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.close(fig)

    # Calculate the bar converge time
    # tune_window_success_rates = {}
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
    # print("tune window success rates: " + str(tune_window_success_rates))
    # first_converge_time = 0
    # index = 0
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
    axes.set_xticks(np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + (exp_length / 10) * 1000,
                              (exp_length / 10) * 1000))
    axes.set_xticklabels([int((x - start_time * 1000) / 1000) for x in
                          np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + (exp_length / 10) * 1000,
                                    (exp_length / 10) * 1000)])
    if (latency_limit < 2000):
        axes.set_ylim(0, 2000)
        axes.set_yticks(np.arange(0, 2200, 200))
    elif (latency_limit < 5000):
        axes.set_ylim(0, 5500)
        axes.set_yticks(np.arange(0, 5500, 500))
    else:
        axes.set_ylim(0, 25000)
        axes.set_yticks(np.arange(0, 27500, 2500))
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'latency_bar.png', bbox_inches='tight')
    plt.close(fig)

    return success_rate, avg_ground_truth_latency_in_range, first_converge_time, converged_bar




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

    taskExecutors = []  # "flink-samza-taskexecutor-0-eagle-sane.out"
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
                if (split[0] == "GT:"):
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
            index = ((delta_x + first_ax) // windowSize) * windowSize  # math.floor(ax / windowSize) * windowSize
            ay = arrivalRatePerTask[task][1][i]
            if index not in totalArrivalRatePerJob[job]:
                totalArrivalRatePerJob[job][index] = ay
            else:
                totalArrivalRatePerJob[job][index] += ay

    print(expName, ParallelismPerJob.keys())
    return [ParallelismPerJob, totalArrivalRatePerJob, initialTime, scalings]


def draw_parallelism_curve(rawDir, outputDir, exp_name, windowSize, startTime, exp_length, draw_parallelism_flag, static_arrival_curve) -> [
    float, [list[int], list[float]]]:
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
    jobList = ["a84740bacf923e828852cc4966f2247c", "eabd4c11f6c6fbdf011f0f1fc42097b1",
               "d01047f852abd5702a0dabeedac99ff5", "d2336f79a0d60b5a4b16c8769ec82e47",
               "feccfb8648621345be01b71938abfb72"]
    fig, axs = plt.subplots(1, 1, figsize=(12, 5), layout='constrained')
    # Add super label
    # fig.supylabel('# of Slots')
    # supylabel2(fig, "Arrival Rate (tps)")
    fig.tight_layout(rect=[0.02, 0, 0.953, 1])
    axs.grid(True)
    if (draw_parallelism_flag):
        ax1 = axs
        ax2 = ax1.twinx()
    else:
        ax2 = axs
        ax2.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
        ax2.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + (exp_length / 10) * 1000,
                                 (exp_length / 10) * 1000))
        ax2.set_xticklabels([int((x - startTime * 1000) / 1000) for x in
                             np.arange(startTime * 1000, (startTime + exp_length) * 1000 + (exp_length / 10) * 1000,
                                       (exp_length / 10) * 1000)])
        ax2.set_xlabel("Time (s)")

    ax2.set_ylabel("Arrival Rate (tps)")

    job = jobList[0]
    ax = sorted(totalArrivalRatesPerJob[job][0].keys())
    ay = [totalArrivalRatesPerJob[job][0][x] / (windowSize / 100) for x in ax]
    arrival_curves = []
    if static_arrival_curve == []:
        arrival_curves = [ax, ay]
    else:
        arrival_curves = static_arrival_curve
        ax, ay = static_arrival_curve
    ax2.plot(ax, ay, '-', color='red', markersize=MARKERSIZE / 2, label="Arrival Rate")
    # ax2.set_ylabel('Rate (tps)')
    # ax2.set_ylim(0, 30000)
    # ax2.set_yticks(np.arange(0, 35000, 5000))
    #if max(ay) <= 8000:
    ax2.set_ylim(2000, 9000)
    ax2.set_yticks(np.arange(2000, 11000, 1000))
    # else:
    #     ax2.set_ylim(0, 20000)
    #     ax2.set_yticks(np.arange(1000, 4500, 500))
    legend = ["Arrival Rate"]
    # ax2.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
    # ax2.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 300000, 300000))
    # ax2.set_xticklabels([int((x - startTime * 1000) / 60000) for x in
    #                      np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000)])
    ax2.legend(legend, loc='upper right', bbox_to_anchor=(1.1, 1.3), ncol=1)
    average_parallelism = 0.0
    if (draw_parallelism_flag):
        ax1.set_ylabel("# of Slots")
        legend = []
        scalingPoints = [[], []]
        for expindex in range(0, len(exps)):
            if (exps[expindex][0] == "Static"):
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
                if (exps[expindex][0] == 'Sluice' and l < r):
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
        ax1.plot(scalingPoints[0], scalingPoints[1], 'o', color="orange", mfc='none', markersize=MARKERSIZE * 2,
                 label="Scaling")
        ax1.legend(legend, loc='upper left', bbox_to_anchor=(-0.1, 1.3), ncol=3, markerscale=4.)
        # ax1.set_ylabel('OP_'+str(jobIndex+1)+' Parallelism')
        ax1.set_ylim(10, 60)
        ax1.set_yticks(np.arange(10, 65, 5))  # (4, 34, 2)) #18, 1))

        ax1.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
        ax1.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + (exp_length / 10) * 1000,
                                 (exp_length / 10) * 1000))
        ax1.set_xticklabels([int((x - startTime * 1000) / 1000) for x in
                             np.arange(startTime * 1000, (startTime + exp_length) * 1000 + (exp_length / 10) * 1000,
                                       (exp_length / 10) * 1000)])
        ax1.set_xlabel("Time (s)")

    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    # plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)
    return average_parallelism, arrival_curves


def main():
    raw_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
    output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
    overall_output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/part8/"
    window_size = 100
    draw_lem_latency_flag = True

    exps_per_label_per_setting = {
        # "Stock": {
        #     "Without_Sluice": "part8-stock-NoControll-5-8-60-1350-90-1000-20-1-200-11-3333-1-200-2-500-1-15-5000-3000-100-0.1-false-false-1",
        #     "With_Sluice": "part8-stock-StreamSluice-5-8-60-1350-90-1000-20-1-200-11-3333-1-200-2-500-1-15-5000-3000-100-0.1-false-true-1",
        # },
        # "Twitter": {
        #     "Without_Sluice": "part8-tweet-NoControll-5-60-1350-90-1700-1-19-3333-9-500-1-50-1-50-1250-2000-100-false-0.1-1",
        #     "With_Sluice": "part8-tweet-StreamSluice-5-60-1350-90-1700-1-19-3333-9-500-1-50-1-50-1250-2000-100-false-0.1-1",
        # },
        "Linear-road": {
            "Without_Sluice": "part8-lr-NoControll-5-8-60-380-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-2",
            "With_Sluice": "part8-lr-StreamSluice-5-8-60-380-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-2",
        }
    }
    def getStartTimeAndExpLength(exp_name):
        if exp_name.startswith("part8-lr"):
            latency_bar = int(exp_name.split('-')[-10])
            start_time = 180
            exp_length = 1200
        elif exp_name.startswith("part8-tweet"):
            latency_bar = int(exp_name.split('-')[-5])
            start_time = 150
            exp_length = 1200  # 600
        elif exp_name.startswith("part8-stock"):
            latency_bar = int(exp_name.split('-')[-6])
            start_time = 150
            exp_length = 1200
        elif exp_name.startswith("part8-micro"):
            latency_bar = int(exp_name.split('-')[-6])
            start_time = 60 #60
            exp_length = 900 #2400 #1440 #900  # 1800
        else:
            latency_bar = int(exp_name.split('-')[-6])
            start_time = 60
            exp_length = 600
        return start_time, exp_length, latency_bar

    for workload_name, exps_per_label in exps_per_label_per_setting.items():
        success_rate_per_label = {}
        avg_parallelism_per_label = {}
        user_limit_per_label = {}
        avg_ground_truth_latency_per_label = {}
        static_arrival_curve = []
        dir_without_sluice = ""
        dir_with_sluice = ""
        for label, exp_name in exps_per_label.items():
            success_rate_per_label[label] = []
            avg_parallelism_per_label[label] = []
            user_limit_per_label[label] = []
            avg_ground_truth_latency_per_label[label] = []
            if label == "Without_Sluice":
                dir_without_sluice = raw_dir + exp_name
            if label == "With_Sluice":
                dir_with_sluice = raw_dir + exp_name
                start_time, exp_length, latency_bar = getStartTimeAndExpLength(exp_name)
                success_rate, avg_ground_truth_latency, first_converge_time, converged_bar = draw_latency_curves(raw_dir,
                                                                                                             output_dir + exp_name + '/',
                                                                                                             exp_name,
                                                                                                             window_size,
                                                                                                             start_time,
                                                                                                             exp_length,
                                                                                                             latency_bar,
                                                                                                             draw_lem_latency_flag)
                avg_parallelism, trash = draw_parallelism_curve(raw_dir, output_dir + exp_name + '/', exp_name,
                                                                window_size,
                                                                start_time, exp_length, True, static_arrival_curve)
                user_limit_per_label[label] += [latency_bar]
                success_rate_per_label[label] += [success_rate]
                avg_ground_truth_latency_per_label[label] += [avg_ground_truth_latency]
                avg_parallelism_per_label[label] += [avg_parallelism]
        print(success_rate_per_label)
        print(avg_ground_truth_latency_per_label)
        print(avg_parallelism_per_label)
        calculate_overhead(dir_without_sluice, dir_with_sluice, overall_output_dir + workload_name + "/", 5)


import pandas as pd

def read_log(file_path):
    """Read the log file and return a DataFrame."""
    return pd.read_csv(file_path, skipinitialspace=True)

def read_enhanced_log(file_path):
    """
    Read the enhanced monitoring log with TaskManagerRunner [PRIMARY] and [secondary] labels.
    
    Args:
        file_path (str): Path to the monitor log file.
        
    Returns:
        tuple: (taskmanager_df, secondary_df, combined_df) DataFrames for different process types.
    """
    if not file_path or not os.path.exists(file_path):
        print(f"Enhanced log file not found: {file_path}")
        return None, None, None
        
    print(f"Reading enhanced monitoring log from: {file_path}")
    
    try:
        # Read the full log
        df = pd.read_csv(file_path, skipinitialspace=True)
        
        # Filter TaskManagerRunner (PRIMARY) data
        taskmanager_df = df[df['Process Name'].str.contains('TaskManagerRunner.*PRIMARY', na=False, regex=True)].copy()
        
        # Filter secondary process data
        secondary_df = df[df['Process Name'].str.contains('secondary', na=False, regex=True)].copy()
        
        # Clean up process names for easier analysis
        if not taskmanager_df.empty:
            taskmanager_df['Process Name'] = 'TaskManagerRunner'
        
        if not secondary_df.empty:
            secondary_df['Process Name'] = secondary_df['Process Name'].str.replace(r'\s*\[secondary\]', '', regex=True)
        
        print(f"TaskManagerRunner records: {len(taskmanager_df)}")
        print(f"Secondary process records: {len(secondary_df)}")
        
        return taskmanager_df, secondary_df, df
        
    except Exception as e:
        print(f"Error reading enhanced log file: {e}")
        return None, None, None


def convert_time_to_seconds(time_str):
    """
    Convert TOTAL_CPU_TIME from 'MM:SS' format to seconds.

    Args:
        time_str (str): Time in MM:SS format.

    Returns:
        float: Time in seconds.
    """
    minutes, seconds = map(float, time_str.split(':'))
    return minutes * 60 + seconds


def calculate_metrics(df):
    """
    Calculate average CPU, memory, and total CPU time for each process.

    Args:
        df (pd.DataFrame): Log data as a DataFrame.

    Returns:
        pd.DataFrame: Aggregated metrics.
    """
    if 'TOTAL_CPU_TIME' in df.columns:
        df['TOTAL_CPU_TIME (s)'] = df['TOTAL_CPU_TIME'].apply(convert_time_to_seconds)
        
        metrics = df.groupby("Process Name").agg({
            "CPU%": "mean",
            "TOTAL_CPU_TIME (s)": "max",  # Use the maximum accumulated value
            "RSS (KB)": "mean",
            "GC Time (ms)": "max"  # Sum GC time since it's a cumulative metric
        }).rename(columns={
            "CPU%": "Avg CPU%",
            "RSS (KB)": "Avg RSS (KB)",
            "GC Time (ms)": "Total GC Time (ms)",
            "TOTAL_CPU_TIME (s)": "Total CPU Time (s)"
        })
    else:
        # Handle new monitoring format with CPU cycles
        agg_dict = {
            "Heap Used (MB)": "mean",
            "GC Time (ms)": "max",  # Maximum accumulated GC time
            "Interval Cycles": "sum",  # Sum all interval cycles
            "Instructions": "sum",  # Sum all instructions
            "Cache Misses": "sum"  # Sum all cache misses
        }
        
        # Check if Total Cycles column exists (it shows per-process accumulated cycles)
        if 'Total Cycles (per process)' in df.columns:
            agg_dict["Total Cycles (per process)"] = "max"
        
        metrics = df.groupby("Process Name").agg(agg_dict)
        
        # Calculate IPC (Instructions Per Cycle) for each process
        if "Instructions" in metrics.columns and "Interval Cycles" in metrics.columns:
            metrics["IPC"] = metrics["Instructions"] / metrics["Interval Cycles"].replace(0, 1)  # Avoid division by zero
        
        metrics = metrics.rename(columns={
            "Heap Used (MB)": "Avg Heap Used (MB)",
            "GC Time (ms)": "Total GC Time (ms)",
            "Interval Cycles": "Total Interval Cycles",
            "Instructions": "Total Instructions",
            "Cache Misses": "Total Cache Misses",
            "Total Cycles (per process)": "Max Accumulated Cycles"
        })
    
    return metrics

def calculate_enhanced_metrics(taskmanager_df, secondary_df, tm_cycles_data):
    """
    Calculate comprehensive metrics using both monitoring logs and TaskManagerRunner cycles data.
    
    Args:
        taskmanager_df (pd.DataFrame): TaskManagerRunner monitoring data.
        secondary_df (pd.DataFrame): Secondary processes monitoring data.
        tm_cycles_data (dict): TaskManagerRunner cycles accumulator data.
        
    Returns:
        dict: Comprehensive metrics including CPU cycles, GC time, and traditional metrics.
    """
    metrics = {}
    
    # TaskManagerRunner metrics from cycles file (most accurate)
    if tm_cycles_data:
        metrics['TaskManagerRunner'] = {
            'Total CPU Cycles': tm_cycles_data['total_cycles'],
            'Total Instructions': tm_cycles_data['total_instructions'],
            'Total GC Time (ms)': tm_cycles_data['total_gc_time_ms'],
            'IPC (Instructions Per Cycle)': tm_cycles_data['ipc'],
            'CPU Cycles (Billions)': tm_cycles_data['total_cycles'] / 1e9,  # For easier reading
            'Source': 'TaskManager Cycles File (PRIMARY)'
        }
    
    # Add monitoring log metrics for TaskManagerRunner
    if taskmanager_df is not None and not taskmanager_df.empty:
        tm_monitor_metrics = calculate_metrics(taskmanager_df)
        if 'TaskManagerRunner' in tm_monitor_metrics.index:
            tm_data = tm_monitor_metrics.loc['TaskManagerRunner']
            if 'TaskManagerRunner' not in metrics:
                metrics['TaskManagerRunner'] = {}
            
            # Add monitoring data (may have additional info like heap usage)
            for col in tm_data.index:
                if col not in metrics['TaskManagerRunner']:
                    metrics['TaskManagerRunner'][col] = tm_data[col]
    
    # Secondary processes metrics
    if secondary_df is not None and not secondary_df.empty:
        secondary_metrics = calculate_metrics(secondary_df)
        for process_name in secondary_metrics.index:
            metrics[f'{process_name} (Secondary)'] = secondary_metrics.loc[process_name].to_dict()
            metrics[f'{process_name} (Secondary)']['Source'] = 'Monitor Log (SECONDARY)'
    
    return metrics

def calculate_overall_metrics(metrics):
    """Calculate overall metrics by summing or averaging across all processes."""
    overall = metrics.sum().rename("Overall")
    overall["Avg CPU%"] = metrics["Avg CPU%"].mean()  # Use average for CPU%
    return overall

def compare_metrics(metrics1, metrics2):
    """
    Compare metrics between two DataFrames to calculate overhead.

    Args:
        metrics1 (pd.DataFrame): Baseline metrics.
        metrics2 (pd.DataFrame): Sluice metrics.

    Returns:
        pd.DataFrame: Overhead comparison.
    """
    comparison = metrics2 - metrics1
    comparison["CPU Overhead%"] = (comparison["Avg CPU%"] / metrics1["Avg CPU%"]) * 100
    comparison["RSS Overhead%"] = (comparison["Avg RSS (KB)"] / metrics1["Avg RSS (KB)"]) * 100
    comparison["GC Time Overhead%"] = (comparison["Total GC Time (ms)"] / metrics1["Total GC Time (ms)"]) * 100
    comparison["CPU Time Overhead%"] = (comparison["Total CPU Time (s)"] / metrics1["Total CPU Time (s)"]) * 100
    return comparison


def find_monitor_file(directory):
    """
    Find a file in the specified directory whose filename starts with 'monitor_'.

    Args:
        directory (str): The path to the directory to search in.

    Returns:
        str: The full path of the first matching file, or None if no match is found.
    """
    try:
        for filename in os.listdir(directory):
            if filename.startswith("monitor_"):
                return os.path.join(directory, filename)
        return None  # No matching file found
    except FileNotFoundError:
        print(f"Error: Directory '{directory}' does not exist.")
        return None
    except PermissionError:
        print(f"Error: Permission denied to access '{directory}'.")
        return None

def find_taskmanager_cycles_file(directory):
    """
    Find TaskManagerRunner cycles file (PRIMARY CPU overhead data).

    Args:
        directory (str): The path to the directory to search in.

    Returns:
        str: The full path of the taskmanager_cycles_*.txt file, or None if no match is found.
    """
    try:
        for filename in os.listdir(directory):
            if filename.startswith("taskmanager_cycles_") and filename.endswith(".txt"):
                return os.path.join(directory, filename)
        return None  # No matching file found
    except FileNotFoundError:
        print(f"Error: Directory '{directory}' does not exist.")
        return None
    except PermissionError:
        print(f"Error: Permission denied to access '{directory}'.")
        return None

def read_taskmanager_cycles(file_path, middle_minutes=20):
    """
    Read TaskManagerRunner CPU cycles file and extract metrics from the middle portion of the experiment.

    Args:
        file_path (str): Path to the taskmanager_cycles_*.txt file.
        middle_minutes (int): Number of minutes from the middle of the experiment to analyze (default: 20).

    Returns:
        dict: TaskManagerRunner metrics including total cycles, instructions, and GC time for the middle period.
    """
    if not file_path or not os.path.exists(file_path):
        print(f"TaskManagerRunner cycles file not found: {file_path}")
        return None
    
    print(f"Reading TaskManagerRunner cycles from: {file_path}")
    print(f"Filtering to middle {middle_minutes} minutes for stable comparison")
    
    try:
        import datetime
        
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        # Skip header lines and get all valid data lines
        data_lines = [line.strip() for line in lines if not line.startswith('#') and line.strip()]
        
        if len(data_lines) < 2:
            print("Insufficient data in TaskManagerRunner cycles file")
            return None
        
        # Parse all data points
        # Format: timestamp,tm_total_cycles,tm_interval_cycles,tm_instructions,tm_gc_time
        data_points = []
        for line in data_lines:
            parts = line.split(',')
            if len(parts) >= 5:
                try:
                    # Parse timestamp (format: "YYYY-MM-DD HH:MM:SS")
                    timestamp_str = parts[0].strip()
                    timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    
                    data_point = {
                        'timestamp': timestamp,
                        'interval_cycles': int(float(parts[2])) if parts[2] != '0' else 0,
                        'interval_instructions': int(float(parts[3])) if parts[3] != '0' else 0,
                        'interval_gc_time': float(parts[4]) if parts[4] != '0' else 0.0
                    }
                    data_points.append(data_point)
                except (ValueError, IndexError) as e:
                    print(f"Skipping invalid line: {line} (Error: {e})")
                    continue
        
        if len(data_points) < 2:
            print("No valid data points found")
            return None
        
        # Calculate experiment duration and middle period
        start_time = data_points[0]['timestamp']
        end_time = data_points[-1]['timestamp']
        total_duration = end_time - start_time
        total_minutes = total_duration.total_seconds() / 60
        
        print(f"Experiment duration: {total_minutes:.1f} minutes ({start_time} to {end_time})")
        
        if total_minutes < middle_minutes:
            print(f"WARNING: Experiment duration ({total_minutes:.1f} min) is shorter than requested middle period ({middle_minutes} min)")
            print("Using entire experiment duration")
            middle_start = start_time
            middle_end = end_time
        else:
            # Calculate middle period bounds
            skip_minutes = (total_minutes - middle_minutes) / 2
            middle_start = start_time + datetime.timedelta(minutes=skip_minutes)
            middle_end = end_time - datetime.timedelta(minutes=skip_minutes)
        
        print(f"Analyzing middle period: {middle_start} to {middle_end} ({middle_minutes} minutes)")
        
        # Filter data points to middle period and sum interval values
        middle_cycles = 0
        middle_instructions = 0
        middle_gc_time = 0.0
        middle_points_count = 0
        
        for point in data_points:
            if middle_start <= point['timestamp'] <= middle_end:
                middle_cycles += point['interval_cycles']
                middle_instructions += point['interval_instructions']
                middle_gc_time += point['interval_gc_time']
                middle_points_count += 1
        
        if middle_points_count == 0:
            print("No data points found in the middle period")
            return None
        
        # Calculate metrics for the middle period
        metrics = {
            'timestamp_start': middle_start.strftime('%Y-%m-%d %H:%M:%S'),
            'timestamp_end': middle_end.strftime('%Y-%m-%d %H:%M:%S'),
            'analysis_period_minutes': middle_minutes,
            'data_points_used': middle_points_count,
            'total_cycles': middle_cycles,
            'total_instructions': middle_instructions,
            'total_gc_time_ms': middle_gc_time,
            'experiment_duration_minutes': total_minutes
        }
        
        # Calculate Instructions Per Cycle (IPC) - efficiency metric
        if metrics['total_cycles'] > 0:
            metrics['ipc'] = metrics['total_instructions'] / metrics['total_cycles']
        else:
            metrics['ipc'] = 0.0
        
        print(f"Middle {middle_minutes}min metrics: {middle_cycles:,} cycles, {middle_instructions:,} instructions, IPC: {metrics['ipc']:.4f}")
        print(f"Used {middle_points_count} data points from middle period")
        
        return metrics
        
    except Exception as e:
        print(f"Error reading TaskManagerRunner cycles file: {e}")
        return None

def calculate_overhead(dir_without_sluice, dir_with_sluice, outputDir, middle_minutes=20):
    """
    Enhanced overhead calculation supporting both legacy and new TaskManagerRunner-focused monitoring.
    
    Args:
        dir_without_sluice (str): Directory with baseline experiment data (without Sluice).
        dir_with_sluice (str): Directory with Sluice experiment data.
        outputDir (str): Output directory for results.
        middle_minutes (int): Number of minutes from the middle of each experiment to analyze (default: 20).
    """
    print(f"\n=== ENHANCED OVERHEAD ANALYSIS ===")
    print(f"Baseline (without Sluice): {dir_without_sluice}")
    print(f"With Sluice: {dir_with_sluice}")
    print(f"Output directory: {outputDir}")

    # Find all relevant files
    log_file1 = find_monitor_file(dir_without_sluice)  # Baseline monitor log
    log_file2 = find_monitor_file(dir_with_sluice)     # Sluice monitor log
    
    tm_cycles_file1 = find_taskmanager_cycles_file(dir_without_sluice)  # Baseline TaskManager cycles
    tm_cycles_file2 = find_taskmanager_cycles_file(dir_with_sluice)     # Sluice TaskManager cycles

    # Read TaskManagerRunner cycles data (PRIMARY for CPU overhead analysis)
    tm_cycles_baseline = read_taskmanager_cycles(tm_cycles_file1, middle_minutes)
    tm_cycles_sluice = read_taskmanager_cycles(tm_cycles_file2, middle_minutes)

    # Try to read enhanced monitoring logs first, fallback to legacy format
    enhanced_baseline = read_enhanced_log(log_file1)
    enhanced_sluice = read_enhanced_log(log_file2)
    
    # Initialize variables for different data sources
    baseline_metrics = {}
    sluice_metrics = {}
    
    # === ENHANCED ANALYSIS (New Format) ===
    if enhanced_baseline[0] is not None and enhanced_sluice[0] is not None:
        print("\n--- Using Enhanced Monitoring Format ---")
        
        # Calculate comprehensive metrics using new format
        baseline_metrics = calculate_enhanced_metrics(
            enhanced_baseline[0], enhanced_baseline[1], tm_cycles_baseline
        )
        sluice_metrics = calculate_enhanced_metrics(
            enhanced_sluice[0], enhanced_sluice[1], tm_cycles_sluice
        )
        
        # Calculate TaskManagerRunner CPU cycle overhead (MOST IMPORTANT)
        cpu_cycle_overhead = {}
        if tm_cycles_baseline and tm_cycles_sluice:
            baseline_cycles = tm_cycles_baseline['total_cycles']
            sluice_cycles = tm_cycles_sluice['total_cycles']
            
            if baseline_cycles > 0:
                cycle_overhead_pct = ((sluice_cycles - baseline_cycles) / baseline_cycles) * 100
                cycle_overhead_abs = sluice_cycles - baseline_cycles
                
                cpu_cycle_overhead = {
                    'Baseline CPU Cycles': baseline_cycles,
                    'Sluice CPU Cycles': sluice_cycles,
                    'Absolute Overhead (cycles)': cycle_overhead_abs,
                    'Relative Overhead (%)': cycle_overhead_pct,
                    'Baseline CPU Cycles (Billions)': baseline_cycles / 1e9,
                    'Sluice CPU Cycles (Billions)': sluice_cycles / 1e9
                }
                
                print(f"\n🎯 **PRIMARY CPU OVERHEAD ANALYSIS (TaskManagerRunner - Middle 20 Minutes)**")
                print(f"   Baseline CPU Cycles: {baseline_cycles:,} ({baseline_cycles/1e9:.2f}B)")
                print(f"   Sluice CPU Cycles:   {sluice_cycles:,} ({sluice_cycles/1e9:.2f}B)")
                print(f"   **CPU Overhead: {cycle_overhead_pct:.2f}%** ({cycle_overhead_abs:,} cycles)")
                
                # Show analysis period details
                if 'analysis_period_minutes' in tm_cycles_baseline:
                    baseline_period = tm_cycles_baseline['analysis_period_minutes']
                    sluice_period = tm_cycles_sluice['analysis_period_minutes']
                    print(f"   Analysis Period: {baseline_period} min (baseline), {sluice_period} min (sluice)")
                    print(f"   Data Points: {tm_cycles_baseline.get('data_points_used', 'N/A')} (baseline), {tm_cycles_sluice.get('data_points_used', 'N/A')} (sluice)")
                
                # Additional insights
                if tm_cycles_baseline['total_instructions'] > 0 and tm_cycles_sluice['total_instructions'] > 0:
                    ipc_baseline = tm_cycles_baseline['ipc']
                    ipc_sluice = tm_cycles_sluice['ipc']
                    ipc_change = ((ipc_sluice - ipc_baseline) / ipc_baseline) * 100 if ipc_baseline > 0 else 0
                    print(f"   Baseline IPC: {ipc_baseline:.4f}")
                    print(f"   Sluice IPC:   {ipc_sluice:.4f}")
                    print(f"   IPC Change:   {ipc_change:.2f}%")
                    
                    cpu_cycle_overhead.update({
                        'Baseline IPC': ipc_baseline,
                        'Sluice IPC': ipc_sluice,
                        'IPC Change (%)': ipc_change
                    })
    
    # === LEGACY ANALYSIS (Backward Compatibility) ===
    else:
        print("\n--- Using Legacy Monitoring Format ---")
        
        # Read logs using legacy format
        try:
            df1 = read_log(log_file1)
            df2 = read_log(log_file2)
            
            # Calculate metrics using legacy method
            legacy_metrics1 = calculate_metrics(df1)
            legacy_metrics2 = calculate_metrics(df2)
            
            # Convert to dictionary format for consistency
            baseline_metrics = {'Legacy Format': legacy_metrics1.to_dict()}
            sluice_metrics = {'Legacy Format': legacy_metrics2.to_dict()}
            
            # Calculate overall overhead using legacy method
            overall_metrics1 = calculate_overall_metrics(legacy_metrics1)
            overall_metrics2 = calculate_overall_metrics(legacy_metrics2)
            overhead = compare_metrics(legacy_metrics1, legacy_metrics2)
            
            print(f"Legacy overhead analysis completed.")
            
        except Exception as e:
            print(f"Error in legacy analysis: {e}")
            baseline_metrics = {}
            sluice_metrics = {}

    # === SAVE COMPREHENSIVE RESULTS ===
    def save_enhanced_results(baseline_metrics, sluice_metrics, cpu_cycle_overhead, output_file):
        """Save comprehensive analysis results to CSV."""
        
        with open(output_file, 'w') as f:
            f.write("=== ENHANCED FLINK OVERHEAD ANALYSIS ===\n")
            f.write(f"Analysis Type: TaskManagerRunner-Focused CPU Cycle Monitoring (Middle 20 Minutes)\n")
            f.write(f"Generated: {pd.Timestamp.now()}\n")
            f.write(f"Note: CPU cycles analyzed from middle 20 minutes of each experiment for stable comparison\n\n")
            
            # PRIMARY ANALYSIS: CPU Cycle Overhead
            if cpu_cycle_overhead:
                f.write("🎯 PRIMARY ANALYSIS: TaskManagerRunner CPU Cycle Overhead (Middle 20 Minutes)\n")
                f.write("Metric,Value,Unit\n")
                for key, value in cpu_cycle_overhead.items():
                    if isinstance(value, float):
                        f.write(f"{key},{value:.6f},\n")
                    else:
                        f.write(f"{key},{value:,},\n")
                f.write("\n")
            
            # BASELINE METRICS
            f.write("BASELINE METRICS (Without Sluice)\n")
            for process_name, metrics in baseline_metrics.items():
                f.write(f"\n--- {process_name} ---\n")
                f.write("Metric,Value,Unit\n")
                for metric_name, metric_value in metrics.items():
                    if isinstance(metric_value, (int, float)):
                        f.write(f"{metric_name},{metric_value:.6f},\n")
                    else:
                        f.write(f"{metric_name},{metric_value},\n")
            f.write("\n")
            
            # SLUICE METRICS
            f.write("SLUICE METRICS (With Sluice)\n")
            for process_name, metrics in sluice_metrics.items():
                f.write(f"\n--- {process_name} ---\n")
                f.write("Metric,Value,Unit\n")
                for metric_name, metric_value in metrics.items():
                    if isinstance(metric_value, (int, float)):
                        f.write(f"{metric_name},{metric_value:.6f},\n")
                    else:
                        f.write(f"{metric_name},{metric_value},\n")
            f.write("\n")
            
            # OVERHEAD COMPARISON
            f.write("OVERHEAD COMPARISON SUMMARY\n")
            f.write("Process,Metric,Baseline,Sluice,Absolute Overhead,Relative Overhead (%)\n")
            
            # Compare matching processes
            for process_name in baseline_metrics.keys():
                if process_name in sluice_metrics:
                    baseline_proc = baseline_metrics[process_name]
                    sluice_proc = sluice_metrics[process_name]
                    
                    # Compare matching metrics
                    for metric_name in baseline_proc.keys():
                        if metric_name in sluice_proc and isinstance(baseline_proc[metric_name], (int, float)) and isinstance(sluice_proc[metric_name], (int, float)):
                            baseline_val = baseline_proc[metric_name]
                            sluice_val = sluice_proc[metric_name]
                            
                            if baseline_val != 0:
                                abs_overhead = sluice_val - baseline_val
                                rel_overhead = (abs_overhead / baseline_val) * 100
                                f.write(f"{process_name},{metric_name},{baseline_val:.6f},{sluice_val:.6f},{abs_overhead:.6f},{rel_overhead:.2f}\n")
        
        print(f"Enhanced results saved to {output_file}")

    # Create output directory
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    
    # Save comprehensive analysis
    save_enhanced_results(
        baseline_metrics, 
        sluice_metrics, 
        cpu_cycle_overhead if 'cpu_cycle_overhead' in locals() else {}, 
        outputDir + "enhanced_overhead_analysis.csv"
    )

    # Print summary
    print(f"\n=== ANALYSIS SUMMARY ===")
    if 'cpu_cycle_overhead' in locals() and cpu_cycle_overhead:
        print(f"✅ TaskManagerRunner CPU Overhead: {cpu_cycle_overhead.get('Relative Overhead (%)', 'N/A')}%")
    print(f"📁 Results saved to: {outputDir}enhanced_overhead_analysis.csv")
    print(f"🔍 Files analyzed:")
    print(f"   Baseline monitor: {log_file1}")
    print(f"   Sluice monitor: {log_file2}")
    print(f"   Baseline TM cycles: {tm_cycles_file1}")
    print(f"   Sluice TM cycles: {tm_cycles_file2}")
    print("=" * 50)

def test_enhanced_overhead_analysis():
    """
    Test function to demonstrate enhanced overhead analysis usage.
    Update these paths to match your actual experiment directories.
    """
    # Example paths - UPDATE THESE TO YOUR ACTUAL EXPERIMENT DIRECTORIES
    baseline_dir = "/path/to/part8-lr-NoControll-experiment-directory/"
    sluice_dir = "/path/to/part8-lr-StreamSluice-experiment-directory/"
    output_dir = "/path/to/output/linear-road/"
    
    print("=== ENHANCED OVERHEAD ANALYSIS TEST ===")
    print("Update the paths in test_enhanced_overhead_analysis() function to run this test.")
    print("\nExample usage:")
    print("baseline_dir = '/data/streamsluice/raw/part8-lr-NoControll-5-8-60-1380-150-1300-10-1-50-3-1000-1-50-27-3333-3000-0.1-100-1-25-0.0-false-1000-0.8-2/'")
    print("sluice_dir = '/data/streamsluice/raw/part8-lr-StreamSluice-5-8-60-1380-150-1300-10-1-50-3-1000-1-50-27-3333-3000-0.1-100-1-25-0.0-false-1000-0.8-2/'")
    print("output_dir = '/data/streamsluice/results/part8_overhead/linear-road/'")
    print("\n# Default: middle 20 minutes")
    print("calculate_overhead(baseline_dir, sluice_dir, output_dir)")
    print("\n# Custom: middle 15 minutes")
    print("calculate_overhead(baseline_dir, sluice_dir, output_dir, middle_minutes=15)")
    
    # Uncomment and update these lines when you have real experiment directories:
    # calculate_overhead(baseline_dir, sluice_dir, output_dir)
    # calculate_overhead(baseline_dir, sluice_dir, output_dir, middle_minutes=15)  # Custom period

def analyze_workload_overhead(workload_name, baseline_exp_name, sluice_exp_name, 
                             raw_data_dir="/data/streamsluice/raw/", 
                             output_base_dir="/data/streamsluice/results/part8_overhead/",
                             middle_minutes=20):
    """
    Convenience function for analyzing overhead for a specific workload.
    
    Args:
        workload_name (str): Name of the workload (e.g., "linear-road", "stock", "twitter", "ml-scoring").
        baseline_exp_name (str): Full experiment name for baseline (without Sluice).
        sluice_exp_name (str): Full experiment name with Sluice.
        raw_data_dir (str): Base directory containing raw experiment data.
        output_base_dir (str): Base directory for output results.
        middle_minutes (int): Number of minutes from the middle of each experiment to analyze (default: 20).
    """
    baseline_dir = os.path.join(raw_data_dir, baseline_exp_name)
    sluice_dir = os.path.join(raw_data_dir, sluice_exp_name)
    output_dir = os.path.join(output_base_dir, workload_name, "")
    
    print(f"\n🔧 Analyzing {workload_name} workload overhead (middle {middle_minutes} minutes)...")
    print(f"   Baseline: {baseline_exp_name}")
    print(f"   Sluice:   {sluice_exp_name}")
    
    # Verify directories exist
    if not os.path.exists(baseline_dir):
        print(f" ERROR: Baseline directory not found: {baseline_dir}")
        return False
    
    if not os.path.exists(sluice_dir):
        print(f" ERROR: Sluice directory not found: {sluice_dir}")
        return False
    
    try:
        calculate_overhead(baseline_dir, sluice_dir, output_dir, middle_minutes)
        print(f" {workload_name} overhead analysis completed successfully!")
        return True
    except Exception as e:
        print(f" ERROR during {workload_name} analysis: {e}")
        return False

if __name__ == "__main__":
    # Run the main analysis (original functionality)
    main()
