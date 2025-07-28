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
import glob
import pandas as pd
import scipy.stats as stats

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
            "Without_Sluice": "part8-lr-NoControll-5-8-60-1380-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-2",
            "With_Sluice": "part8-lr-StreamSluice-5-8-60-1380-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-2",
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

def find_system_monitor_file(directory):
    """
    Find a file in the specified directory whose filename starts with 'system_monitor_'.

    Args:
        directory (str): The path to the directory to search in.

    Returns:
        str: The full path of the first matching file, or None if no match is found.
    """
    try:
        for filename in os.listdir(directory):
            if filename.startswith("system_monitor_"):
                return os.path.join(directory, filename)
        return None  # No matching file found
    except FileNotFoundError:
        print(f"Error: Directory '{directory}' does not exist.")
        return None

def find_kafka_monitor_file(directory):
    """
    Find a file in the specified directory whose filename starts with 'kafka_metrics_'.

    Args:
        directory (str): The path to the directory to search in.

    Returns:
        str: The full path of the first matching file, or None if no match is found.
    """
    try:
        for filename in os.listdir(directory):
            if filename.startswith("kafka_metrics_"):
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

def read_cpu_cycles_from_monitor(directory):
    """
    Read CPU cycles directly from monitor_*.out files (new CSV format).
    
    Args:
        directory (str): Directory containing the monitor file
        
    Returns:
        dict: Dictionary with CPU metrics for all processes
    """
    monitor_file = find_monitor_file(directory)
    if not monitor_file:
        print(f"WARNING: No monitor file found in {directory}")
        return None
    
    print(f"Reading CPU cycles from: {monitor_file}")
    
    # Initialize results for all process types
    result = {
        'taskmanager_cycles': 0,
        'taskmanager_instructions': 0,
        'taskmanager_cache_misses': 0,
        'jobmanager_cycles': 0,
        'jobmanager_instructions': 0,
        'jobmanager_cache_misses': 0,
        'kafka_cycles': 0,
        'kafka_instructions': 0,
        'kafka_cache_misses': 0,
        'zookeeper_cycles': 0,
        'zookeeper_instructions': 0,
        'zookeeper_cache_misses': 0,
        'total_cycles': 0,
        'duration_seconds': 240
    }
    
    try:
        with open(monitor_file, 'r') as f:
            lines = f.readlines()
        
        # Parse the new CSV format
        # Format: Timestamp, PID, Process Name, Total Cycles, Total Instructions, Total Cache Misses, Duration (s)
        for line in lines:
            line = line.strip()
            if not line or line.startswith('Timestamp') or line.startswith('INFO:'):
                continue
            
            try:
                parts = [p.strip() for p in line.split(',')]
                if len(parts) >= 6:
                    timestamp = parts[0]
                    pid = parts[1]
                    process_info = parts[2]
                    cycles = int(parts[3]) if parts[3].isdigit() else 0
                    instructions = int(parts[4]) if parts[4].isdigit() else 0
                    cache_misses = int(parts[5]) if parts[5].isdigit() else 0
                    
                    # Classify process by type
                    if '[FLINK-PRIMARY]' in process_info or 'TaskManagerRunner' in process_info:
                        result['taskmanager_cycles'] = cycles
                        result['taskmanager_instructions'] = instructions
                        result['taskmanager_cache_misses'] = cache_misses
                        print(f"TaskManager: {cycles:,} cycles, {instructions:,} instructions, {cache_misses:,} cache misses")
                    
                    elif '[FLINK-MASTER]' in process_info or 'StandaloneSessionClusterEntrypoint' in process_info:
                        result['jobmanager_cycles'] = cycles
                        result['jobmanager_instructions'] = instructions
                        result['jobmanager_cache_misses'] = cache_misses
                        print(f"JobManager: {cycles:,} cycles, {instructions:,} instructions, {cache_misses:,} cache misses")
                    
                    elif '[KAFKA]' in process_info or 'Kafka' in process_info:
                        result['kafka_cycles'] = cycles
                        result['kafka_instructions'] = instructions
                        result['kafka_cache_misses'] = cache_misses
                        print(f"Kafka: {cycles:,} cycles, {instructions:,} instructions, {cache_misses:,} cache misses")
                    
                    elif '[ZOOKEEPER]' in process_info or 'QuorumPeerMain' in process_info:
                        result['zookeeper_cycles'] = cycles
                        result['zookeeper_instructions'] = instructions
                        result['zookeeper_cache_misses'] = cache_misses
                        print(f"ZooKeeper: {cycles:,} cycles, {instructions:,} instructions, {cache_misses:,} cache misses")
                    
                    # Add to total
                    result['total_cycles'] += cycles
                    
            except (ValueError, IndexError) as e:
                print(f"Warning: Could not parse line: {line} - {e}")
                continue
        
        if result['total_cycles'] > 0:
            print(f"Total cycles across all processes: {result['total_cycles']:,}")
            result['duration_minutes'] = result['duration_seconds'] / 60.0
            return result
        else:
            print(f"WARNING: No valid CPU data found in {monitor_file}")
            return None
            
    except Exception as e:
        print(f"ERROR reading monitor file {monitor_file}: {e}")
        return None


def calculate_overhead(dir_without_sluice, dir_with_sluice, outputDir, middle_minutes=20):
    """
    Enhanced overhead calculation with validation and fine-grained analysis.
    
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

    # Create output directory
    os.makedirs(outputDir, exist_ok=True)

    # Step 1: Validate baseline comparison
    print(f"\n🔍 Step 1: Validating baseline comparison...")
    is_valid = validate_baseline_comparison(dir_without_sluice, dir_with_sluice, outputDir)
    
    if not is_valid:
        print("⚠️  WARNING: Baseline validation failed. Results may not be reliable.")
        print("   Proceeding with analysis, but interpret results with caution.")
    else:
        print("✅ Baseline validation passed. Experiments are comparable.")

    # Step 2: Original coarse-grained analysis
    print(f"\n📊 Step 2: Coarse-grained overhead analysis...")
    baseline_cycles = read_cpu_cycles_from_monitor(dir_without_sluice)
    sluice_cycles = read_cpu_cycles_from_monitor(dir_with_sluice)
    
    if not baseline_cycles or not sluice_cycles:
        print("ERROR: Could not read CPU cycles from monitor files")
        return
    
    # Calculate CPU overhead
    baseline_tm_cycles = baseline_cycles['taskmanager_cycles']
    sluice_tm_cycles = sluice_cycles['taskmanager_cycles']
    baseline_tm_instructions = baseline_cycles.get('taskmanager_instructions', 0)
    sluice_tm_instructions = sluice_cycles.get('taskmanager_instructions', 0)
    baseline_tm_cache_misses = baseline_cycles.get('taskmanager_cache_misses', 0)
    sluice_tm_cache_misses = sluice_cycles.get('taskmanager_cache_misses', 0)
    
    if baseline_tm_cycles > 0:
        cycle_overhead_pct = ((sluice_tm_cycles - baseline_tm_cycles) / baseline_tm_cycles) * 100
        cycle_overhead_abs = sluice_tm_cycles - baseline_tm_cycles
        
        instruction_overhead_pct = ((sluice_tm_instructions - baseline_tm_instructions) / baseline_tm_instructions) * 100 if baseline_tm_instructions > 0 else 0
        instruction_overhead_abs = sluice_tm_instructions - baseline_tm_instructions
        
        cache_miss_overhead_pct = ((sluice_tm_cache_misses - baseline_tm_cache_misses) / baseline_tm_cache_misses) * 100 if baseline_tm_cache_misses > 0 else 0
        cache_miss_overhead_abs = sluice_tm_cache_misses - baseline_tm_cache_misses
        
        print(f"\n🎯 **COARSE-GRAINED CPU OVERHEAD ANALYSIS (TaskManagerRunner)**")
        print(f"   Baseline CPU Cycles: {baseline_tm_cycles:,} ({baseline_tm_cycles/1e9:.2f}B)")
        print(f"   Sluice CPU Cycles:   {sluice_tm_cycles:,} ({sluice_tm_cycles/1e9:.2f}B)")
        print(f"   **CPU Overhead: {cycle_overhead_pct:.2f}%** ({cycle_overhead_abs:,} cycles)")
        print(f"   Baseline Instructions: {baseline_tm_instructions:,}")
        print(f"   Sluice Instructions:   {sluice_tm_instructions:,}")
        print(f"   **Instruction Overhead: {instruction_overhead_pct:.2f}%** ({instruction_overhead_abs:,} instructions)")
        print(f"   Baseline Cache Misses: {baseline_tm_cache_misses:,}")
        print(f"   Sluice Cache Misses:   {sluice_tm_cache_misses:,}")
        print(f"   **Cache Miss Overhead: {cache_miss_overhead_pct:.2f}%** ({cache_miss_overhead_abs:,} misses)")
        print(f"   Duration: {baseline_cycles['duration_minutes']:.1f} minutes")
        
        # Save coarse-grained results
        output_file = os.path.join(outputDir, "coarse_grained_overhead.csv")
        with open(output_file, 'w') as f:
            f.write("Metric,Baseline,Sluice,Absolute Overhead,Relative Overhead (%)\n")
            f.write(f"TaskManager CPU Cycles,{baseline_tm_cycles},{sluice_tm_cycles},{cycle_overhead_abs},{cycle_overhead_pct:.2f}\n")
            f.write(f"TaskManager CPU Cycles (Billions),{baseline_tm_cycles/1e9:.2f},{sluice_tm_cycles/1e9:.2f},{cycle_overhead_abs/1e9:.2f},{cycle_overhead_pct:.2f}\n")
            f.write(f"TaskManager Instructions,{baseline_tm_instructions},{sluice_tm_instructions},{instruction_overhead_abs},{instruction_overhead_pct:.2f}\n")
            f.write(f"TaskManager Cache Misses,{baseline_tm_cache_misses},{sluice_tm_cache_misses},{cache_miss_overhead_abs},{cache_miss_overhead_pct:.2f}\n")
            f.write(f"Duration (minutes),{baseline_cycles['duration_minutes']:.1f},{sluice_cycles['duration_minutes']:.1f},0,0\n")
        
        print(f"📄 Coarse-grained results saved to: {output_file}")
        
        # Additional analysis for Kafka and ZooKeeper overhead
        print(f"\n🔍 **ADDITIONAL PROCESS OVERHEAD ANALYSIS**")
        
        # Kafka overhead
        baseline_kafka_cycles = baseline_cycles.get('kafka_cycles', 0)
        sluice_kafka_cycles = sluice_cycles.get('kafka_cycles', 0)
        if baseline_kafka_cycles > 0 and sluice_kafka_cycles > 0:
            kafka_overhead_pct = ((sluice_kafka_cycles - baseline_kafka_cycles) / baseline_kafka_cycles) * 100
            kafka_overhead_abs = sluice_kafka_cycles - baseline_kafka_cycles
            print(f"   **Kafka CPU Overhead: {kafka_overhead_pct:.2f}%** ({kafka_overhead_abs:,} cycles)")
        elif sluice_kafka_cycles > 0:
            print(f"   **Kafka CPU Usage (Sluice only): {sluice_kafka_cycles:,} cycles** (baseline: 0)")
        else:
            print(f"   Kafka: No CPU data available")
        
        # ZooKeeper overhead
        baseline_zk_cycles = baseline_cycles.get('zookeeper_cycles', 0)
        sluice_zk_cycles = sluice_cycles.get('zookeeper_cycles', 0)
        if baseline_zk_cycles > 0 and sluice_zk_cycles > 0:
            zk_overhead_pct = ((sluice_zk_cycles - baseline_zk_cycles) / baseline_zk_cycles) * 100
            zk_overhead_abs = sluice_zk_cycles - baseline_zk_cycles
            print(f"   **ZooKeeper CPU Overhead: {zk_overhead_pct:.2f}%** ({zk_overhead_abs:,} cycles)")
        elif sluice_zk_cycles > 0:
            print(f"   **ZooKeeper CPU Usage (Sluice only): {sluice_zk_cycles:,} cycles** (baseline: 0)")
        else:
            print(f"   ZooKeeper: No CPU data available")
        
        # JobManager overhead
        baseline_jm_cycles = baseline_cycles.get('jobmanager_cycles', 0)
        sluice_jm_cycles = sluice_cycles.get('jobmanager_cycles', 0)
        if baseline_jm_cycles > 0 and sluice_jm_cycles > 0:
            jm_overhead_pct = ((sluice_jm_cycles - baseline_jm_cycles) / baseline_jm_cycles) * 100
            jm_overhead_abs = sluice_jm_cycles - baseline_jm_cycles
            print(f"   **JobManager CPU Overhead: {jm_overhead_pct:.2f}%** ({jm_overhead_abs:,} cycles)")
        
        # Total system overhead
        baseline_total = baseline_cycles.get('total_cycles', 0)
        sluice_total = sluice_cycles.get('total_cycles', 0)
        if baseline_total > 0 and sluice_total > 0:
            total_overhead_pct = ((sluice_total - baseline_total) / baseline_total) * 100
            total_overhead_abs = sluice_total - baseline_total
            print(f"   **Total System CPU Overhead: {total_overhead_pct:.2f}%** ({total_overhead_abs:,} cycles)")
        
    else:
        print("ERROR: Invalid baseline CPU cycles")
        return

    # Step 3: Fine-grained analysis (if fine-grained data available)
    print(f"\n📈 Step 3: Fine-grained overhead analysis...")
    try:
        analyze_fine_grained_overhead(dir_without_sluice, dir_with_sluice, outputDir)
    except Exception as e:
        print(f"⚠️  Fine-grained analysis failed: {e}")
        print("   This may be due to missing fine-grained monitor data.")

    # Step 4: Generate comprehensive report
    print(f"\n📋 Step 4: Generating comprehensive report...")
    generate_comprehensive_report(dir_without_sluice, dir_with_sluice, outputDir, 
                                baseline_cycles, sluice_cycles, is_valid)
    
    print(f"\n✅ Overhead analysis completed!")
    print(f"📁 All results saved to: {outputDir}")

def generate_comprehensive_report(baseline_dir, sluice_dir, output_dir, 
                                baseline_cycles, sluice_cycles, is_valid):
    """Generate a comprehensive overhead analysis report."""
    
    report_file = os.path.join(output_dir, "overhead_analysis_report.md")
    
    with open(report_file, 'w') as f:
        f.write("# StreamSluice Overhead Analysis Report\n\n")
        
        f.write("## Executive Summary\n\n")
        if baseline_cycles and sluice_cycles:
            baseline_tm_cycles = baseline_cycles['taskmanager_cycles']
            sluice_tm_cycles = sluice_cycles['taskmanager_cycles']
            cycle_overhead_pct = ((sluice_tm_cycles - baseline_tm_cycles) / baseline_tm_cycles) * 100
            
            f.write(f"- **Baseline CPU Cycles**: {baseline_tm_cycles:,} ({baseline_tm_cycles/1e9:.2f}B)\n")
            f.write(f"- **StreamSluice CPU Cycles**: {sluice_tm_cycles:,} ({sluice_tm_cycles/1e9:.2f}B)\n")
            f.write(f"- **CPU Overhead**: {cycle_overhead_pct:.2f}%\n")
            f.write(f"- **Experiment Duration**: {baseline_cycles['duration_minutes']:.1f} minutes\n")
            f.write(f"- **Baseline Validation**: {'✅ PASSED' if is_valid else '❌ FAILED'}\n\n")
        
        f.write("## Methodology\n\n")
        f.write("### Measurement Approach\n")
        f.write("- **Coarse-grained**: Single `perf stat` measurement over entire experiment duration\n")
        f.write("- **Fine-grained**: Time series sampling every 1 second (if available)\n")
        f.write("- **Validation**: System load, memory usage, and configuration comparison\n\n")
        
        f.write("### Experimental Setup\n")
        f.write("- **Baseline**: NoControll with `metrics_report=false`\n")
        f.write("- **StreamSluice**: StreamSluice with `metrics_report=true` and `is_treat=false`\n")
        f.write("- **Metrics Interval**: 5ms (5,000,000 nanoseconds)\n\n")
        
        f.write("## Key Findings\n\n")
        if baseline_cycles and sluice_cycles:
            baseline_tm_cycles = baseline_cycles['taskmanager_cycles']
            sluice_tm_cycles = sluice_cycles['taskmanager_cycles']
            cycle_overhead_pct = ((sluice_tm_cycles - baseline_tm_cycles) / baseline_tm_cycles) * 100
            
            if cycle_overhead_pct < 0:
                f.write("### Counterintuitive Result\n")
                f.write(f"The StreamSluice experiment shows **{abs(cycle_overhead_pct):.2f}% lower CPU cycles** than the baseline.\n\n")
                f.write("### Possible Explanations\n")
                f.write("1. **Metrics Collection Efficiency**: StreamSluice's metrics collection is highly optimized\n")
                f.write("2. **System-Level Effects**: Frequent metrics may improve cache locality and memory management\n")
                f.write("3. **Flink Internal Optimizations**: Regular metrics collection may prevent certain inefficiencies\n")
                f.write("4. **Measurement Artifacts**: The measurement period may not capture long-term overhead patterns\n\n")
            else:
                f.write(f"### Expected Overhead\n")
                f.write(f"The StreamSluice experiment shows **{cycle_overhead_pct:.2f}% higher CPU cycles** than the baseline.\n\n")
        
        f.write("## Recommendations\n\n")
        f.write("1. **Extend Measurement Duration**: Run experiments for longer periods to capture long-term overhead\n")
        f.write("2. **Multiple Repetitions**: Conduct multiple experiment runs to assess variability\n")
        f.write("3. **Different Workloads**: Test overhead across different workload types and intensities\n")
        f.write("4. **System-Level Monitoring**: Monitor system-wide metrics (CPU, memory, network) during experiments\n")
        f.write("5. **Profiling**: Use detailed profiling tools to identify specific overhead sources\n\n")
        
        f.write("## Files Generated\n\n")
        f.write("- `coarse_grained_overhead.csv`: Coarse-grained overhead analysis results\n")
        f.write("- `baseline_validation.csv`: Baseline comparison validation results\n")
        f.write("- `overhead_visualizations.png`: Time series and distribution visualizations (if fine-grained data available)\n")
        f.write("- `detailed_statistics.csv`: Statistical analysis results (if fine-grained data available)\n")
        f.write("- `overhead_analysis_report.md`: This comprehensive report\n\n")
    
    print(f"📄 Comprehensive report saved to: {report_file}")

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

def analyze_fine_grained_overhead(baseline_dir, sluice_dir, output_dir):
    """
    Analyze fine-grained overhead using time series data.
    
    Args:
        baseline_dir (str): Directory with baseline experiment data
        sluice_dir (str): Directory with Sluice experiment data  
        output_dir (str): Output directory for results
    """
    print(f"\n=== FINE-GRAINED OVERHEAD ANALYSIS ===")
    
    # Read fine-grained monitor files
    baseline_file = find_monitor_file(baseline_dir)
    sluice_file = find_monitor_file(sluice_dir)
    
    if not baseline_file or not sluice_file:
        print("ERROR: Could not find monitor files")
        return
    
    try:
        # Read time series data
        baseline_df = pd.read_csv(baseline_file, skipinitialspace=True)
        sluice_df = pd.read_csv(sluice_file, skipinitialspace=True)
        
        # Filter TaskManager data
        baseline_tm = baseline_df[baseline_df['Process Name'].str.contains('TaskManagerRunner.*PRIMARY', na=False, regex=True)]
        sluice_tm = sluice_df[sluice_df['Process Name'].str.contains('TaskManagerRunner.*PRIMARY', na=False, regex=True)]
        
        if baseline_tm.empty or sluice_tm.empty:
            print("ERROR: No TaskManager data found")
            return
        
        # Calculate overhead metrics
        baseline_avg_cycles = baseline_tm['CPU Cycles'].mean()
        sluice_avg_cycles = sluice_tm['CPU Cycles'].mean()
        cycle_overhead_pct = ((sluice_avg_cycles - baseline_avg_cycles) / baseline_avg_cycles) * 100
        
        baseline_avg_instructions = baseline_tm['Instructions'].mean()
        sluice_avg_instructions = sluice_tm['Instructions'].mean()
        instruction_overhead_pct = ((sluice_avg_instructions - baseline_avg_instructions) / baseline_avg_instructions) * 100
        
        # Calculate IPC (Instructions Per Cycle)
        baseline_ipc = baseline_avg_instructions / baseline_avg_cycles if baseline_avg_cycles > 0 else 0
        sluice_ipc = sluice_avg_instructions / sluice_avg_cycles if sluice_avg_cycles > 0 else 0
        
        print(f"\n📊 **FINE-GRAINED OVERHEAD ANALYSIS**")
        print(f"   Baseline Avg Cycles: {baseline_avg_cycles:,.0f}")
        print(f"   Sluice Avg Cycles:   {sluice_avg_cycles:,.0f}")
        print(f"   **Cycle Overhead: {cycle_overhead_pct:.2f}%**")
        print(f"   Baseline Avg Instructions: {baseline_avg_instructions:,.0f}")
        print(f"   Sluice Avg Instructions:   {sluice_avg_instructions:,.0f}")
        print(f"   **Instruction Overhead: {instruction_overhead_pct:.2f}%**")
        print(f"   Baseline IPC: {baseline_ipc:.3f}")
        print(f"   Sluice IPC:   {sluice_ipc:.3f}")
        
        # Create visualizations
        create_overhead_visualizations(baseline_tm, sluice_tm, output_dir)
        
        # Save detailed results
        save_detailed_results(baseline_tm, sluice_tm, output_dir)
        
    except Exception as e:
        print(f"ERROR during fine-grained analysis: {e}")

def create_overhead_visualizations(baseline_tm, sluice_tm, output_dir):
    """Create visualizations to illustrate overhead patterns."""
    import matplotlib.pyplot as plt
    import numpy as np
    
    # Create figure with subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    
    # 1. CPU Cycles over time
    ax1.plot(baseline_tm.index, baseline_tm['CPU Cycles'], label='Baseline (NoControll)', alpha=0.7)
    ax1.plot(sluice_tm.index, sluice_tm['CPU Cycles'], label='StreamSluice (5ms)', alpha=0.7)
    ax1.set_title('CPU Cycles Over Time')
    ax1.set_xlabel('Sample Index')
    ax1.set_ylabel('CPU Cycles')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Instructions over time
    ax2.plot(baseline_tm.index, baseline_tm['Instructions'], label='Baseline (NoControll)', alpha=0.7)
    ax2.plot(sluice_tm.index, sluice_tm['Instructions'], label='StreamSluice (5ms)', alpha=0.7)
    ax2.set_title('Instructions Over Time')
    ax2.set_xlabel('Sample Index')
    ax2.set_ylabel('Instructions')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # 3. Distribution comparison
    ax3.hist(baseline_tm['CPU Cycles'], bins=30, alpha=0.7, label='Baseline (NoControll)', density=True)
    ax3.hist(sluice_tm['CPU Cycles'], bins=30, alpha=0.7, label='StreamSluice (5ms)', density=True)
    ax3.set_title('CPU Cycles Distribution')
    ax3.set_xlabel('CPU Cycles')
    ax3.set_ylabel('Density')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. Overhead percentage over time
    # Align data by time if possible, otherwise use sample index
    min_len = min(len(baseline_tm), len(sluice_tm))
    overhead_pct = ((sluice_tm['CPU Cycles'].iloc[:min_len] - baseline_tm['CPU Cycles'].iloc[:min_len]) / 
                   baseline_tm['CPU Cycles'].iloc[:min_len]) * 100
    ax4.plot(range(min_len), overhead_pct, label='CPU Overhead %', color='red')
    ax4.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    ax4.set_title('CPU Overhead Percentage Over Time')
    ax4.set_xlabel('Sample Index')
    ax4.set_ylabel('Overhead %')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'overhead_visualizations.png'), dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"📈 Visualizations saved to: {os.path.join(output_dir, 'overhead_visualizations.png')}")

def save_detailed_results(baseline_tm, sluice_tm, output_dir):
    """Save detailed statistical analysis results."""
    import scipy.stats as stats
    
    # Calculate comprehensive statistics
    stats_data = {
        'Metric': [],
        'Baseline_Mean': [],
        'Baseline_Std': [],
        'Sluice_Mean': [],
        'Sluice_Std': [],
        'Absolute_Diff': [],
        'Relative_Diff_Pct': [],
        'P_Value': []
    }
    
    # Analyze CPU Cycles
    baseline_cycles = baseline_tm['CPU Cycles'].dropna()
    sluice_cycles = sluice_tm['CPU Cycles'].dropna()
    
    if len(baseline_cycles) > 0 and len(sluice_cycles) > 0:
        # Statistical test
        t_stat, p_value = stats.ttest_ind(baseline_cycles, sluice_cycles)
        
        stats_data['Metric'].append('CPU_Cycles')
        stats_data['Baseline_Mean'].append(baseline_cycles.mean())
        stats_data['Baseline_Std'].append(baseline_cycles.std())
        stats_data['Sluice_Mean'].append(sluice_cycles.mean())
        stats_data['Sluice_Std'].append(sluice_cycles.std())
        stats_data['Absolute_Diff'].append(sluice_cycles.mean() - baseline_cycles.mean())
        stats_data['Relative_Diff_Pct'].append(((sluice_cycles.mean() - baseline_cycles.mean()) / baseline_cycles.mean()) * 100)
        stats_data['P_Value'].append(p_value)
    
    # Analyze Instructions
    baseline_instructions = baseline_tm['Instructions'].dropna()
    sluice_instructions = sluice_tm['Instructions'].dropna()
    
    if len(baseline_instructions) > 0 and len(sluice_instructions) > 0:
        t_stat, p_value = stats.ttest_ind(baseline_instructions, sluice_instructions)
        
        stats_data['Metric'].append('Instructions')
        stats_data['Baseline_Mean'].append(baseline_instructions.mean())
        stats_data['Baseline_Std'].append(baseline_instructions.std())
        stats_data['Sluice_Mean'].append(sluice_instructions.mean())
        stats_data['Sluice_Std'].append(sluice_instructions.std())
        stats_data['Absolute_Diff'].append(sluice_instructions.mean() - baseline_instructions.mean())
        stats_data['Relative_Diff_Pct'].append(((sluice_instructions.mean() - baseline_instructions.mean()) / baseline_instructions.mean()) * 100)
        stats_data['P_Value'].append(p_value)
    
    # Save detailed statistics
    stats_df = pd.DataFrame(stats_data)
    stats_file = os.path.join(output_dir, 'detailed_statistics.csv')
    stats_df.to_csv(stats_file, index=False)
    
    print(f"📊 Detailed statistics saved to: {stats_file}")
    
    # Print significance results
    print(f"\n🔬 **STATISTICAL SIGNIFICANCE**")
    for _, row in stats_df.iterrows():
        significance = "***" if row['P_Value'] < 0.001 else "**" if row['P_Value'] < 0.01 else "*" if row['P_Value'] < 0.05 else "ns"
        print(f"   {row['Metric']}: p-value = {row['P_Value']:.4f} {significance}")
        print(f"   Relative difference: {row['Relative_Diff_Pct']:.2f}%")

def validate_baseline_comparison(baseline_dir, sluice_dir, output_dir):
    """
    Validate that baseline and Sluice experiments are truly comparable.
    
    Args:
        baseline_dir (str): Directory with baseline experiment data
        sluice_dir (str): Directory with Sluice experiment data
        output_dir (str): Output directory for validation results
    """
    print(f"\n=== BASELINE VALIDATION ===")
    
    validation_results = {
        'check': [],
        'baseline_value': [],
        'sluice_value': [],
        'difference': [],
        'status': []
    }
    
    # 1. Check experiment duration
    baseline_cycles = read_cpu_cycles_from_monitor(baseline_dir)
    sluice_cycles = read_cpu_cycles_from_monitor(sluice_dir)
    
    if baseline_cycles and sluice_cycles:
        duration_diff = abs(baseline_cycles['duration_minutes'] - sluice_cycles['duration_minutes'])
        validation_results['check'].append('Experiment Duration (minutes)')
        validation_results['baseline_value'].append(baseline_cycles['duration_minutes'])
        validation_results['sluice_value'].append(sluice_cycles['duration_minutes'])
        validation_results['difference'].append(duration_diff)
        validation_results['status'].append('PASS' if duration_diff < 1.0 else 'FAIL')
    
    # 2. Check system load during experiments
    baseline_load = get_system_load_info(baseline_dir)
    sluice_load = get_system_load_info(sluice_dir)
    
    if baseline_load and sluice_load:
        load_diff = abs(baseline_load['avg_load'] - sluice_load['avg_load'])
        validation_results['check'].append('System Load (1min avg)')
        validation_results['baseline_value'].append(baseline_load['avg_load'])
        validation_results['sluice_value'].append(sluice_load['avg_load'])
        validation_results['difference'].append(load_diff)
        validation_results['status'].append('PASS' if load_diff < 0.5 else 'FAIL')
    
    # 3. Check memory usage
    baseline_mem = get_memory_usage_info(baseline_dir)
    sluice_mem = get_memory_usage_info(sluice_dir)
    
    if baseline_mem and sluice_mem:
        mem_diff_pct = abs(baseline_mem['avg_usage_mb'] - sluice_mem['avg_usage_mb']) / baseline_mem['avg_usage_mb'] * 100
        validation_results['check'].append('Memory Usage (MB)')
        validation_results['baseline_value'].append(baseline_mem['avg_usage_mb'])
        validation_results['sluice_value'].append(sluice_mem['avg_usage_mb'])
        validation_results['difference'].append(mem_diff_pct)
        validation_results['status'].append('PASS' if mem_diff_pct < 10.0 else 'FAIL')
    
    # 4. Check Flink configuration differences
    config_diff = compare_flink_configs(baseline_dir, sluice_dir)
    validation_results['check'].append('Flink Config Differences')
    validation_results['baseline_value'].append('Baseline Config')
    validation_results['sluice_value'].append('Sluice Config')
    validation_results['difference'].append(len(config_diff))
    validation_results['status'].append('PASS' if len(config_diff) <= 2 else 'FAIL')
    
    # Save validation results
    validation_df = pd.DataFrame(validation_results)
    validation_file = os.path.join(output_dir, 'baseline_validation.csv')
    validation_df.to_csv(validation_file, index=False)
    
    print(f"📋 Validation results saved to: {validation_file}")
    
    # Print validation summary
    print(f"\n🔍 **BASELINE VALIDATION SUMMARY**")
    for _, row in validation_df.iterrows():
        status_icon = "✅" if row['status'] == 'PASS' else "❌"
        print(f"   {status_icon} {row['check']}: {row['status']}")
        if row['check'] != 'Flink Config Differences':
            print(f"      Baseline: {row['baseline_value']:.2f}, Sluice: {row['sluice_value']:.2f}")
    
    # Overall validation status
    pass_count = sum(1 for status in validation_results['status'] if status == 'PASS')
    total_count = len(validation_results['status'])
    overall_status = "VALID" if pass_count == total_count else "INVALID"
    
    print(f"\n🎯 **OVERALL VALIDATION: {overall_status}** ({pass_count}/{total_count} checks passed)")
    
    return overall_status == "VALID"

def get_system_load_info(exp_dir):
    """Extract system load information from experiment logs."""
    # Look for system load information in logs
    log_files = glob.glob(os.path.join(exp_dir, "*.log"))
    
    for log_file in log_files:
        try:
            with open(log_file, 'r') as f:
                content = f.read()
                # Look for load average patterns
                import re
                load_pattern = r'load average: ([\d.]+), ([\d.]+), ([\d.]+)'
                matches = re.findall(load_pattern, content)
                if matches:
                    # Use the 1-minute average
                    loads = [float(match[0]) for match in matches]
                    return {'avg_load': sum(loads) / len(loads)}
        except:
            continue
    
    return None

def get_memory_usage_info(exp_dir):
    """Extract memory usage information from experiment logs."""
    # Look for memory usage in monitor files or logs
    monitor_file = find_monitor_file(exp_dir)
    if monitor_file:
        try:
            df = pd.read_csv(monitor_file, skipinitialspace=True)
            if 'Heap Used (MB)' in df.columns:
                tm_data = df[df['Process Name'].str.contains('TaskManagerRunner.*PRIMARY', na=False, regex=True)]
                if not tm_data.empty:
                    return {'avg_usage_mb': tm_data['Heap Used (MB)'].mean()}
        except:
            pass
    
    return None

def compare_flink_configs(baseline_dir, sluice_dir):
    """Compare Flink configurations between baseline and Sluice experiments."""
    config_diffs = []
    
    # Look for flink-conf.yaml files
    baseline_config = os.path.join(baseline_dir, "flink-conf.yaml")
    sluice_config = os.path.join(sluice_dir, "flink-conf.yaml")
    
    if os.path.exists(baseline_config) and os.path.exists(sluice_config):
        try:
            with open(baseline_config, 'r') as f:
                baseline_content = f.read()
            with open(sluice_config, 'r') as f:
                sluice_content = f.read()
            
            # Compare key configuration parameters
            key_params = [
                'policy.windowSize',
                'metrics.report.flag',
                'controller.type',
                'streamsluice.system.is_treat'
            ]
            
            for param in key_params:
                baseline_match = re.search(f'{param}\\s*:\\s*(.+)', baseline_content)
                sluice_match = re.search(f'{param}\\s*:\\s*(.+)', sluice_content)
                
                if baseline_match and sluice_match:
                    baseline_val = baseline_match.group(1).strip()
                    sluice_val = sluice_match.group(1).strip()
                    if baseline_val != sluice_val:
                        config_diffs.append(f"{param}: {baseline_val} vs {sluice_val}")
        
        except Exception as e:
            config_diffs.append(f"Error reading configs: {e}")
    
    return config_diffs



if __name__ == "__main__":
    # Run the main analysis (original functionality)
    main()
