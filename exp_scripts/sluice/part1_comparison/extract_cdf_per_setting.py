import math
import sys
import numpy as np
import matplotlib
from matplotlib.lines import Line2D

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
from brokenaxes import brokenaxes

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

CONTROLLER_COLOR={
    "Static": "black",
    "Static-Adequate": "grey",
    "DS2": "orange",
    "Streamswitch": "green",
    "StreamSwitch": "green",
    "Sluice": "blue",
}

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
        plt.plot(x, y, 'o--', label="Latency Bar", color='orange', linewidth=1.5)
        last_y = latency_bar[time]
        last_time = time - initial_time
    x = [last_time, 10000000]
    y = [last_y, last_y]
    plt.plot(x, y, 'o--', label="Latency Bar", color='orange', linewidth=1.5)

def draw_latency_curves(raw_dir, output_dir, exp_name, window_size, start_time, exp_length, latency_limit, draw_lem_latency_flag):
    exps = [
        ["GroundTruth", exp_name, "blue", "o"]
    ]
    average_ground_truth_latencies = []
    lem_latencies = []
    latency_bar = []
    p99_bar = []
    initial_times = []
    scalings = []
    latency_curve = []
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
        scalings += [result[3]]

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

    def add_scaling_marker(plt, scalings):
        for scaling_index in range(0, len(scalings[0])):
            scaling_time = scalings[0][scaling_index]
            if (scalings[1][scaling_index] == 0):
                plt.plot([scaling_time, scaling_time], [0, 100000], "--", color="orange", linewidth=1)
            else:
                plt.plot([scaling_time, scaling_time], [0, 100000], "--", color="gray", linewidth=1)

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
        latency_curve = [sampled_latency[0], sampled_latency[1]]
        if (draw_lem_latency_flag):
            plt.plot(lem_latencies[i][0], lem_latencies[i][1], '-', color="green", markersize=2, linewidth=2,
                     label='Estimated Latency')
            add_latency_bar_curve(plt, latency_bar[i], initial_times[i])
        add_latency_limit_marker(plt, latency_limit)
        #add_scaling_marker(plt, scalings[i])

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

    if max(sampled_latency[1]) <= 5000:
        axes.set_ylim(0, 5000)
        axes.set_yticks(np.arange(0, 5500, 500))
    else:
        axes.set_ylim(0, 20000)
        axes.set_yticks(np.arange(0, 20000, 2000))
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

    return groundtruth_p99_latency_in_range, success_rate, latency_curve

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

def draw_parallelism_curve(rawDir, outputDir, exp_name, windowSize, startTime, exp_length, draw_parallelism_flag, arrival_curves) -> [float, float, float, [list[int], list[float]]]:
    exps = [
        ["Sluice", exp_name, "blue", "o"]
    ]
    parallelismsPerJob = {}
    totalArrivalRatesPerJob = {}
    totalParallelismPerExps = {}
    parallelism_curve = []
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
    if (len(arrival_curves) == 0):
        arrival_curves = [ax, ay]
    else:
        ax, ay = arrival_curves
    ax2.plot(ax, ay, '-', color='red', markersize=MARKERSIZE / 2, label="Arrival Rate")
    if (exp_name.startswith("lr-")):
        ax2.set_ylim(400, 2000)
        ax2.set_yticks(np.arange(400, 2200, 200))
    elif (exp_name.startswith("stock-")):
        ax2.set_ylim(400, 2000)
        ax2.set_yticks(np.arange(400, 2200, 200))
    else:
        ax2.set_ylim(1000, 8000)
        ax2.set_yticks(np.arange(1000, 8000, 1000))
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
            maxParallelism = 0
            minParallelism = 10000
            Parallelism = totalParallelismPerExps[expindex]
            # print(job + " " + str(expindex) + " " + str(Parallelism))
            legend += ["# of Slots"]
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
                    if y0 > maxParallelism:
                        maxParallelism = y0
                    if y0 < minParallelism:
                        minParallelism = y0
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
            parallelism_curve.append(line)
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
    if output_pdf_flag:
        plt.savefig(outputDir + figName + ".pdf", bbox_inches='tight')
    else:
        plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)
    return average_parallelism, maxParallelism, minParallelism, arrival_curves, parallelism_curve

def plot_latency_cdf(latency_per_label, latency_bar_this_workload, output_dir, workload_name: str):
    labels = list(latency_per_label.keys())

    fig = plt.figure(figsize=(12, 4)) #plt.figure(figsize=(12, 5))
    bax = brokenaxes(ylims=((0, 0.1), (0.6, 1.0)), hspace=.2)

    for label, data in latency_per_label.items():
        line_width = 1
        if label == "Sluice":
            line_width = 1.5
        data_sorted = np.sort(data)
        cdf = np.arange(1, len(data_sorted) + 1) / len(data_sorted)
        bax.plot(data_sorted, cdf, marker='none', linestyle='-', linewidth=line_width, color=CONTROLLER_COLOR[label], label=label)
        # data.sort()
        # plt.ecdf(data, complementary=True, color=CONTROLLER_COLOR[label], label=label)
    # Draw p99
    bax.plot([0, 10000000], [0.99, 0.99], "--", color='red')
    bax.plot([latency_bar_this_workload, latency_bar_this_workload], [0, 1.0], "--", color='red')
    # Add labels, title, and custom x-axis tick labels
    bax.set_xlabel('Latency')
    bax.set_ylabel('CDF')
    # bax.set_ylim(0.5, 1.001)
    # bax.set_yticks(np.arange(0.5, 1.001, 0.05))
    bax.set_xlim(0, 5000)
    bax.set_xticks(np.arange(0, 5000, 500))
    bax.set_title('Cumulative Distribution Function (CDF) of Latency')
    bax.legend(loc='lower right', bbox_to_anchor=(1, -0.15), ncol=1)
    bax.grid(True, axis='y')
    # Save the plot
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if output_pdf_flag:
        plt.savefig(os.path.join(output_dir, 'cdf_' + str(workload_name) + '.pdf'), bbox_inches='tight')
    else:
        plt.savefig(os.path.join(output_dir, 'cdf_' + str(workload_name) + '.png'), bbox_inches='tight')
    plt.close(fig)


def plot_average_latency(latency_per_label, output_dir, workload_name: str):
    # Extract labels and compute average latency for each
    labels = list(latency_per_label.keys())
    avg_latencies = [np.mean(latency_per_label[label]) for label in labels]

    # Plot the bar chart
    fig, ax = plt.subplots(figsize=(12, 5))
    bars = ax.bar(labels, avg_latencies, color=[CONTROLLER_COLOR[label] for label in labels])

    # Add numerical labels on top of the bars
    for bar, avg_latency in zip(bars, avg_latencies):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.0, height, f'{avg_latency:.2f}', ha='center', va='bottom')

    # Customize the chart
    ax.set_xlabel('Controllers')
    ax.set_ylabel('Average Latency (ms)')
    ax.set_title(f'Average Latency per Controller ({workload_name})')
    ax.grid(axis='y', linestyle='--', alpha=0.7)

    # Save the plot
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if output_pdf_flag:
        plot_filename = os.path.join(output_dir, f'avg_latency_{workload_name}.pdf')
        plt.savefig(plot_filename, bbox_inches='tight')
    else:
        plot_filename = os.path.join(output_dir, f'avg_latency_{workload_name}.png')
        plt.savefig(plot_filename, bbox_inches='tight')
    plt.close(fig)
    print(f"Bar chart saved to {plot_filename}")

def calculate_latency_stats(latency_per_label):
    """
    Calculate and return the average, min, and max latency for each label.

    Args:
        latency_per_label (dict): A dictionary where keys are labels (e.g., controller names)
                                  and values are lists of latency measurements.

    Returns:
        dict: A dictionary where each key is a label, and the value is a tuple (average, min, max).
    """
    latency_stats = {}
    for label, latencies in latency_per_label.items():
        avg_latency = np.mean(latencies)
        min_latency = np.min(latencies)
        max_latency = np.max(latencies)
        latency_stats[label] = (avg_latency, min_latency, max_latency)
    return latency_stats

def plot_avg_parallelism_bar(avg_parallelism_per_label, output_dir, workload_name: str):
    x = list(avg_parallelism_per_label.keys())
    y = list(avg_parallelism_per_label.values())
    fig, ax = plt.subplots(figsize=(12, 4)) #plt.subplots(figsize=(12, 5))

    # Set width of bars and positions
    bar_width = 0.5
    plt.bar(x, y, width=bar_width, color='blue')
    plt.xlabel('Controller')
    plt.ylabel('Avg Parallelism')
    plt.title('Avg Parallelism by Controllers in ' + workload_name)
    # ax.legend()
    ax.grid(True, axis='y')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if output_pdf_flag:
        plt.savefig(output_dir + 'avg_parallelism_curve_' + str(workload_name) + '.pdf', bbox_inches='tight')
    else:
        plt.savefig(output_dir + 'avg_parallelism_curve_' + str(workload_name) + '.png', bbox_inches='tight')
    plt.close(fig)

def plot_success_rate_bar(success_rate_per_label, output_dir, workload_name: str):
    x = list(success_rate_per_label.keys())
    y = list(success_rate_per_label.values())
    fig, ax = plt.subplots(figsize=(12, 4)) #plt.subplots(figsize=(12, 5))

    # Set width of bars and positions
    bar_width = 0.5
    plt.bar(x, y, width=bar_width, color='blue')
    plt.xlabel('Controller')
    ax.set_ylabel('Success Rate')
    ax.set_ylim(0.0, 1.001)
    ax.set_yticks(np.arange(0.0, 1.2, 0.2))
    ax.set_title('Success Rates by Controllers in ' + workload_name)
    ax.axhline(y=0.99, color='red', linestyle='--', linewidth=2, label='99%')
    ax.legend()
    ax.grid(True, axis='y')

    # Save the plot
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if output_pdf_flag:
        plt.savefig(os.path.join(output_dir, 'success_rate_bar_' + str(workload_name) + '.pdf'), bbox_inches='tight')
    else:
        plt.savefig(os.path.join(output_dir, 'success_rate_bar_' + str(workload_name) + '.png'), bbox_inches='tight')
    plt.close(fig)

def plot_latency_curves(latency_curves, latency_limit, output_dir, start_time, exp_length, workload_name: str, ax, ylabel_flag):
    #fig, ax = plt.subplots(figsize=(12, 6))
    legend_elements = []
    from matplotlib.lines import Line2D
    for label, latency_curve in latency_curves.items():
        if label == "Sluice":
            linewidth = 2.5
        else:
            linewidth = 1.5
        ax.plot(latency_curve[0], latency_curve[1], MARKER_MAP[label][1:], color=COLOR_MAP[label], label=label, linewidth=linewidth)

        marker_indices = np.arange(0, len(latency_curve[0]), 50)  # Every 100 points = 10 seconds
        marker_x = [latency_curve[0][index] for index in marker_indices]
        marker_y = [latency_curve[1][index] for index in marker_indices]
        ax.scatter(marker_x, marker_y, s=64, color=COLOR_MAP[label], marker=MARKER_MAP[label][0], label=label, zorder=10)

    ax.plot([0, 10000000], [latency_limit, latency_limit], color='red', linestyle='--')
    ax.set_ylim(0, 5000)
    ax.set_yticks(np.arange(0, 6000, 1000))
    ax.set_xlim((start_time) * 1000, (start_time + exp_length) * 1000)
    ax.set_xticks(np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + (exp_length / 5) * 1000,
                              (exp_length / 5) * 1000))
    ax.set_xticklabels([int((x - start_time * 1000) / 60000) for x in
                          np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + (exp_length / 5) * 1000,
                                    (exp_length / 5) * 1000)])
    ax.tick_params(axis='x', labelsize=FONT_SIZE)
    ax.tick_params(axis='y', labelsize=FONT_SIZE)
    if ylabel_flag:
        ax.set_ylabel('Latency (ms)', fontsize=FONT_SIZE)
    ax.set_xlabel('Time (minute)', fontsize=FONT_SIZE)
    ax.set_title(workload_name, fontsize=FONT_SIZE)
    #ax.legend(handles=legend_elements, loc='upper center', bbox_to_anchor=(0.5, 1.25), ncol=2, fontsize=FONT_SIZE)
    #ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.25), ncol=4)
    ax.grid(True)

    # # Save the plot
    # if not os.path.exists(output_dir):
    #     os.makedirs(output_dir)
    #
    # if output_pdf_flag:
    #     plt.savefig(os.path.join(output_dir, 'latency_curves_' + str(workload_name) + '.pdf'), bbox_inches='tight')
    # else:
    #     plt.savefig(os.path.join(output_dir, 'latency_curves_' + str(workload_name) + '.png'), bbox_inches='tight')
    # plt.close(fig)

def plot_parallelism_curves(parallelism_curve, arrival_curve, output_dir, start_time, exp_length, workload_name: str, axs, y1label_flag, y2label_flag):
    #fig, axs = plt.subplots(1, 1, figsize=(12, 6), layout='constrained')

    ax1 = axs
    ax2 = ax1.twinx()
    ax2.plot(arrival_curve[0], arrival_curve[1], color='red', linestyle='-', label="Arrival Rate")
    for label, p_curve in parallelism_curve.items():
        if label == "Sluice":
            linewidth = 2.5
        else:
            linewidth = 1.5
        if label != "Static":
            # Interval for markers in milliseconds (20 seconds = 20000 ms)
            marker_interval = 60000
            next_marker_time = start_time * 1000 + marker_interval

            for xs, ys in p_curve:
                ax1.plot(xs, ys, MARKER_MAP[label][1:], color=COLOR_MAP[label], label=label, linewidth=linewidth)

                for i in range(0, len(xs) - 1):
                    x0 = xs[i]
                    x1 = xs[i+1]
                    y0 = ys[i]
                    # Add marker points every 2 seconds
                    while next_marker_time >= x0 and next_marker_time <= x1 and next_marker_time <= (start_time + exp_length) * 1000:
                        ax1.scatter(next_marker_time, y0, color=COLOR_MAP[label], marker=MARKER_MAP[label][0], s=64, zorder=10)
                        next_marker_time += marker_interval


    ax1.set_ylim(0, 40)
    ax1.set_yticks(np.arange(0, 45, 10))
    if workload_name.startswith("Twitter"):
        ax2.set_ylim(0, 16000)
        ax2.set_yticks(np.arange(0, 20000, 4000))
    else:
        ax2.set_ylim(0, 8000)
        ax2.set_yticks(np.arange(0, 10000, 2000))
    ax1.tick_params(axis='x', labelsize=FONT_SIZE)
    ax1.tick_params(axis='y', labelsize=FONT_SIZE)
    ax2.tick_params(axis='y', labelsize=FONT_SIZE)
    if y1label_flag:
        ax1.set_ylabel('# of Slots', fontsize=FONT_SIZE)
    if y2label_flag:
        ax2.set_ylabel('Arrival Rate (tps)', fontsize=FONT_SIZE)
    axs.set_xlabel('Time (minute)', fontsize=FONT_SIZE)
    axs.set_xlim((start_time) * 1000, (start_time + exp_length) * 1000)
    axs.set_xticks(np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + (exp_length / 5) * 1000,
                              (exp_length / 5) * 1000))
    axs.set_xticklabels([int((x - start_time * 1000) / 60000) for x in
                          np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + (exp_length / 5) * 1000,
                                    (exp_length / 5) * 1000)])
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    #axs.legend(lines1 + lines2, labels1 + labels2, loc='upper center', bbox_to_anchor=(0.5, 1.4), ncol=2, fontsize=FONT_SIZE)
    #axs.legend(loc='upper center', bbox_to_anchor=(0.5, 1.25), ncol=4)

    axs.grid(True)

    # # Save the plot
    # if not os.path.exists(output_dir):
    #     os.makedirs(output_dir)
    #
    # if output_pdf_flag:
    #     plt.savefig(os.path.join(output_dir, 'parallelism_curves_' + str(workload_name) + '.pdf'), bbox_inches='tight')
    # else:
    #     plt.savefig(os.path.join(output_dir, 'parallelism_curves_' + str(workload_name) + '.png'), bbox_inches='tight')
    # plt.close(fig)


output_pdf_flag = True
COLOR_MAP = {
    "Static": "grey",
    "DS2": "purple",
    "StreamSwitch": "green",
    "Sluice": "blue",
}
MARKER_MAP = {
    "Static": "x--",
    "DS2": "^-.",
    "StreamSwitch": "s:",
    "Sluice": "o-",
}

FONT_SIZE = 20

def main():
    raw_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
    output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
    overall_output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/part2/"
    window_size = 100 # for calcualte success rate
    # window_size = 500 # for draw success rate curve
    draw_lem_latency_flag = True
    exps_per_label_per_setting = {
        # "Linear-Road_30min": {
        #     "Static": "lr-ds2-ds2-5-8-60-1380-150-1300-10-1-50-3-1000-1-50-14-3333-2000-0.1-100-1-25-0.0-false-2500-0.8-2",
        #     # "Static-Adequate": "lr-ds2-ds2-5-8-60-1380-150-1300-10-1-50-4-1000-1-50-20-3333-2000-0.1-100-1-25-0.0-false-2500-0.8-2",
        #     "DS2": "lr-ds2-ds2-5-8-60-1380-150-1300-10-1-50-3-1000-1-50-27-3333-2000-0.1-100-1-25-0.0-true-2500-0.8-2",
        #     "StreamSwitch": "lr-streamswitch-streamswitch-5-8-60-1380-150-1300-10-1-50-3-1000-1-50-27-3333-2000-0.1-100-1-25-0.0-true-2500-0.8-2",
        #     "Sluice": "lr-streamsluice-streamsluice-5-8-60-1380-150-1300-10-1-50-3-1000-1-50-27-3333-2000-0.1-100-1-25-0.0-true-1000-0.8-2",
        # },
        "Stock-Analysis_30min": {
            "Static": "stock-ds2-ds2-5-8-60-1350-90-1000-20-1-200-4-3333-1-200-1-500-1-7-5000-3000-100-0.1-false-false-1",
            # "Static-Adequate": "stock-ds2-ds2-5-8-60-1350-90-1000-20-1-200-11-3333-1-200-2-500-1-15-5000-3000-100-0.1-false-false-1",
            "DS2": "stock-ds2-ds2-5-8-60-1350-90-1000-20-1-200-11-3333-1-200-2-500-1-15-5000-3000-100-0.1-true-false-1",
            "StreamSwitch": "stock-streamswitch-streamswitch-5-8-60-1350-90-1000-20-1-200-11-3333-1-200-2-500-1-15-5000-3000-100-0.1-true-false-1",
            #"Sluice": "stock-streamsluice-streamsluice-5-8-60-1350-90-1000-20-1-200-11-3333-1-200-2-500-1-15-5000-3000-100-0.1-true-true-1",
            "Sluice": "stock-streamsluice-streamsluice-5-8-60-1350-90-1000-20-1-200-11-3333-1-200-2-500-1-15-5000-3000-100-0.2-true-true-2",
        },
        # "Twitter_30min": {
        #     # # "Static": "tweet-streamsluice-streamsluice-5-60-1950-90-1500-1-14-6666-5-1000-1-50-1-50-2500-100-false-0.1-1",
        #     # # "Static-Adequate": "tweet-streamsluice-streamsluice-5-60-1950-90-1500-1-19-6666-9-1000-1-50-1-50-2500-100-false-0.1-1",
        #     # # "DS2": "tweet-ds2-ds2-5-60-1950-90-1500-1-19-6666-9-1000-1-50-1-50-2500-100-true-0.1-1",
        #     # # "Streamswitch": "tweet-streamswitch-streamswitch-5-60-1950-90-1500-1-19-6666-9-1000-1-50-1-50-2500-100-true-0.1-1",
        #     # #"Sluice": "tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-2500-100-true-0.1-2",
        #     "Static": "tweet-streamsluice-streamsluice-5-60-1350-90-1700-1-14-3333-5-500-1-50-1-50-1250-2000-100-false-0.1-1",
        #     # #"Static-Adequate": "tweet-streamsluice-streamsluice-5-60-1350-90-1700-1-19-3333-9-500-1-50-1-50-1250-2000-100-false-0.1-1",
        #     "DS2": "tweet-ds2-ds2-5-60-1350-90-1700-1-19-3333-9-500-1-50-1-50-1250-2000-100-true-0.1-1",
        #     "StreamSwitch": "tweet-streamswitch-streamswitch-5-60-1350-90-1700-1-19-3333-9-500-1-50-1-50-1250-2000-100-true-0.1-1",
        #     #"Sluice": "tweet-streamsluice-streamsluice-5-60-1350-90-3400-1-19-3333-9-500-1-50-1-50-1250-2000-100-true-0.1-1",
        #     #"Sluice": "tweet-streamsluice-streamsluice-5-60-1350-90-3400-1-19-3333-9-500-1-50-1-50-1250-2000-100-true-0.2-1",
        #     "Sluice": "tweet-streamsluice-streamsluice-5-60-1350-90-1700-1-19-3333-9-500-1-50-1-50-1250-2000-100-true-0.2-1",
        # },
    }

    fig, axs = plt.subplots(2, 3, figsize=(24, 10), layout='constrained')
    index = 0
    legend_elements = []

    for workload_name, exps_per_label in exps_per_label_per_setting.items():
        latency_bar_this_workload = 0
        latency_per_label = {}
        avg_parallelism_per_label = {}
        max_parallelism_per_label = {}
        min_parallelism_per_label = {}
        success_rate_per_label = {}
        arrival_curve = []
        latency_curves = {}
        parallelism_curves = {}
        for label, exps in exps_per_label.items():
            avg_parallelism_per_label[label] = []
            exp_name = exps
            if exp_name.startswith("lr"):
                latency_bar = int(exp_name.split('-')[-10])
                start_time = 180
                exp_length = 1200
            elif exp_name.startswith("tweet"):
                latency_bar = int(exp_name.split('-')[-5])
                start_time = 60 #30 #0 #150
                exp_length = 1200
            elif exp_name.startswith("stock"):
                latency_bar = int(exp_name.split('-')[-6])
                start_time = 120 #150
                exp_length = 1200
            else:
                latency_bar = int(exp_name.split('-')[-6])
                start_time = 120
                exp_length = 600
            all_latency, success_rate, latency_curve = draw_latency_curves(raw_dir, output_dir + exp_name + '/', exp_name,
                                                                          window_size,
                                                                          start_time, exp_length, latency_bar, draw_lem_latency_flag)
            if label.startswith("Static"):
                avg_parallelism, max_parallelism, min_parallelism, arrival_curve, parallelism_curve = draw_parallelism_curve(raw_dir, output_dir + exp_name + '/', exp_name, window_size,
                                                            start_time, exp_length, True, [])
            else:
                avg_parallelism, max_parallelism, min_parallelism, trash, parallelism_curve = draw_parallelism_curve(raw_dir, output_dir + exp_name + '/', exp_name, window_size,
                                                            start_time, exp_length, True, arrival_curve)
            latency_bar_this_workload = latency_bar
            latency_per_label[label] = all_latency
            latency_curves[label] = latency_curve
            parallelism_curves[label] = parallelism_curve
            success_rate_per_label[label] = success_rate
            avg_parallelism_per_label[label] = avg_parallelism
            max_parallelism_per_label[label] = max_parallelism
            min_parallelism_per_label[label] = min_parallelism
            if index == 0:
                legend_elements.append(
                    Line2D([0], [0], linestyle=MARKER_MAP[label][1:], markersize=6, marker=MARKER_MAP[label][0],
                           color=COLOR_MAP[label], label=label))  # , markersize=8))
        # plot_latency_cdf(latency_per_label, latency_bar_this_workload, overall_output_dir, workload_name)
        # plot_average_latency(latency_per_label, overall_output_dir, workload_name)
        # plot_success_rate_bar(success_rate_per_label, overall_output_dir, workload_name)
        # plot_avg_parallelism_bar(avg_parallelism_per_label, overall_output_dir, workload_name)
        plot_latency_curves(latency_curves, latency_bar_this_workload, overall_output_dir, start_time, exp_length, workload_name, axs[0][index], index == 0)
        plot_parallelism_curves(parallelism_curves, arrival_curve, overall_output_dir, start_time, exp_length,
                           workload_name, axs[1][index], index == 0, index == 2)
        #plot_parallism_curves(parallelism_curves, arrival_curve, overall_output_dir, workload_name)
        print("success rate: " + str(success_rate_per_label))
        print("mean parallelism:" + str(avg_parallelism_per_label))
        print("min parallelism:" + str(min_parallelism_per_label))
        print("max parallelism:" + str(max_parallelism_per_label))
        print("Latency: " + str(calculate_latency_stats(latency_per_label)))
        index += 1
    legend_elements.append(
        Line2D([0], [0], linestyle="-",
               color="r", label="Arrival Rate"))
    legend_elements.append(
        Line2D([0], [0], linestyle="--",
               color="r", label="Latency Limit"))
    fig.legend(handles=legend_elements, loc='upper center', bbox_to_anchor=(0.5, 1.1), ncol=6, markerscale=5)

    if output_pdf_flag:
        plt.savefig(overall_output_dir + "all_in_one.pdf", bbox_inches='tight')
    else:
        plt.savefig(overall_output_dir + "all_in_one.png", bbox_inches='tight')


if __name__ == "__main__":
    main()

