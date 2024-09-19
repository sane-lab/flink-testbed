import math
import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

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
                        break
                latency_bar[time] = bar
            if (len(split) >= 8 and split[1] == "[AUTOTUNE]" and split[4] == "set" and split[5] == "bar" and split[6] == "to" and split[7] == "lowerbound:"):
                time = int(split[3])
                bar = int(split[8].rstrip(','))
                latency_bar[time] = bar

    return [lem_latency, latency_bar]

def add_latency_limit_marker(plt, latency_limit):
    x = [0, 10000000]
    y = [latency_limit, latency_limit]
    plt.plot(x, y, "--", label="Limit", color='red', linewidth=1.5)


def add_latency_bar_curve(plt, latency_bar:dict[int, int], initial_time):
    last_time = 0
    last_y = 0
    for time in latency_bar.keys():
        x = [last_time, time - initial_time]
        y = [last_y, last_y]
        plt.plot(x, y, 'o--', label="Latency Bar", color='red', linewidth=1.5)
        last_y = latency_bar[time]
        last_time = time - initial_time
    x = [last_time, 10000000]
    y = [last_y, last_y]
    plt.plot(x, y, 'o--', label="Latency Bar", color='red', linewidth=1.5)

def draw_latency_curves(raw_dir, output_dir, exp_name, window_size, start_time, exp_length, latency_limit):
    exps = [
        ["GroundTruth", exp_name, "blue", "o"]
    ]

    average_ground_truth_latencies = []
    lem_latencies = []
    latency_bar = []
    initial_times = []
    for i in range(len(exps)):
        result = read_ground_truth_latency(raw_dir, exps[i][1], window_size)
        average_ground_truth_latencies += [result[0]]
        initial_times += [result[1]]
        result = readLEMLatencyAndSpikeAndBar(raw_dir, exps[i][1])
        lem_latencies += [result[0]]
        latency_bar += [result[1]]

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
    axes.set_xticks(np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + 60000, 60000))
    axes.set_xticklabels([int((x - start_time * 1000) / 1000) for x in
                          np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + 60000, 60000)])
    axes.set_ylim(0, 2000)
    axes.set_yticks(np.arange(0, 2200, 200))
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.close(fig)

    # Plotting the latency curve
    fig, ax = plt.subplots(figsize=(12, 5))
    for i in range(len(exps)):
        lem_latencies[i][0] = [x - initial_times[0] for x in lem_latencies[i][0]]
        plt.plot(lem_latencies[i][0], lem_latencies[i][1], '-', color=exps[i][2], markersize=4, linewidth=3,
                 label="Estimated Latency")
        add_latency_bar_curve(plt, latency_bar[i], initial_times[i])

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
    axes.set_xticks(np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + 60000, 60000))
    axes.set_xticklabels([int((x - start_time * 1000) / 1000) for x in
                          np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + 60000, 60000)])
    axes.set_ylim(0, 2000)
    axes.set_yticks(np.arange(0, 2200, 200))
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'latency_bar.png', bbox_inches='tight')
    plt.close(fig)

    return success_rate


def main():
    raw_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
    output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
    overall_output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/autotuner/"
    window_size = 100
    start_time = 60 #30 #60
    exp_length = 300
    exps = [
        "autotune-setting1-true-streamsluice-streamsluice-30-100-300-1-0.2-1-2.0-2-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        "autotune-setting1-true-streamsluice-streamsluice-30-100-300-1-0.2-2-0.8-2-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        "autotune-setting1-true-streamsluice-streamsluice-30-100-300-2-0.2-1-2.0-2-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
        "autotune-setting1-true-streamsluice-streamsluice-30-100-300-2-0.2-2-0.8-2-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
    ]
    for exp_name in exps:
        latency_bar = int(exp_name.split('-')[-6])
        success_rate = draw_latency_curves(raw_dir, output_dir + exp_name + '/', exp_name,
                                                                      window_size,
                                                                      start_time, exp_length, latency_bar)

if __name__ == "__main__":
    main()

