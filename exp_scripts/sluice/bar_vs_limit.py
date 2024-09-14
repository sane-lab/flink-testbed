import math
import os
import numpy as np
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt

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


def add_latency_limit_marker(plt, latency_limit):
    x = [0, 10000000]
    y = [latency_limit, latency_limit]
    plt.plot(x, y, "--", label="Limit", color='red', linewidth=1.5)


def calculate_latency_limits(p99_latencies):
    filtered_latencies = p99_latencies
    if not filtered_latencies:
        raise ValueError("No latencies found in the specified time range.")

    sorted_latencies = sorted(filtered_latencies)
    target_p95_index = int(len(sorted_latencies) * 0.95) - 1
    latency_limit_95 = sorted_latencies[target_p95_index]
    target_p99_index = int(len(sorted_latencies) * 0.99) - 1
    latency_limit_99 = sorted_latencies[target_p99_index]

    return latency_limit_95, latency_limit_99


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


def draw_latency_curves(raw_dir, output_dir, exp_name, window_size, start_time, exp_length, latency_limit):
    exps = [
        ["GroundTruth", exp_name, "blue", "o"]
    ]

    average_ground_truth_latencies = []
    initial_times = []
    for i in range(len(exps)):
        result = read_ground_truth_latency(raw_dir, exps[i][1], window_size)
        average_ground_truth_latencies += [result[0]]
        initial_times += [result[1]]

    success_rate_per_exps = {}
    for i in range(len(exps)):
        total_success = len([x for x in range(len(average_ground_truth_latencies[i][0])) if
                             average_ground_truth_latencies[i][0][x] >= start_time * 1000 and
                             average_ground_truth_latencies[i][0][x] <= (start_time + 1800) * 1000 and
                             average_ground_truth_latencies[i][1][x] <= latency_limit])
        total_windows = len([x for x in range(len(average_ground_truth_latencies[i][0])) if
                             average_ground_truth_latencies[i][0][x] >= start_time * 1000 and
                             average_ground_truth_latencies[i][0][x] <= (start_time + 1800) * 1000])
        success_rate_per_exps[exps[i][0]] = total_success / float(total_windows)

        groundtruth_p99_latency_in_range = [average_ground_truth_latencies[i][1][x] for x in
                                            range(len(average_ground_truth_latencies[i][0])) if
                                            average_ground_truth_latencies[i][0][x] >= start_time * 1000 and
                                            average_ground_truth_latencies[i][0][x] <= (start_time + exp_length) * 1000]
        result_limits = calculate_latency_limits(groundtruth_p99_latency_in_range)
        print(f"in range ground truth P99 limit: {result_limits[1]}, P95 limit: {result_limits[0]}")

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
    axes.set_ylim(-1, 500)
    axes.set_yticks(np.arange(0, 550, 50))
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.close(fig)

    return result_limits[1], success_rate_per_exps['GroundTruth']


def main():
    raw_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
    output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
    overall_output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
    window_size = 100
    latency_limit = 1000
    start_time = 60
    exp_length = 300


    setting1 = {
        "1 op": [
            "",
            "",
            "",
            "",
        ],
        "2 ops": [
            "",
            "",
            "",
            "",
        ],
        "3 ops": [
            "",
            "",
            "",
            "",
        ],
    }

    settings = [
        setting1,
    ]

    for index in range(0, len(settings)):
        print("Start draw setting " + str(index + 1))
        setting = settings[index]

        p99limits_per_labels = {}
        successrate_per_labels = {}
        resource_per_labels = {}

        for label, exps in setting.items():
            print("Start draw label" + label)
            p99limits_per_labels[label] = []
            successrate_per_labels[label] = []
            resource_per_labels[label] = []
            for exp_name in exps:
                if exp_name.split('-')[-2] == "false": # Show workload
                    draw_arrival_rate()
                else:
                    latency_limit = int(exp_name.split('-')[-6])
                    p99_latency_limit, latency_bar_success_rate = draw_latency_curves(raw_dir, output_dir, exp_name, window_size,
                                                                                  start_time, exp_length, latency_limit)
                    print(f"P99 Latency Limit: {p99_latency_limit}")
                    print(f"Latency Bar Success Rate: {latency_bar_success_rate}")
                    avg_parallelism = draw_parallelism()
                    print(f"Average parallelism: {avg_parallelism}")

                    p99limits_per_labels[label] += [p99_latency_limit]
                    successrate_per_labels[label] += [latency_bar_success_rate]
                    resource_per_labels[label] += [avg_parallelism]

        draw_setting_figures()
        
if __name__ == "__main__":
    main()

