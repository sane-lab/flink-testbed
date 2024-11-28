import math
import sys
import re
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
def plot_key_arrival_rate_distributions(arrival_rates_per_operator_along_time, output_dir, start_index, index_length):
    #print(arrival_rates_per_operator_along_time[1])
    print("!!!" + str(len(arrival_rates_per_operator_along_time[1])))
    for operator in arrival_rates_per_operator_along_time[1][0].keys():
        arrival_rates_list = [arrival_rates_per_operator_along_time[1][i][operator] for i in range(start_index, min(len(arrival_rates_per_operator_along_time[1]), start_index + index_length))]
        """
        Plots the changing key-level arrival rate distribution.
    
        Parameters:
            arrival_rates_list (list[dict]): A list of dictionaries where each dictionary represents the
                                             key-level arrival rate at a specific time or scenario.
            output_file (str): The output file name for the figure.
        """
        # Prepare data for plotting
        #time_steps = [(arrival_rates_per_operator_along_time[0][i] - arrival_rates_per_operator_along_time[0][0])/1000 for i in range(0, min(len(arrival_rates_per_operator_along_time[0]), 100))]
        time_steps = range(len(arrival_rates_list))
        all_keys = set(key for rates in arrival_rates_list for key in rates.keys())
        all_keys = sorted(all_keys)[0:8]  # Sort keys for consistent order

        #total_arrival_this_operator = [sum([arrival_rate for arrival_rate in arrival_rates.values()]) for arrival_rates in arrival_rates_list]
        total_arrival_this_operator = []
        for rates in arrival_rates_list:
            total_arrival_this_operator.append(sum(rates.values()) * 1000)
            #print(rates, sum(rates.values()))

        # Generate data matrix for plotting
        data_matrix = []
        for rates in arrival_rates_list:
            data_matrix.append([rates.get(key, 0) * 1000 for key in all_keys])  # Use 0 for missing keys

        # Convert to a numpy array for easier manipulation
        data_matrix = np.array(data_matrix)

        # Plot
        fig, ax = plt.subplots(figsize=(12, 6))

        for idx, key in enumerate(all_keys):
            ax.plot(time_steps, data_matrix[:, idx], marker='o', label=f"Key {key}")

        ax.set_xlabel("Time Steps")
        ax.set_ylabel("Arrival Rate")
        ax.set_title("Changing Key-Level Arrival Rate Distribution")
        ax.legend(title="Keys", bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(True)

        # Save and display the figure
        plt.tight_layout()
        import os
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        plt.savefig(output_dir + "key_arrival_" + operator + ".png")

        # Plot
        fig, ax = plt.subplots(figsize=(12, 6))
        print(total_arrival_this_operator)
        ax.plot(time_steps, total_arrival_this_operator, marker='o')

        ax.set_xlabel("Time Steps")
        ax.set_ylabel("Arrival Rate")
        ax.set_title("Operator Arrival Rate " + operator)
        ax.set_ylim(0, 7000)
        #ax.legend(title="Keys", bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(True)

        # Save and display the figure
        plt.tight_layout()
        import os
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        plt.savefig(output_dir + "operator_arrival_" + operator + ".png")


def plot_task_arrival_rate_distributions(arrival_rates_per_operator_along_time, output_dir, start_index, index_length):
    #print(arrival_rates_per_operator_along_time[1])
    for operator in arrival_rates_per_operator_along_time[1][0].keys():
        arrival_rates_list = [arrival_rates_per_operator_along_time[1][i][operator] for i in
                              range(start_index, min(len(arrival_rates_per_operator_along_time[1]), start_index + index_length))]
        """
        Plots the changing key-level arrival rate distribution.

        Parameters:
            arrival_rates_list (list[dict]): A list of dictionaries where each dictionary represents the
                                             key-level arrival rate at a specific time or scenario.
            output_file (str): The output file name for the figure.
        """
        # Prepare data for plotting
        # time_steps = [(arrival_rates_per_operator_along_time[0][i] - arrival_rates_per_operator_along_time[0][0])/1000 for i in range(0, min(len(arrival_rates_per_operator_along_time[0]), 100))]
        time_steps = range(len(arrival_rates_list))
        all_tasks = set(task for rates in arrival_rates_list for task in rates.keys())
        all_tasks = [operator + "_" + str(i) for i in range(0, 10)]  # Sort keys for consistent order

        # Generate data matrix for plotting
        data_matrix = []
        for rates in arrival_rates_list:
            data_matrix.append([rates.get(key, 0) * 1000 for key in all_tasks])  # Use 0 for missing keys

        # Convert to a numpy array for easier manipulation
        data_matrix = np.array(data_matrix)

        # Plot
        fig, ax = plt.subplots(figsize=(12, 6))

        for idx, key in enumerate(all_tasks):
            ax.plot(time_steps, data_matrix[:, idx], marker='o', label=f"Task {key}")

        ax.set_xlabel("Time Steps")
        ax.set_ylabel("Arrival Rate")
        ax.set_title("Changing Task-Level Arrival Rate Distribution")
        ax.legend(title="Task", bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(True)

        # Save and display the figure
        plt.tight_layout()
        import os
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        plt.savefig(output_dir + "task_arrival_" + operator + ".png")


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

def parsePerTaskValue(splits):
    taskValues = {}
    for split in splits:
        split = split.lstrip("{").rstrip("}").rstrip(",")
        words = split.split("=")
        taskName = words[0]
        operator = taskName.split("_")[0]
        value = float(words[1])
        if operator not in taskValues:
            taskValues[operator] = {}
        taskValues[operator][taskName] = value
    return taskValues


def retrieve_key_arrival_rate(rawDir, expName):
    initialTime = -1
    lastTime = 0
    key_arrival_per_operator_along_time = [[], []]
    task_arrival_per_operator_along_time = [[], []]
    scalings = []

    taskExecutors = [] #"flink-samza-taskexecutor-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("taskexecutor") == 1:
                taskExecutors += [file]
    # for taskExecutor in taskExecutors:
    #     groundTruthPath = rawDir + expName + "/" + taskExecutor
    #     print("Reading ground truth file:" + groundTruthPath)
    #     counter = 0
    #     with open(groundTruthPath) as f:
    #         lines = f.readlines()
    #         for i in range(0, len(lines)):
    #             line = lines[i]
    #             split = line.rstrip().split()
    #             counter += 1
    #             if (counter % 5000 == 0):
    #                 print("Processed to line:" + str(counter))
    #             if(split[0] == "GT:"):
    #                 completedTime = int(split[2].rstrip(","))
    #                 latency = int(split[3].rstrip(","))
    #                 arrivedTime = completedTime - latency
    #                 if (arrivedTime < 0):
    #                     print("!!!! " + str(i) + "  " + line)
    #                 if (initialTime == -1 or initialTime > arrivedTime):
    #                     initialTime = arrivedTime
    #                 if (lastTime < completedTime):
    #                     lastTime = completedTime
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

    with open(streamSluiceOutputPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if (len(split) >= 6 and split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "key" and split[
                5] == "arrivalRate:"):
                time = int(split[3])
                key_arrival_per_operator = parse_key_metrics(''.join(split[6:]).strip())
                key_arrival_per_operator_along_time[0].append(time)
                key_arrival_per_operator_along_time[1].append(key_arrival_per_operator)
                if(time - key_arrival_per_operator_along_time[0][0] > 600 * 1000):
                    break
            if (len(split) >= 6 and split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[
                5] == "arrivalRate:"):
                time = int(split[3])
                task_arrival_per_operator = parsePerTaskValue(split[6:])
                task_arrival_per_operator_along_time[0].append(time)
                task_arrival_per_operator_along_time[1].append(task_arrival_per_operator)
                if (time - task_arrival_per_operator_along_time[0][0] > 600 * 1000):
                    break

                #print(key_arrival_per_operator)
    return [key_arrival_per_operator_along_time, task_arrival_per_operator_along_time, initialTime, scalings]


def main():
    raw_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
    output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
    experiment_list = [
        #"setting6--streamsluice-ds2-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-1250-1-20-1-1250-1-20-1-1250-17-1000-1250-1250-3000-100-1-false-1",
        #"setting7--streamsluice-ds2-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.2-1250-3000-100-1-false-1",
        #"stock-streamsluice-streamsluice--1950-90-1000-20-1-200-11-2500-1-200-2-500-1-15-3333-1000-100-0.1-false-false-1"
        #"tweet-streamsluice-streamsluice-1-1950-90-1500-1-19-6666-9-1000-1-50-1-50-1000-100-false-0.4-1",
        #"lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-27-8000-4-2000-1-50-1000-0.1-100-1-0-0.0-true-3000-1",
        #"lr-streamsluice-streamsluice--1980-150-1300-10-1-50-27-8000-4-2000-1-50-1000-0.1-100-1-0-0.0-false-3000-1",
        #"lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-27-8000-4-2000-1-50-1000-0.1-100-1-0-0.0-true-3000-0.6-1",
        #"lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-27-8000-4-2000-1-50-1000-0.1-100-1-0-0.0-true-3000-0.8-1"
    ]
    start_index = 1800
    index_length = 50
    for exp_name in experiment_list:
        key_arrival_along_time, task_arrival_along_time, trash, trash1 = retrieve_key_arrival_rate(raw_dir, exp_name)
        plot_key_arrival_rate_distributions(key_arrival_along_time, output_dir + exp_name + "/", start_index, index_length)
        plot_task_arrival_rate_distributions(task_arrival_along_time, output_dir + exp_name + "/", start_index, index_length)

if __name__ == "__main__":
    main()
