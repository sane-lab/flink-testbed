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

# Function to plot success rates
def plot_success_rate_bar(xs_per_label, success_rate_per_label, output_dir, workload_name: str, dimension: str):
    labels = list(success_rate_per_label.keys())
    xs = xs_per_label[labels[0]]  # Assuming all labels have the same user limits for simplicity

    fig, ax = plt.subplots(figsize=(12, 5))

    # Set width of bars and positions
    bar_width = 0.3 #0.15
    x = np.arange(len(xs))

    # Plot bars for each label
    for i, label in enumerate(labels):
        success_rates = success_rate_per_label[label]
        if label == "":
            ax.bar(x + i * bar_width, success_rates, width=bar_width)
        else:
            ax.bar(x + i * bar_width, success_rates, width=bar_width, label=("User_Limit=" + label))

    # Add labels, title, and custom x-axis tick labels
    ax.set_xlabel(dimension)
    ax.set_ylabel('Success Rate')
    min_rate = min([min(rates) for rates in success_rate_per_label.values()])
    if min_rate >= 0.9:
        ax.set_ylim(0.9, 1.0)
        ax.set_yticks(np.arange(0.9, 1.00, 0.01))
    elif min_rate >= 0.85:
        ax.set_ylim(0.85, 1.0)
        ax.set_yticks(np.arange(0.85, 1.00, 0.03))
    else:
        ax.set_ylim(0.0, 1.0)
        ax.set_yticks(np.arange(0.0, 1.0, 0.1))

    ax.set_xticks(x + bar_width * (len(labels) - 1) / 2)
    ax.set_xticklabels(xs)
    ax.set_title('Success Rates by ' + dimension)
    #ax.legend()
    ax.grid(True, axis='y')

    # Save the plot
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(os.path.join(output_dir, 'success_rate_bar_' + str(workload_name) + '.png'), bbox_inches='tight')
    plt.close(fig)


def plot_avg_latency(x_per_label, weighted_success_rate_per_label, output_dir, workload_name: str, dimension: str):
    labels = list(weighted_success_rate_per_label.keys())
    user_limits = x_per_label[labels[0]]  # Assuming all labels have the same user limits for simplicity

    fig, ax = plt.subplots(figsize=(12, 5))

    # Set width of bars and positions
    bar_width = 0.3 #0.15
    x = np.arange(len(user_limits))

    # Plot bars for each label
    for i, label in enumerate(labels):
        success_rates = weighted_success_rate_per_label[label]
        ax.bar(x + i * bar_width, success_rates, width=bar_width, label=("User_Limit=" + label))

    # Add labels, title, and custom x-axis tick labels
    ax.set_xlabel(dimension)
    ax.set_ylabel('Average GT Latency')

    ax.set_xticks(x + bar_width * (len(labels) - 1) / 2)
    ax.set_xticklabels(user_limits)
    ax.set_title('Average Ground Truth Latency by ' + dimension)
    #ax.legend()
    ax.grid(True, axis='y')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'avg_latency_' + str(workload_name) + '.png', bbox_inches='tight')
    plt.close(fig)



def plot_avg_parallelism_bar(x_per_label, avg_parallelism_per_label, output_dir, workload_name: str, dimension:str):
    labels = list(avg_parallelism_per_label.keys())
    xs = x_per_label[labels[0]]  # Assuming all labels have the same user limits for simplicity

    fig, ax = plt.subplots(figsize=(12, 5))

    # Set width of bars and positions
    bar_width = 0.3 #0.15
    x = np.arange(len(xs))

    # Plot bars for each label
    for i, label in enumerate(labels):
        avg_parallelisms = avg_parallelism_per_label[label]
        ax.bar(x + i * bar_width, avg_parallelisms, width=bar_width, label=("User_Limit=" + label))

    # Add labels, title, and custom x-axis tick labels
    ax.set_xticks(x + bar_width * (len(labels) - 1) / 2)
    ax.set_xticklabels(xs)
    plt.xlabel(dimension)
    plt.ylabel('Avg Parallelism')
    plt.title('Avg Parallelism by ' + dimension)
    #ax.legend()
    ax.grid(True, axis='y')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'avg_parallelism_curve_' + str(workload_name) + '.png', bbox_inches='tight')
    plt.close(fig)

def main():
    overall_output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/part4/"
    success_rate_per_label = {}
    name_list = [
        #"tweet",
        #"lr",
        #"stock",
        "micro",
    ]
    for name in name_list:
        overall_output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/part4/" + name + "/"
        with open("system_results_" + name + ".txt", "r") as file:
            lines = file.readlines()
        dimension = ""
        for line in lines:
            if line.startswith("X"):
                continue
            splits = line.strip().split()
            if len(splits) == 2:
                workload_name = splits[0]
                dimension = splits[1]
                success_rate_per_label = {}
                avg_parallelism_per_label = {}
                avg_latency_per_label = {}
                x_per_label = {}
            elif len(splits) == 1 and splits[0] == "end":
                print(success_rate_per_label)
                print(avg_latency_per_label)
                print(avg_parallelism_per_label)
                plot_success_rate_bar(x_per_label, success_rate_per_label, overall_output_dir, workload_name, dimension)
                plot_avg_latency(x_per_label, avg_latency_per_label, overall_output_dir, workload_name, dimension)
                plot_avg_parallelism_bar(x_per_label, avg_parallelism_per_label, overall_output_dir, workload_name, dimension)
            elif len(splits) > 1:
                label = splits[3]
                x = splits[2]
                if (dimension == "User_Limit"):
                    label = ""

                success_rate = float(splits[4])
                weighted_success_rate = float(splits[5])
                avg_parallelism = float(splits[6])
                if(label not in success_rate_per_label):
                    success_rate_per_label[label] = []
                    avg_parallelism_per_label[label] = []
                    x_per_label[label] = []
                    avg_latency_per_label[label] = []
                x_per_label[label].append(x)
                success_rate_per_label[label].append(success_rate)
                avg_latency_per_label[label].append(weighted_success_rate)
                avg_parallelism_per_label[label].append(avg_parallelism)
                print(label)


if __name__ == "__main__":
    main()
