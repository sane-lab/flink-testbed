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


def clean_string(input_string):
    import re
    # Remove the last bracket and its content
    modified_string = re.sub(r'\s*\([^)]*\)$', '', input_string)

    return modified_string

# Function to plot success rates
def plot_success_rate_bar(xs_per_label, success_rate_per_label, output_dir, workload_name: str, dimension: str):
    labels = list(success_rate_per_label.keys())
    xs = xs_per_label[labels[0]]  # Assuming all labels have the same user limits for simplicity

    fig, ax = plt.subplots(figsize=(12, 3.5))

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
    ax.set_ylabel('Success Rate(%)')
    ax.yaxis.set_label_coords(-0.075, 0.4)
    min_rate = min([min(rates) for rates in success_rate_per_label.values()])
    if min_rate >= 0.95:
        ax.set_ylim(0.95, 1.0)
        ax.set_yticks(np.arange(0.95, 1.00, 0.01))
        ax.set_yticklabels([math.ceil(x * 100) for x in np.arange(0.95, 1.00, 0.01)])
    elif min_rate >= 0.9:
        ax.set_ylim(0.9, 1.0)
        ax.set_yticks(np.arange(0.9, 1.00, 0.02))
        ax.set_yticklabels([math.ceil(x * 100) for x in np.arange(0.9, 1.00, 0.02)])
    elif min_rate >= 0.80:
        ax.set_ylim(0.80, 1.0)
        ax.set_yticks(np.arange(0.80, 1.00, 0.04))
        ax.set_yticklabels([math.ceil(x * 100) for x in np.arange(0.80, 1.00, 0.04)])
    else:
        ax.set_ylim(0.0, 1.0)
        ax.set_yticks(np.arange(0.0, 1.0, 0.2))
        ax.set_yticklabels([math.ceil(x * 100) for x in np.arange(0.0, 1.0, 0.2)])

    ax.set_xticks(x + bar_width * (len(labels) - 1) / 2)
    ax.set_xticklabels(xs)
    #ax.legend()
    ax.grid(True, axis='y')

    # Save the plot
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if output_pdf_flag:
        plt.savefig(os.path.join(output_dir, 'success_rate_bar_' + str(workload_name) + '.pdf'), bbox_inches='tight')
    else:
        ax.set_title('Success Rates by ' + clean_string(dimension))
        plt.savefig(os.path.join(output_dir, 'success_rate_bar_' + str(workload_name) + '.png'), bbox_inches='tight')
    plt.close(fig)


def plot_avg_latency(x_per_label, avg_latency_per_label, output_dir, workload_name: str, dimension: str):
    labels = list(avg_latency_per_label.keys())
    user_limits = x_per_label[labels[0]]  # Assuming all labels have the same user limits for simplicity

    fig, ax = plt.subplots(figsize=(12, 4))

    # Set width of bars and positions
    bar_width = 0.3 #0.15
    x = np.arange(len(user_limits))

    # Plot bars for each label
    for i, label in enumerate(labels):
        success_rates = avg_latency_per_label[label]
        ax.bar(x + i * bar_width, success_rates, width=bar_width, label=("User_Limit=" + label))

    max_latency = max([max(latencys) for latencys in avg_latency_per_label.values()])
    if max_latency <= 800:
        ax.set_ylim(0, 800)
        ax.set_yticks(np.arange(0, 1000, 200))
    elif max_latency <= 1000:
        ax.set_ylim(0, 1000)
        ax.set_yticks(np.arange(0, 1200, 200))
    else:
        ax.set_ylim(0, 1600)
        ax.set_yticks(np.arange(0, 1500, 500))



    # Add labels, title, and custom x-axis tick labels
    ax.set_xlabel(dimension)
    ax.set_ylabel('Avg Latency (ms)')
    ax.yaxis.set_label_coords(-0.1, 0.4)
    ax.set_xticks(x + bar_width * (len(labels) - 1) / 2)
    ax.set_xticklabels(user_limits)

    #ax.legend()
    ax.grid(True, axis='y')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if output_pdf_flag:
        plt.savefig(output_dir + 'avg_latency_' + str(workload_name) + '.pdf', bbox_inches='tight')
    else:
        ax.set_title('Average End-to-End Latency by ' + clean_string(dimension))
        plt.savefig(output_dir + 'avg_latency_' + str(workload_name) + '.png', bbox_inches='tight')
    plt.close(fig)



def plot_avg_parallelism_bar(x_per_label, avg_parallelism_per_label, output_dir, workload_name: str, dimension:str):
    labels = list(avg_parallelism_per_label.keys())
    xs = x_per_label[labels[0]]  # Assuming all labels have the same user limits for simplicity

    fig, ax = plt.subplots(figsize=(12, 4))

    # Set width of bars and positions
    bar_width = 0.3 #0.15
    x = np.arange(len(xs))

    # Plot bars for each label
    for i, label in enumerate(labels):
        avg_parallelisms = avg_parallelism_per_label[label]
        ax.bar(x + i * bar_width, avg_parallelisms, width=bar_width, label=("User_Limit=" + label))

    max_parallelism = max([max(parallelism) for parallelism in avg_parallelism_per_label.values()])
    if max_parallelism <= 20:
        ax.set_ylim(0, 20)
        ax.set_yticks(np.arange(0, 20, 5))
    elif max_parallelism <= 21:
        ax.set_ylim(0, 21)
        ax.set_yticks(np.arange(0, 25, 5))
    else:
        ax.set_ylim(0, 40)
        ax.set_yticks(np.arange(0, 40, 10))

    # Add labels, title, and custom x-axis tick labels
    ax.set_xticks(x + bar_width * (len(labels) - 1) / 2)
    ax.set_xticklabels(xs)
    plt.xlabel(dimension)
    plt.ylabel('Avg # of Slots')
    ax.yaxis.set_label_coords(-0.075, 0.4)
    #ax.legend()
    ax.grid(True, axis='y')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if output_pdf_flag:
        plt.savefig(output_dir + 'avg_parallelism_curve_' + str(workload_name) + '.pdf', bbox_inches='tight')
    else:
        plt.title('Avg Resources by ' + clean_string(dimension))
        plt.savefig(output_dir + 'avg_parallelism_curve_' + str(workload_name) + '.png', bbox_inches='tight')
    plt.close(fig)


output_pdf_flag=True
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
                dimension = dimension.replace('_', ' ')
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
                if (dimension == "User Limit(ms)"):
                    label = ""

                success_rate = float(splits[4])
                avg_latency = float(splits[5])
                avg_parallelism = float(splits[6])
                if(label not in success_rate_per_label):
                    success_rate_per_label[label] = []
                    avg_parallelism_per_label[label] = []
                    x_per_label[label] = []
                    avg_latency_per_label[label] = []
                x_per_label[label].append(x)
                success_rate_per_label[label].append(success_rate)
                avg_latency_per_label[label].append(avg_latency)
                avg_parallelism_per_label[label].append(avg_parallelism)
                print(label)


if __name__ == "__main__":
    main()
