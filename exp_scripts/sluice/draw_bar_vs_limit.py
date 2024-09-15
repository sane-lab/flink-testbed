import json
import matplotlib.pyplot as plt
import os

import numpy as np

def get_max_value(data: dict[str, list[float]]) -> float:
    max_value = float('-inf')
    for values in data.values():
        if values:  # Ensure the list is not empty
            max_value = max(max_value, max(values))
    return max_value

# Define the function to draw metrics by latency limit
def draw_metrics_by_latency_limit(latency_limits, p99limits_per_labels, successrate_per_labels, resource_per_labels,
                                  output_dir):
    # Create a figure for P99 Latency Limits
    fig, axs = plt.subplots(figsize=(10, 5))
    for label, p99limits in p99limits_per_labels.items():
        plt.plot(latency_limits[label], p99limits, label=label, marker='o')
    ax1 = axs
    if get_max_value(p99limits_per_labels) <= 500:
        ax1.set_ylim(0, 500)
        ax1.set_yticks(np.arange(0, 600, 100))
    else:
        ax1.set_ylim(0, 3000)
        ax1.set_yticks(np.arange(0, 3000, 300))
    ax1.set_xlim(0, 1750)
    ax1.set_xticks(np.arange(0, 2000, 250))
    # Add vertical dashed line at x=300
    plt.axvline(x=300, color='black', linestyle='--')
    # Add label for the vertical line
    plt.text(300, ax1.get_ylim()[1] * 0.9, 'x=300', color='black', rotation=90, verticalalignment='center')

    plt.xlabel('Latency Bar')
    plt.ylabel('P99 Latency Limit')
    plt.title('P99 Latency Limits vs Latency Bar')
    plt.legend()
    plt.grid(True)
    import os
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + "p99limit_vs_bar.png", bbox_inches='tight')
    plt.close(fig)

    # Create a figure for Success Rates
    fig, axs = plt.subplots(figsize=(10, 5))
    for label, success_rates in successrate_per_labels.items():
        plt.plot(latency_limits[label], success_rates, label=label, marker='o')
    plt.xlabel('Latency Bar')
    plt.ylabel('Latency Bar Success Rate')
    plt.title('Success Rates vs Latency Bar')
    ax1 = axs
    #ax1.set_ylim(0, 1.05)
    #ax1.set_yticks(np.arange(0, 1.05, 0.10))
    ax1.set_xlim(0, 1750)
    ax1.set_xticks(np.arange(0, 2000, 250))
    # Add vertical dashed line at x=300
    plt.axvline(x=300, color='black', linestyle='--')
    # Add label for the vertical line
    plt.text(300, ax1.get_ylim()[1] * 0.9, 'x=300', color='black', rotation=90, verticalalignment='center')

    plt.legend()
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + "success_rate_vs_bar.png", bbox_inches='tight')
    plt.close(fig)

    # Create a figure for Resource Usage (Average Parallelism)
    fig, axs = plt.subplots(figsize=(10, 5))
    for label, resource_usages in resource_per_labels.items():
        plt.plot(latency_limits[label], resource_usages, label=label, marker='o')
    ax1 = axs
    #ax1.set_ylim(0, 32)
    #ax1.set_yticks(np.arange(0, 36, 4))
    ax1.set_xlim(0, 1750)
    ax1.set_xticks(np.arange(0, 2000, 250))
    # Add vertical dashed line at x=300
    plt.axvline(x=300, color='black', linestyle='--')
    # Add label for the vertical line
    plt.text(300, ax1.get_ylim()[1] * 0.9, 'x=300', color='black', rotation=90, verticalalignment='center')

    plt.xlabel('Latency Limit')
    plt.ylabel('Average Parallelism')
    plt.title('Resource Usage vs Latency Bar')
    plt.legend()
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + "resource_vs_bar.png", bbox_inches='tight')
    plt.close(fig)


# Load and draw each setting from its individual file
results_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/autotuner/"
settings = [
        #"setting_1",
        #"setting_2",
        "setting_3",
        "setting_4",
        "setting_5",
        #"setting_6",
        #"setting_7",
    ]
for setting in settings:
    file_name = setting + ".json"
    if file_name.endswith('.json'):
        with open(os.path.join(results_dir, file_name), 'r') as f:
            setting_data = json.load(f)
        print("Drawing for setting " + file_name)
        latency_bars = setting_data['bar_per_labels']
        p99limits_per_labels = setting_data['p99limits_per_labels']
        successrate_per_labels = setting_data['successrate_per_labels']
        resource_per_labels = setting_data['resource_per_labels']

        # Define the output directory based on the setting file name
        setting_index = file_name.split('_')[1].split('.')[0]
        output_dir = results_dir + f"results/setting_{setting_index}/"
        # Draw figures
        draw_metrics_by_latency_limit(latency_bars, p99limits_per_labels, successrate_per_labels, resource_per_labels,
                                      output_dir)
