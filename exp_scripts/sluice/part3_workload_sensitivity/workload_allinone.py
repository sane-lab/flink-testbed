import math
import sys
import numpy as np
import matplotlib
from matplotlib import cm

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
        fig.savefig(os.path.join(output_dir, 'success_rate_bar_' + str(workload_name) + '.pdf'), bbox_inches='tight')
    else:
        ax.set_title('Success Rates by ' + clean_string(dimension))
        fig.savefig(os.path.join(output_dir, 'success_rate_bar_' + str(workload_name) + '.png'), bbox_inches='tight')
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
        fig.savefig(output_dir + 'avg_latency_' + str(workload_name) + '.pdf', bbox_inches='tight')
    else:
        ax.set_title('Average End-to-End Latency by ' + clean_string(dimension))
        fig.savefig(output_dir + 'avg_latency_' + str(workload_name) + '.png', bbox_inches='tight')
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
    ax.set_xlabel(dimension)
    ax.set_ylabel('Avg # of Slots')
    ax.yaxis.set_label_coords(-0.075, 0.4)
    #ax.legend()
    ax.grid(True, axis='y')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if output_pdf_flag:
        fig.savefig(output_dir + 'avg_parallelism_curve_' + str(workload_name) + '.pdf', bbox_inches='tight')
    else:
        fig.title('Avg Resources by ' + clean_string(dimension))
        fig.savefig(output_dir + 'avg_parallelism_curve_' + str(workload_name) + '.png', bbox_inches='tight')
    plt.close(fig)

def plot_all_in_one(axs, legend_info, title, success_rates:dict[str:object], latency:dict[str:list[object]], parallelism:dict[str:list[object]], output_dir:str, workload_name:str, dimension:str, y1label_flag, y2label_flag):
    # Extract labels
    x_labels = list(success_rates.keys())

    # Use Pastel1 colormap
    cmap = matplotlib.colormaps["Paired"] # cm.get_cmap('Paired', len(labels) * 2)
    colors = [cmap(i) for i in range(len(x_labels) * 2)]

    # Create the figure and subplots
    #fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 6), gridspec_kw={'height_ratios': [1, 2]})
    ax1 = axs[0]
    ax2 = axs[1]

    # 1. Top subplot: Success rate curve
    curve_plot, = ax1.plot(np.arange(0, len(x_labels)), list(success_rates.values()), marker='o', color='blue', label='Success Rate (%)', markersize=10)
    # ax1.set_xlim(-0.75, len(x_labels) - 0.25)
    # ax1.set_xticks(np.arange(0, len(x_labels)))
    # ax1.set_xticklabels(x_labels)

    min_rate = min(success_rates.values())
    if min_rate >= 0.95:
        ax1.set_ylim(0.95, 1.0)
        ax1.set_yticks(np.arange(0.96, 1.01, 0.02))
        ax1.set_yticklabels([math.ceil(x * 100) for x in np.arange(0.96, 1.01, 0.02)])
    elif min_rate >= 0.9:
        ax1.set_ylim(0.9, 1.005)
        ax1.set_yticks(np.arange(0.9, 1.01, 0.05))
        ax1.set_yticklabels([math.ceil(x * 100) for x in np.arange(0.9, 1.01, 0.05)])
    elif min_rate >= 0.8:
        ax1.set_ylim(0.8, 1.0)
        ax1.set_yticks(np.arange(0.80, 1.01, 0.1))
        ax1.set_yticklabels([math.ceil(x * 100) for x in np.arange(0.80, 1.01, 0.1)])
    else:
        ax1.set_ylim(0.0, 1.0)
        ax1.set_yticks(np.arange(0.0, 1.001, 0.5))
        ax1.set_yticklabels([math.ceil(x * 100) for x in np.arange(0.0, 1.001, 0.5)])
    ax1.grid(True)
    ax1.set_xticks(np.arange(0, len(x_labels)))
    ax1.tick_params(axis='x', bottom=False)
    ax1.set_xticklabels([])
    if y1label_flag:
        ax1.set_ylabel("Success\nRate (%)", fontsize=30)
        ax1.tick_params(axis='y')
    # else:
    #     ax1.tick_params(axis='y', left=False)

    handles = [curve_plot]
    labels = ['Success Rate (%)']


    # ax1.set_title("Success Rate Curve", fontsize=14)
    # ax1.legend(loc="upper right")


    if boxplot_flag:
        # Prepare data for boxplots
        latency_stats = [
            {
                "q1": v[2], "q3": v[4], "whislo": max(v[2] - 1.5 * (v[4] - v[2]), v[1]),
                "whishi": min(v[4] + 1.5 * (v[4] - v[2]), v[5]),
                "med": v[3], "mean": v[0], "label": k, "fliers": []  # Add fliers key
            }
            for k, v in latency.items()
        ]

        parallelism_stats = [
            {
                "q1": v[2], "q3": v[4], "whislo": max(v[2] - 1.5 * (v[4] - v[2]), v[1]),
                "whishi": min(v[4] + 1.5 * (v[4] - v[2]), v[5]),
                "med": v[3], "mean": v[0], "label": k, "fliers": []  # Add fliers key
            }
            for k, v in parallelism.items()
        ]

        # 2. Bottom subplot: Latency and parallelism boxplots
        box_positions = range(len(x_labels))  # Positions for boxplots

        # Plot latency boxplots with colormap
        for i, pos in enumerate(box_positions):
            ax2.bxp([latency_stats[i]], positions=[pos - 0.2], widths=0.3,
                    showmeans=True, meanline=True, patch_artist=True,
                    boxprops=dict(facecolor=colors[i * 2], alpha=0.5), meanprops=dict(color='red'))
            #ax2.text(pos - 0.2, latency_stats[i]['whishi'] + 10, 'Latency', ha='center', fontsize=10, color="black")

        # Second y-axis for parallelism
        ax3 = ax2.twinx()

        # Plot parallelism boxplots with colormap
        for i, pos in enumerate(box_positions):
            ax3.bxp([parallelism_stats[i]], positions=[pos + 0.2], widths=0.3,
                    showmeans=True, meanline=True, patch_artist=True,
                    boxprops=dict(facecolor=colors[i * 2 + 1], alpha=0.5), meanprops=dict(color='purple'))
            # ax3.text(pos + 0.2, parallelism_stats[i]['whishi'] + 1, 'Parallelism', ha='center', fontsize=10,
            #         color="black")

        ax2.set_xticks(box_positions)
        ax2.set_xticklabels(x_labels)
        # Set labels and legends
        ax2.set_ylabel("Latency\n(ms)", fontsize=18)
        ax3.set_ylabel("# of Slots", fontsize=18)
    else:
        latency_mean = [v[0] for k, v in latency.items()]
        parallelism_mean = [v[0] for k, v in parallelism.items()]
        keys = [k for k, v in parallelism.items()]
        x = np.arange(len(keys))

        width = 0.35 #0.4

        bar1 = ax2.bar(x - width / 2, latency_mean, width, label='Average Latency', color='lightblue', edgecolor='black', hatch='/')

        # Create the secondary y-axis
        ax3 = ax2.twinx()

        bar2 = ax3.bar(x + width / 2, parallelism_mean, width, label='Average Resources', color='lightgreen', edgecolor='black', hatch='\\')

        # Adjust the y-axis limits for ax2 (Primary y-axis for latency)
        ylim_upper_ax2 = max(latency_mean) * 1.3  # Increase the upper limit by 20% for better spacing
        ax2.set_ylim(0, ylim_upper_ax2)

        # Adjust the y-axis limits for ax3 (Secondary y-axis for parallelism)
        ylim_upper_ax3 = max(parallelism_mean) * 1.3  # Increase the upper limit by 20%

        for bar in bar1:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.0f}', ha='center', va='bottom',
                    fontsize=25, rotation=90)
        for bar in bar2:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.1f}', ha='center', va='bottom',
                     fontsize=25, rotation=90)

        if y1label_flag:
            ax2.set_ylabel("Latency\n(ms)", fontsize=30)
        #else:
        #    ax2.set_yticklabels([])

        #ax2.set_yticklabels(ax2.get_yticks(), rotation=90)
        ax3.set_ylim(0, 65)
        if y2label_flag:
            ax3.set_ylabel("# of Slots", fontsize=30)
        else:
            ax3.tick_params(axis='y', left=False)
            ax3.set_yticklabels([])


        ax2.set_xticks(x)
        ax2.set_xticklabels(keys)

        handles.append(bar1)
        labels.append('Average Latency')
        handles.append(bar2)
        labels.append('Average Resources')
        ax2.set_title(title, y=-0.26, fontsize=35)

    # fig.legend(handles=handles, labels=labels, bbox_to_anchor=(0.5, 1.08), loc='upper center', ncol=2)

    legend_info[0] = handles
    legend_info[1] = labels

    # if not os.path.exists(output_dir):
    #     os.makedirs(output_dir)
    # if output_pdf_flag:
    #     plt.savefig(output_dir + 'successrate_latency_parallelism_' + str(workload_name) + '.pdf', bbox_inches='tight')
    # else:
    #     plt.title('Avg Resources by ' + clean_string(dimension))
    #     plt.savefig(output_dir + 'successrate_latency_parallelism_' + str(workload_name) + '.png', bbox_inches='tight')
    # plt.close(fig)

output_pdf_flag=True
boxplot_flag=False # False for barchart of mean

title_list = [
    "(a) Pattern",
    "(b) Amplitude (%)",
    "(c) Period (s)",
    "(d) Topology",
    "(e) Service Rate",
    "(f) State Size (MB)",
    "(g) Skewness",
]

def main():
    success_rate_per_label = {}
    name_list = [
        #"tweet",
        #"lr",
        #"stock",
        "micro",
    ]
    for name in name_list:
        overall_output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/part3/" + name + "/"

        #fig_all, axs_all = plt.subplots(2, 4, figsize=(28, 10), layout='constrained', gridspec_kw={'height_ratios': [1, 2], 'width_ratios': [1, 1, 1, 1]})
        # fig_all, axs_all = plt.subplots(2, 4, figsize=(28, 7.5), layout='constrained',
        #                                 gridspec_kw={'height_ratios': [3, 5], 'width_ratios': [1, 1, 1, 1]})
        fig_all, axs_all = plt.subplots(2, 4, figsize=(28, 6.5), layout='constrained',
                                        gridspec_kw={'height_ratios': [1.5, 4.5], 'width_ratios': [1, 1, 1, 1]})
        legend_info = [[], []]
        index = 0

        with open("workload_results_" + name + ".txt", "r") as file:
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
                success_rate_per_x = {}
                latency_per_x = {}
                parallelism_per_x = {}
            elif len(splits) == 1 and splits[0] == "end":
                print(success_rate_per_label)
                print(avg_latency_per_label)
                print(avg_parallelism_per_label)
                plot_success_rate_bar(x_per_label, success_rate_per_label, overall_output_dir, workload_name, dimension)
                plot_avg_latency(x_per_label, avg_latency_per_label, overall_output_dir, workload_name, dimension)
                plot_avg_parallelism_bar(x_per_label, avg_parallelism_per_label, overall_output_dir, workload_name, dimension)
                if index == 0 or index == 4:
                    y1label_flag = True
                else:
                    y1label_flag = False
                if index == 3 or index == 6:
                    y2label_flag = True
                else:
                    y2label_flag = False
                plot_all_in_one([axs_all[0][index % 4], axs_all[1][index % 4]], legend_info, title_list[index], success_rate_per_x, latency_per_x, parallelism_per_x, overall_output_dir, workload_name, dimension, y1label_flag, y2label_flag)
                index += 1
                if index == 4:
                    fig_all.legend(legend_info[0], legend_info[1], fontsize=30, loc='upper center',
                                   bbox_to_anchor=(0.51, 1.15), ncol=7, markerscale=1)

                    if not os.path.exists(overall_output_dir):
                        os.makedirs(overall_output_dir)
                    if output_pdf_flag:
                        fig_all.savefig(overall_output_dir + 'part3_all_in_one_' + str(name) + '_1.pdf',
                                        bbox_inches='tight')
                    else:
                        fig_all.title('Avg Resources by ' + clean_string(dimension))
                        fig_all.savefig(overall_output_dir + 'part3_all_in_one_' + str(name) + '_1.png',
                                        bbox_inches='tight')
                    plt.close(fig_all)
                    #fig_all, axs_all = plt.subplots(2, 3, figsize=(21, 10), layout='constrained',
                    #                                gridspec_kw={'height_ratios': [1, 2], 'width_ratios': [1, 1, 1]})
                    # fig_all, axs_all = plt.subplots(2, 3, figsize=(21, 7.5), layout='constrained',
                    #                                 gridspec_kw={'height_ratios': [3, 5], 'width_ratios': [1, 1, 1]})
                    fig_all, axs_all = plt.subplots(2, 3, figsize=(21, 6.5), layout='constrained',
                                                    gridspec_kw={'height_ratios': [1.5, 4.5],
                                                                 'width_ratios': [1, 1, 1]})

            elif len(splits) > 1:
                label = splits[3]
                x = splits[2]

                if (dimension == "User Limit(ms)"):
                    label = ""

                success_rate = float(splits[4])
                avg_latency = float(splits[5])
                min_latency = float(splits[6])
                q1_latency = float(splits[7])
                med_latency = float(splits[8])
                q3_latency = float(splits[9])
                max_latency = float(splits[10])
                avg_parallelism = float(splits[11])
                min_parallelism = float(splits[12])
                q1_parallelism = float(splits[13])
                med_parallelism = float(splits[14])
                q3_parallelism = float(splits[15])
                max_parallelism = float(splits[16])

                if(label not in success_rate_per_label):
                    success_rate_per_label[label] = []
                    avg_parallelism_per_label[label] = []
                    x_per_label[label] = []
                    avg_latency_per_label[label] = []
                x_per_label[label].append(x)
                success_rate_per_label[label].append(success_rate)
                avg_latency_per_label[label].append(avg_latency)
                avg_parallelism_per_label[label].append(avg_parallelism)
                success_rate_per_x[x] = success_rate
                latency_per_x[x] = [avg_latency, min_latency, q1_latency, med_latency, q3_latency, max_latency]
                parallelism_per_x[x] = [avg_parallelism, min_parallelism, q1_parallelism, med_parallelism, q3_parallelism, max_parallelism]
                print(label)

        fig_all.legend([], [], fontsize=30, loc='upper center',
                       bbox_to_anchor=(0.51, 1.1), ncol=7, markerscale=1, frameon=False)

        if not os.path.exists(overall_output_dir):
            os.makedirs(overall_output_dir)
        if output_pdf_flag:
            fig_all.savefig(overall_output_dir + 'part3_all_in_one_' + str(name) + '_2.pdf', bbox_inches='tight')
        else:
            fig_all.title('Avg Resources by ' + clean_string(dimension))
            fig_all.savefig(overall_output_dir + 'part3_all_in_one_' + str(name) + '_2.png', bbox_inches='tight')
        plt.close(fig_all)


if __name__ == "__main__":
    main()
