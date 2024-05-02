import sys
import numpy as np
import matplotlib
from fontTools.misc.py23 import xrange

matplotlib.use('Agg')
import matplotlib.pyplot as plt

workload_index = {
    0: "Stock",
    1: "Twitter",
    2: "Linear_Road",
    3: "Spam_Detection",
}
workload_limit = {
    "Stock": 2000,
    "Twitter": 4000,
    "Linear_Road": 4000,
}
controller_index = {
    0: "DS2",
    1: "StreamSwitch",
    2: "Sluice",
}
controller_color = {
    0: "#fdae61",
    1: "#abdda4",
    2: "#2b83ba",
}

output_directory = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/new/"

workload_avg_latency = {}
workload_success_rate = {}
workload_avg_parallelism = {}
with open("overall_results.txt") as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split()
        if len(split) > 2:
            if split[0].startswith("stock"):
                workload = 0
            elif split[0].startswith("tweet"):
                workload = 1
            elif split[0].startswith("linear-road"):
                workload = 2
            elif split[0].startswith("spam-detection"):
                workload = 3
            if "ds2" in split[0]:
                controller = 0
            elif "streamswitch" in split[0]:
                controller = 1
            elif "sluice" in split[0]:
                controller = 2

            average_latency = float(split[1])
            success_rate = float(split[2])
            parallelism = float(split[3])
            if workload not in workload_avg_latency:
                workload_avg_latency[workload] = {}
                workload_success_rate[workload] = {}
                workload_avg_parallelism[workload] = {}
            if controller not in workload_avg_latency[workload]:
                workload_avg_latency[workload][controller] = []
                workload_success_rate[workload][controller] = []
                workload_avg_parallelism[workload][controller] = []

            workload_avg_latency[workload][controller] += [average_latency]
            workload_success_rate[workload][controller] += [success_rate]
            workload_avg_parallelism[workload][controller] += [parallelism]

# Plot average latency
print("Draw average latency")
data_controller = [[], [], []]
workload_labels = []
for workload in sorted(workload_avg_latency.keys()):
    for controller in workload_avg_latency[workload].keys():
        data_controller[controller].append(workload_avg_latency[workload][controller])
    workload_labels.append(workload_index[workload])

print(data_controller)
print(workload_labels)
def set_box_color(bp, color):
    plt.setp(bp['boxes'], color=color)
    plt.setp(bp['whiskers'], color=color)
    plt.setp(bp['caps'], color=color)
    plt.setp(bp['medians'], color=color)

fig, axs = plt.subplots(1, 1, figsize=(8, 4), layout='constrained') #(24, 9)
figName = "Overall_average_latency.jpg"
for controller in range(0, 3):
    bp = plt.boxplot(data_controller[controller], positions=np.array(range(len(data_controller[controller]))) * 3.0 - 0.4 + controller * 0.4, sym='', widths=0.6)
    set_box_color(bp, controller_color[controller])
    plt.plot([], c=controller_color[controller], label=controller_index[controller])
plt.legend()
plt.xticks(range(0, len(workload_labels) * 3, 3), workload_labels)
plt.xlim(-3, len(workload_labels)*3)
plt.ylim(0, 5000)
plt.tight_layout()
plt.savefig(output_directory+figName)

