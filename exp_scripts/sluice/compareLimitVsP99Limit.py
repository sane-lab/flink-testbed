import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

SMALL_SIZE = 25
MEDIUM_SIZE = 30
BIGGER_SIZE = 35

plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=SMALL_SIZE)    # font-size of the tick labels
plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title
MARKERSIZE=4


# Function to plot 'Our Latency Limit' vs 'P99 Latency Limit' and 'Success Rate'
def plot_latency_success_curve(outputDir, setting_name, data):
    # Extract values from input data
    our_latency_limit = [entry[0] for entry in data]
    p99_latency_limit = [entry[1] for entry in data]
    p95_latency_limit = [entry[2] for entry in data]
    success_rate = [entry[3] for entry in data]

    fig, ax1 = plt.subplots(figsize=(14, 6))

    # Plot 'P99 latency limit' on the primary y-axis
    ax1.set_xlabel('Our Latency Limit')
    ax1.set_ylabel('Latency Limit Difference', color='tab:blue')
    ax1.plot(our_latency_limit, [p99_latency_limit[x] - our_latency_limit[x] for x in range(0, len(our_latency_limit))], label='(P99 Latency Limit - Our Limit)', color='tab:blue', marker='d')
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    ax1.set_xticks(our_latency_limit)
    #ax1.plot(our_latency_limit, [p95_latency_limit[x] - our_latency_limit[x] for x in range(0, len(our_latency_limit))], label='(P95 Latency Limit - Our Limit)', color='tab:purple', marker='o')
    ax1.legend(loc='upper left')
    # Create another y-axis for 'success rate'
    # ax2 = ax1.twinx()
    # ax2.set_ylabel('Success Rate', color='tab:red')
    # ax2.plot(our_latency_limit, success_rate, label='Success Rate', color='tab:red', marker='x')
    # ax2.tick_params(axis='y', labelcolor='tab:red')
    # ax2.legend(loc='upper right')

    # Adding a title and showing the figure
    fig.tight_layout()
    plt.title("P99/P95 Latency Limit vs Our Latency Limit")

    import os
    if not os.path.exists(outputDir + setting_name):
        os.makedirs(outputDir + setting_name)
    plt.savefig(outputDir + setting_name + "/" + '_P99Limit_vs_ourlimit.png', bbox_inches='tight')
    plt.close(fig)


# Function to plot 'Setting Index' vs 'P99 Latency Limit', 'Our Limit', and 'Success Rate'
def plot_metrics_vs_settings(outputDir, setting_name, data):
    # Extract values from input data
    setting_index = list(range(1, len(data) + 1))
    our_latency_limit = [entry[0] for entry in data]
    p99_latency_limit = [entry[1] for entry in data]
    p95_latency_limit = [entry[2] for entry in data]
    success_rate = [entry[3] for entry in data]

    fig, ax1 = plt.subplots(figsize=(14, 6))

    # Plot 'P99 latency limit' and 'Our limit' on the primary y-axis
    ax1.set_xlabel('Setting Index')
    ax1.set_ylabel('Latency Limit', color='tab:blue')
    ax1.plot(our_latency_limit, p99_latency_limit, label='P99 Latency Limit', color='tab:blue', marker='o')
    #ax1.plot(our_latency_limit, p95_latency_limit, label='P95 Latency Limit', color='tab:purple', marker='s')
    #ax1.plot(our_latency_limit, our_latency_limit, label='Our Limit', color='tab:green', marker='s')
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    ax1.set_xticks(our_latency_limit)

    # Create another y-axis for 'success rate'
    ax2 = ax1.twinx()
    ax2.set_ylabel('Success Rate', color='tab:red')
    ax2.plot(our_latency_limit, success_rate, label='Success Rate', color='tab:red', marker='x')
    ax2.tick_params(axis='y', labelcolor='tab:red')

    # Add legends for both y-axes
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')

    # Adding a title and showing the figure
    fig.tight_layout()
    plt.title("P99/P95 Latency Limit, Our Limit, and Success Rate vs Setting Index")
    import os
    if not os.path.exists(outputDir + setting_name):
        os.makedirs(outputDir + setting_name)
    plt.savefig(outputDir + setting_name + "/" + '_Limit_vs_index.png', bbox_inches='tight')
    plt.close(fig)


outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/autotuner/"
# Example usage with input in the format [[our latency limit, p99 latency limit, p95 latency limit, success rate]]
data_under_settings = {
    "system-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-980-5500-3000-4000-30-1-0-1-50-1-5000-1-50-1-5000-1-50-1-5000-6-530-5000-500-3000-100-1-true-1":
        [
            [400, 841, 507, 0.9095282920196632],
            #[450, 769, 512, 0.9248826291079812],
            [500, 1005, 622, 0.9218717359515354],
            [750, 942, 690, 0.963013411567477],
            [1000, 1128, 815, 0.977928870292887],
            [1250, 1508, 1179, 0.9642707460184409],
            [1500, 1790, 1470, 0.9531642917015927],
            [1750, 2030, 1744, 0.9518274164834014],
            [2000, 2244, 1948, 0.9602427286043105],
        ],
    "system-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-980-5500-3000-4000-30-1-0-1-50-1-5000-1-50-1-5000-1-50-1-5000-6-530-5000-500-3000-100-1-true-1":
        [
            [400, 567, 375, 0.960603520536462],
            #[450, 551, 372, 0.9750576399077762],
            [500, 541, 371, 0.9847248378321825],
            [750, 875, 630, 0.9779966471081307],
            [1000, 1120, 835, 0.9798805407104684],
            [1250, 1469, 1058, 0.9731740542806245],
            [1500, 1768, 1424, 0.9653185247275775],
            [1750, 2018, 1598, 0.970233728120742],
            [2000, 2347, 2008, 0.9514776776357158],
        ],
    "system-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-980-5500-3000-4000-30-1-0-1-50-1-5000-1-50-1-5000-1-50-1-5000-6-530-5000-500-3000-100-1-true-1":
        [
            [500, 484, 347, 0.990780513357779],
            [750, 735, 536, 0.9907776147558164],
            [1000, 994, 696, 0.9908833700094309],
            [1250, 1368, 999, 0.9784134968039401],
            [1500, 1654, 1234, 0.979457079970653],
            [1750, 1921, 1594, 0.9677351770375027],
            [2000, 2162, 1821, 0.9730664430936911],
        ],
    "system-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-980-6000-3000-4000-30-1-0-1-50-1-5000-1-50-1-5000-1-50-1-5000-6-530-5000-500-3000-100-1-true-1":
        [
            [400, 1103, 664, 0.878395319682407],
            [450, 1193, 647, 0.8991017338625444],
            [500, 1140, 735, 0.8999163354946664],
            [750, 1521, 995, 0.9329638151014432],
            [1000, 1266, 919, 0.9650591066011089],
            [1250, 1565, 1218, 0.9553393996443886],
            [1500, 1855, 1483, 0.9519924694069658],
            [1750, 2112, 1749, 0.9510562643798368],
            [2000, 2349, 1974, 0.9533911589507785],
        ],
    "system-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-980-6000-3000-4000-30-1-0-1-50-1-5000-1-50-1-5000-1-50-1-5000-6-530-5000-500-3000-100-1-true-1":
        [
            [500, 601, 405, 0.9771942671827597],
            [750, 940, 649, 0.9724463069669984],
            [1000, 1191, 884, 0.9752672395724167],
            [1250, 1608, 1229, 0.9557837384744342],
            [1500, 1799, 1425, 0.963320058687906],
            [1750, 2135, 1677, 0.9612159329140462],
            [2000, 2333, 1942, 0.9611233364769989],
        ],
    "system-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-980-6500-3000-4000-30-1-0-1-50-1-5000-1-50-1-5000-1-50-1-5000-6-530-5000-500-3000-100-1-true-1":
        [
            [500, 2021, 1235, 0.870596843315564],
            [750, 2109, 1477, 0.898714330511132],
            [1000, 1797, 1377, 0.9016668413879861],
            [1250, 1751, 1453, 0.8999266478046736],
            [1500, 2066, 1610, 0.9270746018440905],
            [1750, 2296, 1799, 0.9417718877178337],
            [2000, 2600, 2122, 0.935014650481373],
        ],
    "system-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-980-6500-3000-4000-30-1-0-1-50-1-5000-1-50-1-5000-1-50-1-5000-6-530-5000-500-3000-100-1-true-1":
        [
            [500, 657, 421, 0.9730012557555463],
            [750, 1054, 695, 0.9630056591909453],
            [1000, 1306, 961, 0.9016668413879861],
            [1250, 1664, 1277, 0.9477158424140821],
            [1500, 2073, 1655, 0.920020964360587],
            [1750, 2222, 1838, 0.9344330757987054],
            [2000, 2647, 2228, 0.9055027670460478],
        ],

}

for setting_name, data in data_under_settings.items():
    # First plot: Our Latency Limit vs P99 Latency Limit and Success Rate
    plot_latency_success_curve(outputDir, setting_name, data)
    # Second plot: Setting Index vs P99 Latency Limit, Our Limit, and Success Rate
    plot_metrics_vs_settings(outputDir, setting_name, data)