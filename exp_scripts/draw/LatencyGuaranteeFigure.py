import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/"

ranges = ("5000-15000", "6000-14000", "7000-13000", "8000-12000", "9000-11000")
achievedLatencyLimit = {
    "95\%": (750, 750, 750, 750, 750),
    "99\%": (1000, 1000, 1000, 750, 750),
    "99.9\%": (1000, 1000, 1000, 1000, 750),
    "99.99\%": (2000, 2000, 1250, 1250, 750),
    "100\%": (0, 0, 2000, 2000, 750),
}
# Different Range
print("Draw different range figure...")
figName = "different_range.png"
x = np.arange(len(ranges))
width = 1.0 / (len(ranges) + 1)
multiplier = 0

fig = plt.figure(figsize=(12, 6), layout="constrained")
for attribute, measurement in achievedLatencyLimit.items():
    offset = width * multiplier
    rects = plt.bar(x + offset, measurement, width, label=attribute)
    plt.bar_label(rects, padding=5)
    multiplier += 1
plt.xlabel("Arrival Rate Ranges")
plt.ylabel("Achieved Latency Limit")
plt.title("Latency guarantee achieved under different arrival range")
plt.legend(achievedLatencyLimit.keys(), loc='upper left')
axes = plt.gca()
axes.set_xticks(x + width, ranges)
axes.set_ylim(0, 2500)


import os
if not os.path.exists(outputDir):
    os.makedirs(outputDir)
plt.savefig(outputDir + figName)
plt.close(fig)

periods = ("30s", "48s", "60s", "80s", "120s")
achievedLatencyLimit = {
    "95\%": (2000, 1000, 1000, 750, 750),
    "99\%": (0, 1000, 1000, 750, 750),
    "99.9\%": (0, 2000, 1250, 1250, 1000),
    "99.99\%": (0, 2000, 1250, 1500, 1250),
    "100\%": (0, 2000, 0, 1500, 2000),
}
# Different Range
print("Draw different periods figure...")
figName = "different_periods.png"
x = np.arange(len(periods))
width = 1.0 / (len(periods) + 1)
multiplier = 0

fig = plt.figure(figsize=(12, 6), layout="constrained")
for attribute, measurement in achievedLatencyLimit.items():
    offset = width * multiplier
    rects = plt.bar(x + offset, measurement, width, label=attribute)
    plt.bar_label(rects, padding=5)
    multiplier += 1
plt.xlabel("Arrival Rate Periods")
plt.ylabel("Achieved Latency Limit")
plt.title("Latency guarantee achieved under different arrival periods")
plt.legend(achievedLatencyLimit.keys(), loc='upper left')
axes = plt.gca()
axes.set_xticks(x + width, periods)
axes.set_ylim(0, 2500)


import os
if not os.path.exists(outputDir):
    os.makedirs(outputDir)
plt.savefig(outputDir + figName)
plt.close(fig)