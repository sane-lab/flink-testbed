import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/"
inputLog = "../tune_log"
inputResult = "../tune_result"

host = "samza@camel-sane.d2.comp.nus.edu.sg"
targetFiles = [
    "~/workspace/flink-related/flink-testbed-sane/streamsluice_scripts/scripts/tune_result",
    "~/workspace/flink-related/flink-testbed-sane/streamsluice_scripts/scripts/tune_log"
]

import subprocess
for targetFile in targetFiles:
    subprocess.run(["scp", host + ":" + targetFile, "../"])

periods = ["60s", "30s", "20s", "15s"]

# Read result
latencyName = []
latencyResult = []
with open(inputResult) as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split()
        latencyName += [split[0]]
        latencyResult += [int(split[1])]
figName = "BestLatencyLimits.png"
fig = plt.figure(figsize=(10, 7))
plt.plot(periods, latencyResult, "*", markersize=6)
plt.title("Best latency guarante can achieved.")
plt.ylabel("Latency limit")
plt.xlabel("Periods")
import os
if not os.path.exists(outputDir):
    os.makedirs(outputDir)
plt.savefig(outputDir + figName)
plt.close(fig)

# Read tuner log
successRateCurvePerWorkload = []
with open(inputLog) as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split()
        if split[0] == "Auto" and split[2] == "round":
            if(split[3].rstrip(":") == "9"):
                successRateCurve = {}
                successRateCurvePerWorkload += [successRateCurve]
        if split[-1] == "false" and len(split) > 2:
            L = int(split[0])
            spike = int(split[1])
            successRate = float(split[2])
            if L not in successRateCurve or successRateCurve[L] < successRate:
                successRateCurve[L] = successRate
        if split[-1] == "true":
            L = int(split[0])
            successRate = 0.99
            if L not in successRateCurve or successRateCurve[L] < successRate:
                successRateCurve[L] = successRate

figName = "tuningSuccessRateCurve.png"
fig = plt.figure(figsize=(10, 7))
legend = []
for i in range(0, len(successRateCurvePerWorkload)):
    limits = sorted(successRateCurvePerWorkload[i].keys())
    successRate = [successRateCurvePerWorkload[i][x] for x in limits]
    plt.plot(limits, successRate, "*-", markersize=6)
    legend += [periods[i]]
plt.title("Success rate under different latency limits")
plt.legend(legend)
plt.ylabel("Success Rate")
plt.xlabel("Latency Limit")
axes = plt.gca()
#axes.set_yscale('log')
#plt.yscale('log')
#axes.invert_yaxis()
import os
if not os.path.exists(outputDir):
    os.makedirs(outputDir)
plt.savefig(outputDir + figName)
plt.close(fig)





