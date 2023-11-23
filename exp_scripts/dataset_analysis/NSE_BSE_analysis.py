import sys
import numpy as np
import matplotlib
import pandas as pd

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime

startTime = -1
NSE_dir = "/Users/swrrt/Workplace/Experiment/NSE_BSE/"
fileName = "log_inf.csv"
counter = 0
stockRecords = []
with open(NSE_dir + fileName) as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        counter += 1
        if counter > 1:
            split = line.rstrip().split(",")
            time = datetime.datetime.strptime(split[0], "%Y-%m-%d %H:%M:%S.%f")
            timestamp = int(time.timestamp()) * 1000.0 + time.microsecond / 1000.0
            stockRecords += [[timestamp, split[1]]]
            if startTime == -1 or startTime > timestamp:
                startTime = timestamp
        if counter % 10000 == 0:
            print(counter)
arrivedPerWindow = {}
distribution = {}
for record in stockRecords:
    index = int((record[0] - startTime)/1000.0)
    if index not in arrivedPerWindow:
        arrivedPerWindow[index] = 0
    arrivedPerWindow[index] += 1
    if record[1] not in distribution:
        distribution[record[1]] = 0
    distribution[record[1]] += 1

arrivalRate = [[], []]
for index in arrivedPerWindow.keys():
    arrivalRate[0] += [index]
    arrivalRate[1] += [arrivedPerWindow[index]]
fig = plt.figure(figsize=(48, 6))
plt.plot(arrivalRate[0], arrivalRate[1], '*', color='blue')
import os
if not os.path.exists(NSE_dir):
    os.makedirs(NSE_dir)
plt.savefig(NSE_dir + "arrivalRate.png")
plt.close(fig)

import pandas
df = pd.DataFrame({
    'Token': sorted(distribution.keys()),
    'Times': [distribution[x] for x in sorted(distribution.keys())]})
sorted_df = df.sort_values(by="Times", ascending=False)
fig = plt.figure(figsize=(48, 6))
p = plt.bar(sorted_df['Token'], sorted_df['Times'], 0.6, label="Average")
import os
if not os.path.exists(NSE_dir):
    os.makedirs(NSE_dir)
plt.savefig(NSE_dir + "distribution.png")
plt.close(fig)