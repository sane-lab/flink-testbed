import sys
import numpy as np
import matplotlib
import pandas as pd

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime

ETH_flag = True

startTime = -1
data_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/BTCUSDT_ETHUSDT/"
fileName = "BTCUSDT.csv"
name="BTCUSDT"
counter = 0
transRecords = []
with open(data_dir + fileName) as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        counter += 1
        if counter > 1:
            split = line.rstrip().split(",")
            timestamp = int(split[0])
            transRecords += [[timestamp, split[1], float(split[2]), float(split[3]), split[4]]]
            if startTime == -1 or startTime > timestamp:
                startTime = timestamp
            if timestamp - startTime > 1 * 24 * 60 * 60 * 1000:
                break
        if counter % 10000 == 0:
            print(counter)

if(ETH_flag):
    fileName = "ETHUSDT.csv"
    name = "BTC&ETH"
    counter = 0
    with open(data_dir + fileName) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            counter += 1
            if counter > 1:
                split = line.rstrip().split(",")
                timestamp = int(split[0])
                transRecords += [[timestamp, split[1], float(split[2]), float(split[3]), split[4]]]
                if startTime == -1 or startTime > timestamp:
                    startTime = timestamp
                if timestamp - startTime > 1 * 24 * 60 * 60 * 1000:
                    break
            if counter % 10000 == 0:
                print(counter)



arrivedPerWindow = {}
prices = []
quantities = []
for record in transRecords:
    index = int((record[0] - startTime)/10000.0)
    if index not in arrivedPerWindow:
        arrivedPerWindow[index] = 0
    arrivedPerWindow[index] += 1
    prices += [record[2]]
    quantities += [record[3]]
arrivalRate = [[], []]
for index in arrivedPerWindow.keys():
    arrivalRate[0] += [index]
    arrivalRate[1] += [arrivedPerWindow[index]]
fig = plt.figure(figsize=(12, 6))
plt.plot(arrivalRate[0], arrivalRate[1], '*', color='blue', markersize=1)
plt.xlim([0, 8640])
plt.ylim([0, 2500])
#plt.ylim([0, 1600])
axes = plt.gca()
axes.set_yticks(np.arange(0, 2750, 250))
#axes.set_yticks(np.arange(0, 1800, 200))
import os
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
plt.savefig(data_dir + name + "_arrivalRate.png", bbox_inches='tight')
plt.close(fig)

#counts, bins = np.histogram(prices)
fig = plt.figure(figsize=(12, 8))
#plt.stairs(counts, bins)
plt.hist(prices, bins=100)
import os
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
plt.savefig(data_dir + name + "_prices_distribution.png", bbox_inches='tight')
plt.close(fig)

#counts, bins = np.histogram(quantities)
fig = plt.figure(figsize=(16, 9))
#plt.stairs(counts, bins)
plt.hist(quantities, bins=100)
import os
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
plt.savefig(data_dir + name + "_quantities_distribution.png")
plt.close(fig)