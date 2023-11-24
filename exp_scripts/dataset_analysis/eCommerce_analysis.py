import sys
import numpy as np
import matplotlib
import pandas as pd

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime

startTime = -1
dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/eCommerce/"
fileName = "2019-OCT.csv"
counter = 0
eventRecords = []
with open(dir + fileName) as f:
    #lines = f.readlines()
    for line in f:
        counter += 1
        if counter > 1:
            split = line.rstrip().split(",")
            time = datetime.datetime.strptime(split[0].rstrip(" UTC"), "%Y-%m-%d %H:%M:%S")
            timestamp = int(time.timestamp()) * 1000.0
            eventRecords += [[timestamp, split[2]]]
            if startTime == -1 or startTime > timestamp:
                startTime = timestamp
            if timestamp - startTime > 1 * 24 * 60 * 60 * 1000:
                break
        if counter % 10000 == 0:
            print(counter)

arrivedPerWindow = {}
distribution = {}
for record in eventRecords:
    index = int((record[0] - startTime)/10000.0)
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
fig = plt.figure(figsize=(12, 8))
plt.plot(arrivalRate[0], arrivalRate[1], '*', color='blue', markersize=1)
import os
if not os.path.exists(dir):
    os.makedirs(dir)
plt.savefig(dir + "arrivalRate.png", bbox_inches='tight')
plt.close(fig)

# import pandas
# df = pd.DataFrame({
#     'ID': sorted(distribution.keys()),
#     'Times': [distribution[x] for x in sorted(distribution.keys())]})
# sorted_df = df.sort_values(by="Times", ascending=False)
# fig = plt.figure(figsize=(16, 9))
# p = plt.bar(sorted_df['ID'], sorted_df['Times'], 0.6, label="Average")
# import os
# if not os.path.exists(dir):
#     os.makedirs(dir)
# plt.savefig(dir + "distribution.png")
# plt.close(fig)