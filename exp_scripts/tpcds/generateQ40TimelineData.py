import os


tpcds_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/tpcds/"
catalog_sales = "catalog_sales_1_2.dat"
catalog_returns = "catalog_returns_1_2.dat"
date_dim = "date_dim_1_2.dat"
time_dim = "time_dim_1_2.dat"

date_map = {}
time_map = {}

dateFile = tpcds_dir + date_dim
with open(dateFile) as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split('|')
        date_sk = split[0]
        date = split[2]
        import datetime
        import time

        d = datetime.datetime.strptime(date, "%Y-%m-%d").timestamp()
        print(d)
        date_map[date_sk] = d

timeFile = tpcds_dir + time_dim
with open(timeFile) as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split('|')
        time_sk = split[0]
        time = split[2]
        time_map[time_sk] = int(time)

saleFile = tpcds_dir + catalog_sales
newSalesFile = tpcds_dir + "catalog_sales_new.dat"
salesPerTime = {}
with open(saleFile) as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split('|')
        date_sk = split[0]
        time_sk = split[1]
        if date_sk != '' and time_sk != '':
            dt = date_map[date_sk] * 86400 + time_map[time_sk]
            if dt not in salesPerTime:
                salesPerTime[dt] = []
            salesPerTime[dt].append(line)
f = open(newSalesFile, "w")
for dt in sorted(salesPerTime.keys()):
    for line in salesPerTime[dt]:
        f.write(line)
f.close()

returnFile = tpcds_dir + catalog_returns
newReturnsFile = tpcds_dir + "catalog_returns_new.dat"
returnsPerTime = {}
with open(returnFile) as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split('|')
        date_sk = split[0]
        time_sk = split[1]
        if date_sk != '' and time_sk != '':
            dt = date_map[date_sk] * 86400 + time_map[time_sk]
            if dt not in returnsPerTime:
                returnsPerTime[dt] = []
            returnsPerTime[dt].append(line)
f = open(newReturnsFile, "w")
for dt in sorted(returnsPerTime.keys()):
    for line in returnsPerTime[dt]:
        f.write(line)
f.close()

import math
import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

x1 = sorted(salesPerTime.keys())
y1 = [len(salesPerTime[x]) for x in x1]
x2 = sorted(returnsPerTime.keys())
y2 = [len(returnsPerTime[x]) for x in x2]
fig = plt.figure(figsize=(12, 6))
legend = ["catalog sales #", "catalog returns #"]
plt.plot(x1, y1, 's', color='b', markersize=1)
plt.plot(x2, y2, 'o', color='g', markersize=1)
axes = plt.gca()
axes.set_xlim(x1[0], x1[0] + 86400)
axes.set_xticks(np.arange(x1[0], x1[0] + 86400 + 3600, 3600))
axes.set_xticklabels([int((x - x1[0])/3600) for x in np.arange(x1[0], x1[0] + 86400 + 3600, 3600)])
plt.legend(legend)
plt.grid(True)
plt.savefig(tpcds_dir + 'catalog.png', bbox_inches='tight')
plt.close(fig)