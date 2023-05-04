from math import floor
import os

from analysis.config.default_config import timers_plot, per_task_rate, parallelism, per_key_state_size, \
    replicate_keys_filter, state_access_ratio, max_parallelism, FILE_FOLER, order_function, zipf_skew, sync_keys
from analysis.config.general_utilities import DrawFigureV4, breakdown_total


def ReadFile(repeat_num = 1):
    w, h = 3, 4
    y = [[] for y in range(h)]

    parallelism = 8
    max_parallelism = 512
    per_key_state_size = 16384
    replicate_keys_filter = 0
    sync_keys = 0
    per_task_rate = 5000
    zipf_skew = 1
    state_access_ratio = 2
    configScenario = "dynamic"

    col = []
    coly = []
    start_ts = float('inf')
    temp_dict = {}
    for tid in range(0, 8):
        f = open(FILE_FOLER + "/spector-{}-{}-{}-{}-{}-{}-{}-{}-{}/Splitter FlatMap-{}.output"
                 .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, sync_keys,
                         replicate_keys_filter, state_access_ratio, zipf_skew, configScenario, tid))
        read = f.readlines()
        for r in read:
            if r.find("endToEnd latency: ") != -1:
                ts = int(int(r.split("ts: ")[1][:13])/1000)
                if ts < start_ts: # find the starting point from parallel tasks
                    start_ts = ts
                latency = int(r.split("endToEnd latency: ")[1])
                if ts not in temp_dict:
                    temp_dict[ts] = []
                temp_dict[ts].append(latency)

    for ts in temp_dict:
        # coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
        temp_dict[ts].sort()
        coly.append(temp_dict[ts][floor((len(temp_dict[ts]))*0.99)])
        col.append(ts - start_ts)

    # Get P95 latency
    phase1 = coly[0:20]
    phase1.sort()

    phase2 = coly[20:60]
    phase2.sort()

    phase3 = coly[60:100]
    phase3.sort()

    y[0].append(phase1[-1])
    y[0].append(phase2[-1])
    y[0].append(phase3[-1])


    configScenario = "static"
    replicate_keys_filter = 0
    sync_keys = 0

    col = []
    coly = []
    start_ts = float('inf')
    temp_dict = {}
    for tid in range(0, 8):
        f = open(FILE_FOLER + "/spector-{}-{}-{}-{}-{}-{}-{}-{}-{}/Splitter FlatMap-{}.output"
                 .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, sync_keys,
                         replicate_keys_filter, state_access_ratio, zipf_skew, configScenario, tid))
        read = f.readlines()
        for r in read:
            if r.find("endToEnd latency: ") != -1:
                ts = int(int(r.split("ts: ")[1][:13]) / 1000)
                if ts < start_ts:  # find the starting point from parallel tasks
                    start_ts = ts
                latency = int(r.split("endToEnd latency: ")[1])
                if ts not in temp_dict:
                    temp_dict[ts] = []
                temp_dict[ts].append(latency)

    for ts in temp_dict:
        # coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
        temp_dict[ts].sort()
        coly.append(temp_dict[ts][floor((len(temp_dict[ts])) * 0.99)])
        col.append(ts - start_ts)

    # Get P95 latency
    phase1 = coly[0:20]
    phase1.sort()

    phase2 = coly[20:60]
    phase2.sort()

    phase3 = coly[60:100]
    phase3.sort()

    y[1].append(phase1[-1])
    y[1].append(phase2[-1])
    y[1].append(phase3[-1])

    configScenario = "static"
    replicate_keys_filter = 0
    sync_keys = 8

    col = []
    coly = []
    start_ts = float('inf')
    temp_dict = {}
    for tid in range(0, 8):
        f = open(FILE_FOLER + "/spector-{}-{}-{}-{}-{}-{}-{}-{}-{}/Splitter FlatMap-{}.output"
                 .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, sync_keys,
                         replicate_keys_filter, state_access_ratio, zipf_skew, configScenario, tid))
        read = f.readlines()
        for r in read:
            if r.find("endToEnd latency: ") != -1:
                ts = int(int(r.split("ts: ")[1][:13]) / 1000)
                if ts < start_ts:  # find the starting point from parallel tasks
                    start_ts = ts
                latency = int(r.split("endToEnd latency: ")[1])
                if ts not in temp_dict:
                    temp_dict[ts] = []
                temp_dict[ts].append(latency)

    for ts in temp_dict:
        # coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
        temp_dict[ts].sort()
        coly.append(temp_dict[ts][floor((len(temp_dict[ts])) * 0.99)])
        col.append(ts - start_ts)

    # Get P95 latency
    phase1 = coly[0:20]
    phase1.sort()

    phase2 = coly[20:60]
    phase2.sort()

    phase3 = coly[60:100]
    phase3.sort()

    y[2].append(phase1[-1])
    y[2].append(phase2[-1])
    y[2].append(phase3[-1])

    configScenario = "static"
    replicate_keys_filter = 1
    sync_keys = 0

    col = []
    coly = []
    start_ts = float('inf')
    temp_dict = {}
    for tid in range(0, 8):
        f = open(FILE_FOLER + "/spector-{}-{}-{}-{}-{}-{}-{}-{}-{}/Splitter FlatMap-{}.output"
                 .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, sync_keys,
                         replicate_keys_filter, state_access_ratio, zipf_skew, configScenario, tid))
        read = f.readlines()
        for r in read:
            if r.find("endToEnd latency: ") != -1:
                ts = int(int(r.split("ts: ")[1][:13]) / 1000)
                if ts < start_ts:  # find the starting point from parallel tasks
                    start_ts = ts
                latency = int(r.split("endToEnd latency: ")[1])
                if ts not in temp_dict:
                    temp_dict[ts] = []
                temp_dict[ts].append(latency)

    for ts in temp_dict:
        # coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
        temp_dict[ts].sort()
        coly.append(temp_dict[ts][floor((len(temp_dict[ts])) * 0.99)])
        col.append(ts - start_ts)

    # Get P95 latency
    phase1 = coly[0:20]
    phase1.sort()

    phase2 = coly[20:60]
    phase2.sort()

    phase3 = coly[60:100]
    phase3.sort()

    y[3].append(phase1[-1])
    y[3].append(phase2[-1])
    y[3].append(phase3[-1])

    print(y)

    return y


def draw():
    x_values = [1, 2, 3]
    y_values = ReadFile(repeat_num = 1)

    legend_labels = ["Spacker", "All-at-once", "Fluid", "Replication"]

    print(y_values)

    DrawFigureV4(x_values, y_values, legend_labels,
                         'Phase', 'Latency (ms)',
                         'breakdown_latency_overview', False)

if __name__ == '__main__': 
    draw()