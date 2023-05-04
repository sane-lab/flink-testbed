from math import floor
import os

from analysis.config.default_config import timers_plot, per_task_rate, parallelism, per_key_state_size, \
    replicate_keys_filter, state_access_ratio, max_parallelism, FILE_FOLER, order_function, zipf_skew, sync_keys
from analysis.config.general_utilities import DrawFigure, breakdown_total


def ReadFile(repeat_num, query_id, query_name):
    w, h = 4, 1
    y = [[] for y in range(h)]

    if query_id == 8:
        reading_files = [[0, 8], [0, 0], [0, 1], [1, 0]]
    elif query_id == 5:
        reading_files = [[0, 0], [0, 0], [0, 1], [1, 0]]

    for pair in reading_files:
        sync_keys = pair[1]
        replicate_keys_filter = pair[0]
        col = []
        coly = []
        start_ts = float('inf')
        temp_dict = {}
        for tid in range(0, 8):
            f = open(FILE_FOLER + "/spector-nexmark-query{}-{}-{}/{}-{}.output"
                     .format(query_id, sync_keys, replicate_keys_filter, query_name, tid))
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
            temp_dict[ts].sort()
            coly.append(temp_dict[ts][floor((len(temp_dict[ts]))*0.99)])
            col.append(ts - start_ts)

        list = coly[20:40]
        list.sort()
        y[0].append(list[-1])

    print(y)

    return y


def draw():
    for (query_id, query_name) in [(5, "window"), (8, "join")]: # (1, "Mapper"), (2, "Splitter FlatMap"), (5, "window"), (8, "join")
        x_values = ["Spacker", "All-at-once", "Fluid", "Replication"]
        y_values = ReadFile(1, query_id, query_name)

        legend_labels = ["Spacker", "All-at-once", "Fluid", "Replication"]

        print(y_values)

        DrawFigure(x_values, y_values, legend_labels,
                            '', 'Latency (ms)',
                            'nexmark_q{}_latency_overview'.format(query_id), False)

if __name__ == '__main__': 
    draw()