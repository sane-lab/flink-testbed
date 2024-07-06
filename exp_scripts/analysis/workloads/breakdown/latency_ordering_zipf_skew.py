from math import floor
import os

from analysis.config.default_config import timers_plot, per_task_rate, parallelism, per_key_state_size, \
    replicate_keys_filter, state_access_ratio, max_parallelism, FILE_FOLER, order_function, zipf_skew, sync_keys
from analysis.config.general_utilities import DrawFigureV4, breakdown_total


def ReadFile(repeat_num = 1):
    w, h = 5, 2
    y = [[] for y in range(h)]
    # y = []

    # replicate_keys_filter = 0
    sync_keys = 32
    state_access_ratio = 100
    per_task_rate = 5000
    # parallelism = 2
    # max_parallelism = 512

    for repeat in range(1, repeat_num + 1):
        # for zipf_skew in [0, 0.25, 0.5, 0.75, 1]:
        for zipf_skew in [0.25, 0.5, 0.75]:
            latency_dict = {}
            for order_function in ["default", "reverse"]:
                col = []
                coly = []
                start_ts = float('inf')
                temp_dict = {}
                for tid in range(0, parallelism):
                    f = open(FILE_FOLER + '/workloads/spector-{}-{}-{}-{}-{}-{}-{}-{}-{}/Splitter FlatMap-{}.output'
                            .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, \
                                    sync_keys, replicate_keys_filter, state_access_ratio, order_function, zipf_skew, tid))
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
                coly.sort()
                # latency_dict[order_function] = coly[floor(len(coly)*0.99)]
                latency_dict[order_function] = coly[-1]


            print(latency_dict)
            i = 0
            for latency in latency_dict.values():
                y[i].append(latency)
                i += 1


    return y


def draw():
    # runtime, per_task_rate, parallelism, key_set, per_key_state_size, reconfig_interval, reconfig_type, affected_tasks, repeat_num = val

    # parallelism
    # x_values = [0, 0.25, 0.5, 0.75, 1]
    x_values = [0.25, 0.5, 0.75]
    y_values = ReadFile(repeat_num = 1)

    legend_labels = ["hotkey-first", "random"]

    print(y_values)

    DrawFigureV4(x_values, y_values, legend_labels,
                         'Zipf Skew Ratio', 'P99 Latency Spike (ms)',
                         'latency_ordering_zipf_skew', False)
