from math import floor
import os

from analysis.config.default_config import timers_plot, per_task_rate, parallelism, per_key_state_size, \
    replicate_keys_filter, state_access_ratio, max_parallelism, FILE_FOLER, order_function, zipf_skew, sync_keys
from analysis.config.general_utilities import DrawFigure, breakdown_total


def ReadFile(repeat_num, query_id, query_name):
    w, h = 4, 1
    y = [[] for y in range(h)]

    replicate_keys_filter = 0
    if query_id == 8:
        sync_keys = 8
        # sync_keys = 4
    elif query_id == 5:
        sync_keys = 0
    else:
        sync_keys = 0

    exp = FILE_FOLER + '/spector-nexmark-query{}-{}-{}'\
                    .format(query_id, sync_keys, replicate_keys_filter)
    
    file_path = os.path.join(exp, "timer.output")

    fp = open(file_path)
    lines = fp.readlines()
    prev_timer = ""
    cur_migration_time = 0
    for line in lines:
        timer = line.split(" : ")
        # if timer[0] == "++++++replicationTimer" and "Timer" in prev_timer and prev_timer != "++++++replicationTimer":
        #     # migration end, compute the completion time
        #     y[0].append(cur_migration_time)
        #     cur_migration_time = 0
        if "Timer" in timer[0]:
            if timer[0] != "++++++replicationTimer" and timer[0] != "++++++endToEndTimer":
                # migration start, sum up all timers in between
                cur_migration_time += int(timer[1][:-3])
        # if "Timer" in timer[0]:
        #     prev_timer = timer[0]
    y[0].append(cur_migration_time)
    cur_migration_time = 0



    replicate_keys_filter = 0
    sync_keys = 0

    exp = FILE_FOLER + '/spector-nexmark-query{}-{}-{}'\
                    .format(query_id, sync_keys, replicate_keys_filter)

    file_path = os.path.join(exp, "timer.output")

    fp = open(file_path)
    lines = fp.readlines()
    prev_timer = ""
    cur_migration_time = 0
    for line in lines:
        timer = line.split(" : ")
        # if timer[0] == "++++++replicationTimer" and "Timer" in prev_timer and prev_timer != "++++++replicationTimer":
        #     # migration end, compute the completion time
        #     y[0].append(cur_migration_time)
        #     cur_migration_time = 0
        if "Timer" in timer[0]:
            if timer[0] != "++++++replicationTimer" and timer[0] != "++++++endToEndTimer":
                # migration start, sum up all timers in between
                cur_migration_time += int(timer[1][:-3])
        # if "Timer" in timer[0]:
        #     prev_timer = timer[0]
    y[0].append(cur_migration_time)
    cur_migration_time = 0




    replicate_keys_filter = 0
    sync_keys = 1

    exp = FILE_FOLER + '/spector-nexmark-query{}-{}-{}'\
                    .format(query_id, sync_keys, replicate_keys_filter)
    file_path = os.path.join(exp, "timer.output")

    fp = open(file_path)
    lines = fp.readlines()
    prev_timer = ""
    cur_migration_time = 0
    for line in lines:
        timer = line.split(" : ")
        # if timer[0] == "++++++replicationTimer" and "Timer" in prev_timer and prev_timer != "++++++replicationTimer":
        #     # migration end, compute the completion time
        #     y[0].append(cur_migration_time)
        #     cur_migration_time = 0
        if "Timer" in timer[0]:
            if timer[0] != "++++++replicationTimer" and timer[0] != "++++++endToEndTimer":
                # migration start, sum up all timers in between
                cur_migration_time += int(timer[1][:-3])
        # if "Timer" in timer[0]:
        #     prev_timer = timer[0]
    y[0].append(cur_migration_time)
    cur_migration_time = 0




    replicate_keys_filter = 1
    sync_keys = 0

    exp = FILE_FOLER + '/spector-nexmark-query{}-{}-{}'\
                    .format(query_id, sync_keys, replicate_keys_filter)
    file_path = os.path.join(exp, "timer.output")

    print(exp)

    fp = open(file_path)
    lines = fp.readlines()
    prev_timer = ""
    cur_migration_time = 0
    for line in lines:
        timer = line.split(" : ")
        # if timer[0] == "++++++replicationTimer" and "Timer" in prev_timer and prev_timer != "++++++replicationTimer":
        #     # migration end, compute the completion time
        #     y[0].append(cur_migration_time)
        #     cur_migration_time = 0
        if "Timer" in timer[0]:
            if timer[0] != "++++++replicationTimer" and timer[0] != "++++++endToEndTimer":
                # migration start, sum up all timers in between
                cur_migration_time += int(timer[1][:-3])
        # if "Timer" in timer[0]:
        #     prev_timer = timer[0]
    y[0].append(cur_migration_time)
    cur_migration_time = 0

    print(y)

    return y


def draw():
    for (query_id, query_name) in [(5, "window"), (8, "join"), (11, "window")]: # (1, "Mapper"), (2, "Splitter FlatMap"), (5, "window"), (8, "join"), (11, "window")
        x_values = ["Spacker", "All-at-once", "Fluid", "Replication"]
        y_values = ReadFile(1, query_id, query_name)

        legend_labels = ["Spacker", "All-at-once", "Fluid", "Replication"]

        print(y_values)

        DrawFigure(x_values, y_values, legend_labels,
                            '', 'Completion Time (ms)',
                            'nexmark_q{}_completion_time_overview'.format(query_id), False)

if __name__ == '__main__': 
    draw()