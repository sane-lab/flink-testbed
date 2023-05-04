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

    exp = FILE_FOLER + '/spector-{}-{}-{}-{}-{}-{}-{}-{}-{}'\
                    .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, sync_keys,
                         replicate_keys_filter, state_access_ratio, zipf_skew, configScenario)
    file_path = os.path.join(exp, "timer.output")

    fp = open(file_path)
    lines = fp.readlines()
    prev_timer = ""
    cur_migration_time = 0
    for line in lines:
        timer = line.split(" : ")
        if timer[0] == "++++++replicationTimer" and "Timer" in prev_timer and prev_timer != "++++++replicationTimer":
            # migration end, compute the completion time
            y[0].append(cur_migration_time)
            cur_migration_time = 0
        elif timer[0] != "++++++replicationTimer" and "Timer" in timer[0]:
            # migration start, sum up all timers in between
            cur_migration_time += int(timer[1][:-3])
        if "Timer" in timer[0]:
            prev_timer = timer[0]


    configScenario = "static"
    replicate_keys_filter = 0
    sync_keys = 0

    exp = FILE_FOLER + '/spector-{}-{}-{}-{}-{}-{}-{}-{}-{}'\
                    .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, sync_keys,
                         replicate_keys_filter, state_access_ratio, zipf_skew, configScenario)
    file_path = os.path.join(exp, "timer.output")

    fp = open(file_path)
    lines = fp.readlines()
    prev_timer = ""
    cur_migration_time = 0
    for line in lines:
        timer = line.split(" : ")
        if timer[0] == "++++++replicationTimer" and "Timer" in prev_timer and prev_timer != "++++++replicationTimer":
            # migration end, compute the completion time
            y[1].append(cur_migration_time)
            cur_migration_time = 0
        elif timer[0] != "++++++replicationTimer" and "Timer" in timer[0]:
            # migration start, sum up all timers in between
            cur_migration_time += int(timer[1][:-3])
        if "Timer" in timer[0]:
            prev_timer = timer[0]


    configScenario = "static"
    replicate_keys_filter = 0
    sync_keys = 8

    exp = FILE_FOLER + '/spector-{}-{}-{}-{}-{}-{}-{}-{}-{}'\
                    .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, sync_keys,
                         replicate_keys_filter, state_access_ratio, zipf_skew, configScenario)
    file_path = os.path.join(exp, "timer.output")

    fp = open(file_path)
    lines = fp.readlines()
    prev_timer = ""
    cur_migration_time = 0
    for line in lines:
        timer = line.split(" : ")
        if timer[0] == "++++++replicationTimer" and "Timer" in prev_timer and prev_timer != "++++++replicationTimer":
            # migration end, compute the completion time
            y[2].append(cur_migration_time)
            cur_migration_time = 0
        elif timer[0] != "++++++replicationTimer" and "Timer" in timer[0]:
            # migration start, sum up all timers in between
            cur_migration_time += int(timer[1][:-3])
        if "Timer" in timer[0]:
            prev_timer = timer[0]

    configScenario = "static"
    replicate_keys_filter = 1
    sync_keys = 0

    exp = FILE_FOLER + '/spector-{}-{}-{}-{}-{}-{}-{}-{}-{}'\
                    .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, sync_keys,
                         replicate_keys_filter, state_access_ratio, zipf_skew, configScenario)
    file_path = os.path.join(exp, "timer.output")

    fp = open(file_path)
    lines = fp.readlines()
    prev_timer = ""
    cur_migration_time = 0
    for line in lines:
        timer = line.split(" : ")
        if timer[0] == "++++++replicationTimer" and "Timer" in prev_timer and prev_timer != "++++++replicationTimer":
            # migration end, compute the completion time
            y[3].append(cur_migration_time)
            cur_migration_time = 0
        elif timer[0] != "++++++replicationTimer" and "Timer" in timer[0]:
            # migration start, sum up all timers in between
            cur_migration_time += int(timer[1][:-3])
        if "Timer" in timer[0]:
            prev_timer = timer[0]

    print(y)

    return y


def draw():
    x_values = [1, 2, 3]
    y_values = ReadFile(repeat_num = 1)

    legend_labels = ["Spacker", "All-at-once", "Fluid", "Replication"]

    print(y_values)

    DrawFigureV4(x_values, y_values, legend_labels,
                         'Phase', 'Completion Time (ms)',
                         'breakdown_completion_time_overview', True)

if __name__ == '__main__': 
    draw()