import os

from analysis.config.default_config import timers_plot, per_task_rate, parallelism, per_key_state_size, \
    replicate_keys_filter, state_access_ratio, max_parallelism, FILE_FOLER, order_function, zipf_skew, sync_keys
from analysis.config.general_utilities import DrawFigureV4, DrawFigureV5, breakdown_total


def ReadFile(repeat_num = 1):
    w, h = 5, 3
    y = [[] for y in range(h)]
    # y = []

    # replicate_keys_filter = 0
    sync_keys = 16
    state_access_ratio = 100
    per_task_rate = 5000
    # parallelism = 2
    # max_parallelism = 512

    for repeat in range(1, repeat_num + 1):
        # for zipf_skew in [0, 0.25, 0.5, 0.75, 1]:
        # for zipf_skew in [0.5, 0.75, 1]:
        for zipf_skew in [0.2, 0.4, 0.6]:
            i = 0
            w, h = 3, 3
            col_y = [[0 for x in range(w)] for y in range(h)]
            for order_function in ["default", "random", "reverse"]:
                exp = FILE_FOLER + '/workloads/spector-{}-{}-{}-{}-{}-{}-{}-{}-{}'\
                    .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, \
                             sync_keys, replicate_keys_filter, state_access_ratio, order_function, zipf_skew)
                file_path = os.path.join(exp, "timer.output")
                # try:
                stats = breakdown_total(open(file_path).readlines())
                print(stats)
                for j in range(3):
                    if timers_plot[j] not in stats:
                        col_y[j][i] = 0
                    else:
                        col_y[j][i] += stats[timers_plot[j]]
                i += 1
                # except Exception as e:
                #     print("Error while processing the file {}: {}".format(exp, e))

            for j in range(h):
                for i in range(w):
                    col_y[j][i] = col_y[j][i] / repeat_num

            col = []

            for i in range(w):
                completion_time = 0
                for j in range(h):
                    completion_time += col_y[j][i]
                y[i].append(completion_time)
            #     col.append(completion_time)
            #
            # print(col)
            # y.append(col)

    return y


def draw():
    # runtime, per_task_rate, parallelism, key_set, per_key_state_size, reconfig_interval, reconfig_type, affected_tasks, repeat_num = val

    # parallelism
    # x_values = [0, 0.25, 0.5, 0.75, 1]
    x_values = [0.2, 0.4, 0.6]
    # x_values = [0.5, 0.75, 1]
    y_values = ReadFile(repeat_num = 1)

    legend_labels = ["hotkey-first", "random", "coldkey-first"]

    print(y_values)

    DrawFigureV4(x_values, y_values, legend_labels,
                         'Zipf Skew Ratio', 'Completion Time (ms)',
                         'breakdown_ordering_zipf_skew', True)
