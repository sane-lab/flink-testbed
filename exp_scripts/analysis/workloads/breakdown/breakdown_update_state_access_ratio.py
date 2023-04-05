import os

from analysis.config.default_config import timers_plot, per_task_rate, parallelism, state_access_ratio, max_parallelism, \
    FILE_FOLER, sync_keys, per_key_state_size
from analysis.config.general_utilities import DrawFigureV4, breakdown


def ReadFile(repeat_num = 1):
    w, h = 4, 4
    y = [[] for y in range(h)]
    # y = []

    per_key_state_size = 4096
    # replicate_keys_filter = 0
    # sync_keys = 1
    # state_access_ratio = 2
    # per_task_rate = 5000
    # parallelism = 2
    # max_parallelism = 512

    for repeat in range(1, repeat_num + 1):
        for state_access_ratio in [1, 2, 4, 8]:
            i = 0
            w, h = 4, 3

            col_y = [[0 for x in range(w)] for y in range(h)]
            for replicate_keys_filter in [1, 2, 4, 8]:
                exp = FILE_FOLER + '/workloads/spector-{}-{}-{}-{}-{}-{}-{}'\
                    .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, sync_keys, replicate_keys_filter, state_access_ratio)
                file_path = os.path.join(exp, "timer.output")
                # try:
                stats = breakdown(open(file_path).readlines())
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
    # x_values = [1024, 10240, 20480, 40960]
    x_values = [1, 2, 4, 8]
    y_values = ReadFile(repeat_num = 1)

    legend_labels = ["Repl-100%", "Repl-50%", "Repl-25%", "Repl-12.5%"]

    print(y_values)

    DrawFigureV4(x_values, y_values, legend_labels,
                         'State Access Ratio (%)', 'Breakdown (ms)',
                         'breakdown_update_state_access_ratio', True)
