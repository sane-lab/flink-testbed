import os

from analysis.config.default_config import timers_plot, per_task_rate, parallelism, per_key_state_size, \
    replicate_keys_filter, state_access_ratio, FILE_FOLER
from analysis.config.general_utilities import DrawFigureV4, breakdown_total


def ReadFile(repeat_num = 1):
    w, h = 3, 3
    y = [[] for y in range(h)]
    # y = []

    # per_key_state_size = 32768
    # replicate_keys_filter = 0
    # sync_keys = 1
    # state_access_ratio = 2
    # per_task_rate = 5000
    # parallelism = 2
    max_parallelism = 1024

    for repeat in range(1, repeat_num + 1):
        for affected_keys in [256, 512, 1024]:
            i = 0
            w, h = 3, 3
            col_y = [[0 for x in range(w)] for y in range(h)]
            # for sync_keys in [1, int(max_parallelism / 16), int(max_parallelism / 2)]:
            for sync_keys in [1, 16, int(affected_keys / parallelism)]:
                exp = FILE_FOLER + '/workloads/spector-{}-{}-{}-{}-{}-{}-{}-{}'\
                    .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, sync_keys, replicate_keys_filter, state_access_ratio, affected_keys)
                file_path = os.path.join(exp, "timer.output")
                print(file_path)
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
    # x_values = [1024, 10240, 20480, 40960]
    # x_values = [128, 256, 512, 1024]
    x_values = [256, 512, 1024]
    y_values = ReadFile(repeat_num = 1)

    legend_labels = ["Chunk-1", "Chunk-16", "All-at-Once"]

    print(y_values)

    DrawFigureV4(x_values, y_values, legend_labels,
                         'Number of Keys to Migrate', 'Completion Time (ms)',
                         'breakdown_batching_affected_key_size', False)
