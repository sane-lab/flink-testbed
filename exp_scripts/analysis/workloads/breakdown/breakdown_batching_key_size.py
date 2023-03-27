import os
import utilities


def ReadFile(repeat_num = 1):
    w, h = 4, 3
    y = [[] for y in range(h)]
    # y = []

    per_key_state_size = 32768
    replicate_keys_filter = 0
    sync_keys = 1
    state_access_ratio = 2
    per_task_rate = 5000
    parallelism = 2
    max_parallelism = 512

    for repeat in range(1, repeat_num + 1):
        for max_parallelism in [128, 256, 512, 1024]:
            i = 0
            w, h = 3, 3
            col_y = [[0 for x in range(w)] for y in range(h)]
            for sync_keys in [1, int(max_parallelism / 16), int(max_parallelism/2)]:
                exp = utilities.FILE_FOLER + '/spector-{}-{}-{}-{}-{}-{}-{}'\
                    .format(per_task_rate, parallelism, max_parallelism, per_key_state_size, sync_keys, replicate_keys_filter, state_access_ratio)
                file_path = os.path.join(exp, "timer.output")
                # try:
                stats = utilities.breakdown_total(open(file_path).readlines())
                print(stats)
                for j in range(3):
                    if utilities.timers_plot[j] not in stats:
                        col_y[j][i] = 0
                    else:
                        col_y[j][i] += stats[utilities.timers_plot[j]]
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


def draw(val):
    # runtime, per_task_rate, parallelism, key_set, per_key_state_size, reconfig_interval, reconfig_type, affected_tasks, repeat_num = val

    # parallelism
    # x_values = [1024, 10240, 20480, 40960]
    x_values = [128, 256, 512, 1024]
    y_values = ReadFile(repeat_num = 1)

    legend_labels = ["Fluid", "Batched", "All-At-Once"]

    print(y_values)

    utilities.DrawFigure(x_values, y_values, legend_labels,
                         'Sync Keys', 'Breakdown (ms)',
                         'breakdown_batching_key_size', True)
