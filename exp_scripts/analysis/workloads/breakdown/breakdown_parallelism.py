import os
import utilities


def ReadFile(repeat_num = 1):
    w, h = 3, 3
    y = [[0 for x in range(w)] for y in range(h)]

    per_key_state_size = 32768
    replicate_keys_filter = 0
    sync_keys = 0
    state_access_ratio = 2
    per_task_rate = 5000

    for repeat in range(1, repeat_num + 1):
        i = 0
        for parallelism in [2, 4, 8]:
            exp = utilities.FILE_FOLER + '/workloads//spector-{}-{}-{}-{}-{}-{}'.format(per_task_rate, per_key_state_size, sync_keys, replicate_keys_filter, parallelism,
                             state_access_ratio)
            file_path = os.path.join(exp, "timer.output")
            # try:
            stats = utilities.breakdown_total(open(file_path).readlines())
            print(stats)
            for j in range(3):
                if utilities.timers_plot[j] not in stats:
                    y[j][i] = 0
                else:
                    y[j][i] += stats[utilities.timers_plot[j]]
            i += 1
            # except Exception as e:
            #     print("Error while processing the file {}: {}".format(exp, e))

    for j in range(h):
        for i in range(w):
            y[j][i] = y[j][i] / repeat_num

    return y


def draw(val):
    x_values = [2, 4, 8]
    y_values = ReadFile(repeat_num = 1)

    legend_labels = utilities.legend_labels

    print(y_values)

    utilities.DrawFigure(x_values, y_values, legend_labels,
                         'Sync Keys', 'Breakdown (ms)',
                         'breakdown_parallelism', True)
