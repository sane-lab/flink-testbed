import getopt
import os
import sys
import utilities


def ReadFile(repeat_num = 1):
    w, h = 4, 3
    y = [[0 for _ in range(w)] for _ in range(h)]

    per_key_state_size = 16384

    for repeat in range(1, repeat_num + 1):
        i = 0
        sync_keys = 0
        for replicate_keys_filter in [1, 2, 4, 8]:
            exp = utilities.FILE_FOLER + '/spector-{}-{}-{}'.format(per_key_state_size, sync_keys, replicate_keys_filter)
            file_path = os.path.join(exp, "timer.output")
            # try:
            stats = utilities.breakdown(open(file_path).readlines())
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
    # runtime, per_task_rate, parallelism, key_set, per_key_state_size, reconfig_interval, reconfig_type, affected_tasks, repeat_num = val

    # parallelism
    # x_values = [1024, 10240, 20480, 40960]
    x_values = ["100%", "50%", "25%", "12.5%"]
    y_values = ReadFile(repeat_num = 1)

    legend_labels = utilities.legend_labels

    print(y_values)

    utilities.DrawFigure(x_values, y_values, legend_labels,
                         'Replicate Keys', 'Breakdown (ms)',
                         'breakdown_replicate_keys', True)
