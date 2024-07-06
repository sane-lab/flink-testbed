import os

from analysis.config.default_config import FILE_FOLER, timers_plot, \
    breakdown_legend_labels
from analysis.config.general_utilities import DrawFigure, breakdown


def ReadFile(repeat_num = 1):
    w, h = 4, 3
    y = [[0 for x in range(w)] for y in range(h)]

    for repeat in range(1, repeat_num+1):
        i = 0
        for per_key_state_size in [1024, 4096, 8192, 16384]:
            exp = FILE_FOLER + '/spector-{}'.format(per_key_state_size)
            file_path = os.path.join(exp, "timer.output")
            # try:
            stats = breakdown(open(file_path).readlines())
            print(stats)
            for j in range(3):
                if timers_plot[j] not in stats:
                    y[j][i] = 0
                else:
                    y[j][i] += stats[timers_plot[j]]
            i += 1
            # except Exception as e:
            #     print("Error while processing the file {}: {}".format(exp, e))

    for j in range(h):
        for i in range(w):
            y[j][i] = y[j][i] / repeat_num

    return y


def draw():
    # runtime, per_task_rate, parallelism, key_set, per_key_state_size, reconfig_interval, reconfig_type, affected_tasks, repeat_num = val

    # parallelism
    # x_values = [1024, 10240, 20480, 40960]
    x_values = [1024 /16, 4096 / 16, 8192 / 16, 16384 / 16]
    y_values = ReadFile(repeat_num = 1)

    legend_labels = breakdown_legend_labels

    print(y_values)

    DrawFigure(x_values, y_values, legend_labels,
                         'State Size(mb)', 'Breakdown (ms)',
                         'breakdown_state_size', True)
