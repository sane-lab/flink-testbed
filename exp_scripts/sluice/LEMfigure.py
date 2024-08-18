import numpy as np
import matplotlib
import random
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def estimateTaskLatency(tau_j:float, backlogs:list[float], arrival_rates:list[float], service_rate:float, keys:list[int], wait_time:float) -> float:
    # Estimate backlog as per the formula for \(\beta^{j}_{i}\)
    aggregate_arrival_rate = sum([arrival_rates[key] for key in keys])
    aggregate_backlog = sum([backlogs[key] for key in keys])
    projected_backlog = max(aggregate_backlog + (aggregate_arrival_rate - service_rate) * tau_j, 0.0)
    # Calculate task latency as per the formula \(l^{j}_{i}(\tau^j, \mathcal{C})\)
    task_latency = (projected_backlog + 1) / service_rate + wait_time
    return task_latency

def estimateOperatorLatency(tau_j: float, backlogs: list[float], arrivals: list[float], services: dict[str, float], mapping: dict[str, list[int]], wait_times: dict[str, float]) -> float:
    # Estimate operator latency as the maximum task latency
    task_latencies = [
        estimateTaskLatency(tau_j, backlogs, arrivals, services[task], mapping[task], wait_times[task])
        for task in mapping.keys()
    ]
    return max(task_latencies)


def estimateEndToEndLatency(t: float, sorted_operators: list[str], in_neighbors: dict[str, list[str]],
                            backlogs_per_operator: dict[str, list[float]],
                            arrivals_per_operator: dict[str, list[float]],
                            services_per_operator: dict[str, dict[str, float]], config: dict[str, dict[str, list[int]]],
                            wait_times_per_task: dict[str, dict[str, float]]) -> float:
    tau = {}  # Arrival times \(\tau_j\) at each operator
    l = {}  # Latency \(l^j(\tau^j, \mathcal{C})\) for each operator

    for operator in sorted_operators:
        if not in_neighbors[operator]:
            tau[operator] = t  # Set initial arrival time for sources
        else:
            # Compute \(\tau_j\) as the maximum arrival time from the predecessors plus their latency
            tau[operator] = max((tau[po] + l[po]) for po in in_neighbors[operator])

        # Estimate the operator's latency using its tasks' latencies
        l[operator] = estimateOperatorLatency(tau[operator], backlogs_per_operator[operator],
                                              arrivals_per_operator[operator], services_per_operator[operator],
                                              config[operator], wait_times_per_task[operator])

    # Return the maximum end-to-end latency minus the initial time
    return max((tau[operator] + l[operator]) for operator in sorted_operators) - t



outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/LEM/"
random_seed = 114514
random.seed(random_seed)

def generateSetting(name, flags):
    max_operator_num = flags["max_operator_num"]
    max_task_num = flags["max_task_num"]
    max_key_num = flags["max_key_num"]
    arrival_ranges = flags["arrival_ranges"]
    backlog_ranges = flags["backlog_ranges"]

    setting = {}
    setting["name"] = name
    setting_type = random.randint(0, 0)
    setting["type"] = setting_type
    num_operator = random.randint(1, max_operator_num)
    sorted_operators = ["op" + str(x) for x in range(1, num_operator + 1)]
    setting["operators"] = sorted_operators
    in_neighbors = {}
    arrivals = {}
    services = {}
    backlogs = {}
    delta_arrivals = {}
    config = {}
    for index in range(0, num_operator):
        operator = sorted_operators[index]
        if index == 0:
            in_neighbors[operator] = []
        else:
            n_in = random.randint(1, index)
            shuffled = [y for y in range(0, index)]
            in_neighbors[operator] = [sorted_operators[x] for x in shuffled[0: n_in]]
        services[operator] = {}
        arrivals[operator] = []
        backlogs[operator] = []
        task_num = random.randint(1, max_task_num)
        mapping = {}
        key_total_num = 0
        for j in range(1, task_num + 1):
            task = operator + "_t" + str(j)
            services[operator][task] = random.randint(950, 1050) / 1000.0
            key_number = random.randint(1, max_key_num)
            mapping[task] = range(key_total_num, key_total_num + key_number)
            config[operator] = mapping
            key_total_num += key_number
            arrival_interval = random.randint(0, len(arrival_ranges) - 1)
            task_arrival = random.randint(arrival_ranges[arrival_interval][0], arrival_ranges[arrival_interval][1]) / 1000.0
            random_arrival_weights = [random.random() ** 0.2 for _ in range(key_number)]
            for key in mapping[task]:
                arrivals[operator].append(task_arrival / sum(random_arrival_weights) * random_arrival_weights[key - key_total_num + key_number])
            backlog_interval = random.randint(0, len(backlog_ranges) - 1)
            task_backlog = random.randint(backlog_ranges[backlog_interval][0], backlog_ranges[backlog_interval][1])
            random_backlog_weights = [random.random() ** 0.2 for _ in range(key_number)]
            for key in mapping[task]:
                backlogs[operator].append(int(task_backlog / sum(random_backlog_weights) * random_backlog_weights[
                    key - key_total_num + key_number]))
    setting["config"] = config
    setting["in_neighbors"] = in_neighbors
    setting["service"] = services
    setting["arrival"] = arrivals
    setting["backlog"] = backlogs
    return setting
def getManualSetting(name):
    setting = {}
    if name == "special":
        # Special case
        setting_name = "special"
        sorted_operators = ["op1", "op2", "op3", "op4"]
        in_neighbors = {
            "op1": [],
            "op2": ["op1"],
            "op3": ["op2"],
            "op4": ["op3"],
        }
        config = {
            "op1":
                {"op1_t1": [0]},
            "op2":
                {"op2_t1": [0]},
            "op3":
                {"op3_t1": [0]},
            "op4":
                {"op4_t1": [0]},
        }
        init_backlog = {
            "op1": [1],
            "op2": [1],
            "op3": [1],
            "op4": [1],
        }
        init_arrival = {
            "op1": [0.100],
            "op2": [0.100],
            "op3": [0.100],
            "op4": [0.105],
        }
        init_service = {
            "op1": {
                "op1_t1": 0.120,
            },
            "op2": {
                "op2_t1": 0.120,
            },
            "op3": {
                "op3_t1": 0.120,
            },
            "op4": {
                "op4_t1": 0.100,
            }
        }
        setting["arrival"] = init_arrival
        setting["service"] = init_service
        setting["backlog"] = init_backlog
        setting["operators"] = sorted_operators
        setting["config"] = config
        setting["in_neighbors"] = in_neighbors
        setting["name"] = setting_name
        setting["type"] = 0
    elif name == "time_low_load":
        setting_name = name
        sorted_operators = ["op1", "op2", "op3"]
        in_neighbors = {
            "op1": [],
            "op2": ["op1"],
            "op3": ["op2"],
        }
        config = {
            "op1":
                {"op1_t1": [0]},
            "op2":
                {"op2_t1": [0]},
            "op3":
                {"op3_t1": [0]},
        }
        init_backlog = {
            "op1": [1500],
            "op2": [1000],
            "op3": [800],
        }
        init_arrival = {
            "op1": [0.2],
            "op2": [0.8],
            "op3": [0.9],
        }
        init_service = {
            "op1": {
                "op1_t1": 1.0,
            },
            "op2": {
                "op2_t1": 1.0,
            },
            "op3": {
                "op3_t1": 1.0,
            }
        }
        setting["arrival"] = init_arrival
        setting["service"] = init_service
        setting["backlog"] = init_backlog
        setting["operators"] = sorted_operators
        setting["config"] = config
        setting["in_neighbors"] = in_neighbors
        setting["name"] = setting_name
        setting["type"] = 0
    elif name == "time_moderate_load":
        setting_name = name
        sorted_operators = ["op1", "op2", "op3"]
        in_neighbors = {
            "op1": [],
            "op2": ["op1"],
            "op3": ["op2"],
        }
        config = {
            "op1":
                {"op1_t1": [0]},
            "op2":
                {"op2_t1": [0]},
            "op3":
                {"op3_t1": [0]},
        }
        init_backlog = {
            "op1": [1000],
            "op2": [100],
            "op3": [1],
        }
        init_arrival = {
            "op1": [0.5],
            "op2": [1.05],
            "op3": [0.1],
        }
        init_service = {
            "op1": {
                "op1_t1": 1.0,
            },
            "op2": {
                "op2_t1": 1.0,
            },
            "op3": {
                "op3_t1": 1.0,
            }
        }
        setting["arrival"] = init_arrival
        setting["service"] = init_service
        setting["backlog"] = init_backlog
        setting["config"] = config
        setting["operators"] = sorted_operators
        setting["in_neighbors"] = in_neighbors
        setting["name"] = setting_name
        setting["type"] = 0
    elif name == "time_heavy_load":
        setting_name = name
        sorted_operators = ["op1", "op2", "op3"]
        in_neighbors = {
            "op1": [],
            "op2": ["op1"],
            "op3": ["op2"],
        }
        config = {
            "op1":
                {"op1_t1": [0], "op1_t2": [1]},
            "op2":
                {"op2_t1": [0]},
            "op3":
                {"op3_t1": [0]},
        }
        init_backlog = {
            "op1": [1, 1000],
            "op2": [500],
            "op3": [1],
        }
        init_arrival = {
            "op1": [1.3, 1.1],
            "op2": [1.05],
            "op3": [0.1],
        }
        init_service = {
            "op1": {
                "op1_t1": 1.0,
                "op1_t2": 1.0,
            },
            "op2": {
                "op2_t1": 1.0,
            },
            "op3": {
                "op3_t1": 1.0,
            }
        }
        setting["arrival"] = init_arrival
        setting["service"] = init_service
        setting["backlog"] = init_backlog
        setting["config"] = config
        setting["operators"] = sorted_operators
        setting["in_neighbors"] = in_neighbors
        setting["name"] = setting_name
        setting["type"] = 0
    elif name == "time_complex":
        setting_name = name
        sorted_operators = ["op1", "op2", "op3", "op4"]
        in_neighbors = {
            "op1": [],
            "op2": ["op1"],
            "op3": ["op1"],
            "op4": ["op2", "op3"],
        }
        config = {
            "op1":
                {"op1_t1": [0], "op1_t2": [1]},
            "op2":
                {"op2_t1": [0]},
            "op3":
                {"op3_t1": [0]},
            "op4":
                {"op4_t1": [0], "op4_t2": [1]},
        }
        init_backlog = {
            "op1": [1, 1000],
            "op2": [5000],
            "op3": [1],
            "op4": [1, 3000],
        }
        init_arrival = {
            "op1": [1.05, 0.75],
            "op2": [0.9],
            "op3": [1.5],
            "op4": [1, 0.8],
        }
        init_service = {
            "op1": {
                "op1_t1": 1.0,
                "op1_t2": 1.0,
            },
            "op2": {
                "op2_t1": 1.0,
            },
            "op3": {
                "op3_t1": 1.0,
            },
            "op4": {
                "op4_t1": 1.0,
                "op4_t2": 1.0,
            },
        }
        setting["arrival"] = init_arrival
        setting["service"] = init_service
        setting["backlog"] = init_backlog
        setting["config"] = config
        setting["operators"] = sorted_operators
        setting["config"] = config
        setting["in_neighbors"] = in_neighbors
        setting["name"] = setting_name
        setting["type"] = 0
    elif name == "configuration_low_load":
        setting_name = name
        sorted_operators = ["op1", "op2", "op3"]
        in_neighbors = {
            "op1": [],
            "op2": ["op1"],
            "op3": ["op2"],
        }
        config = {
            "op1":
                {"op1_t1": [0, 1, 2, 3]},
            "op2":
                {"op2_t1": [0, 1, 2, 3]},
            "op3":
                {"op3_t1": [0, 1, 2, 3]},
        }
        init_backlog = {
            "op1": [600, 400, 300, 200],
            "op2": [400, 300, 200, 100],
            "op3": [300, 250, 150, 100],
        }
        init_arrival = {
            "op1": [0.05, 0.05, 0.05, 0.05],
            "op2": [0.2, 0.3, 0.1, 0.2],
            "op3": [0.2, 0.25, 0.25, 0.2],
        }
        init_service = {
            "op1": {
                "op1_t1": 1.0,
            },
            "op2": {
                "op2_t1": 1.0,
            },
            "op3": {
                "op3_t1": 1.0,
            }
        }
        setting["arrival"] = init_arrival
        setting["service"] = init_service
        setting["backlog"] = init_backlog
        setting["config"] = config
        setting["operators"] = sorted_operators
        setting["in_neighbors"] = in_neighbors
        setting["name"] = setting_name
        setting["type"] = 0
    elif name == "configuration_moderate_load":
        setting_name = name
        sorted_operators = ["op1", "op2", "op3"]
        in_neighbors = {
            "op1": [],
            "op2": ["op1"],
            "op3": ["op2"],
        }
        config = {
            "op1":
                {"op1_t1": [0, 1, 2, 3]},
            "op2":
                {"op2_t1": [0, 1, 2, 3]},
            "op3":
                {"op3_t1": [0, 1, 2, 3]},
        }
        init_backlog = {
            "op1": [400, 300, 200, 100],
            "op2": [40, 30, 20, 10],
            "op3": [0, 0, 0, 1],
        }
        init_arrival = {
            "op1": [0.2, 0.15, 0.1, 0.05],
            "op2": [0.4, 0.3, 0.15, 0.2],
            "op3": [0.025, 0.025, 0.025, 0.025],
        }
        init_service = {
            "op1": {
                "op1_t1": 1.0,
            },
            "op2": {
                "op2_t1": 1.0,
            },
            "op3": {
                "op3_t1": 1.0,
            }
        }
        setting["arrival"] = init_arrival
        setting["service"] = init_service
        setting["backlog"] = init_backlog
        setting["config"] = config
        setting["operators"] = sorted_operators
        setting["in_neighbors"] = in_neighbors
        setting["name"] = setting_name
        setting["type"] = 0
    elif name == "configuration_heavy_load":
        setting_name = name
        sorted_operators = ["op1", "op2", "op3"]
        in_neighbors = {
            "op1": [],
            "op2": ["op1"],
            "op3": ["op2"],
        }
        config = {
            "op1":
                {"op1_t1": [0, 1, 2, 3]},
            "op2":
                {"op2_t1": [0, 1, 2, 3]},
            "op3":
                {"op3_t1": [0, 1, 2, 3]},
        }
        init_backlog = {
            "op1": [1, 1, 500, 500],
            "op2": [125, 125, 125, 125],
            "op3": [1, 1, 1, 1],
        }
        init_arrival = {
            "op1": [0.65, 0.65, 0.55, 0.55],
            "op2": [0.27, 0.27, 0.27, 0.27],
            "op3": [0.1, 0.1, 0.1, 0.1],
        }
        init_service = {
            "op1": {
                "op1_t1": 1.0,
            },
            "op2": {
                "op2_t1": 1.0,
            },
            "op3": {
                "op3_t1": 1.0,
            }
        }
        setting["arrival"] = init_arrival
        setting["service"] = init_service
        setting["backlog"] = init_backlog
        setting["config"] = config
        setting["operators"] = sorted_operators
        setting["in_neighbors"] = in_neighbors
        setting["name"] = setting_name
        setting["type"] = 0
    return setting


def evenly_reduce_backlogs(backlogs, new_sum, keys):
    X = sum([backlogs[key] for key in keys]) - new_sum
    if X >= 0:
        total_reduction = 0
        tkeys = keys
        while total_reduction < X:
            # Calculate the amount to reduce in this round
            reduction_per_item = max(int((X - total_reduction) / len(tkeys)), 1)

            # Distribute the reduction evenly
            for key in tkeys:
                if backlogs[key] > 0:
                    reduction = min(reduction_per_item, backlogs[key])
                    backlogs[key] -= reduction
                    total_reduction += reduction

                    # Stop if we've reduced by X
                    if total_reduction >= X:
                        break

            # Remove any zeros from consideration in future iterations
            tkeys = [key for key in tkeys if backlogs[key] > 0]

            # Break if there are no more positive backlogs left
            if not tkeys:
                break
    else:
        per_key_increase = int((-X) / len(keys) + 1e-9)
        remain = (-X) % len(keys)
        for key in keys:
            backlogs[key] += per_key_increase
            if remain > 0:
                backlogs[key] += 1
                remain -= 1

    return backlogs


def emulateOnSetting(setting, flags):
    is_remained_backlog_flag = flags["is_remained_backlog_flag"]
    time_epoch = flags["time_epoch"]
    time_times = flags["time_times"]
    emulation_epoch = flags["emulation_epoch"]
    emulation_times = flags["emulation_times"]
    latency_spike = flags["latency_spike"]
    latency_bound = flags["latency_bound"]
    rate_flag = flags["rate_flag"]



    setting_name = setting["name"]
    sorted_operators = setting["operators"]
    in_neighbors = setting["in_neighbors"]
    setting_type = setting["type"]
    # init_delta_arrival = setting["delta_arrival"]
    init_backlog = setting["backlog"]
    init_arrival = setting["arrival"]
    init_service = setting["service"]
    config = setting["config"]
    if "wait_time" not in setting:
        wait_times = {}
        for operator, tasks in init_service.items():
            wait_times[operator] = {task: 0.0 for task in tasks}
    else:
        wait_times = setting["wait_time"]

    arrival = {}
    service = {}
    backlog = {}
    first_so = -1
    first_so_l = 0
    need_so = False
    need_so_time = 0
    need_so_l = 0
    last_y0 = -1000

    fig = plt.figure(figsize=(7, 4))
    # Have a look at the colormaps here and decide which one you'd like:
    # http://matplotlib.org/1.2.1/examples/pylab_examples/show_colormaps.html
    num_plots = emulation_times
    colormap = plt.cm.gist_ncar
    plt.gca().set_prop_cycle(plt.cycler('color', plt.cm.jet(np.linspace(0, 1, num_plots))))
    legend = []
    for emulate_time in range(0, emulation_times):
        new_backlogs = {}
        for operator in init_backlog.keys():
            new_backlogs[operator] = {}
            if operator not in arrival:
                arrival[operator] = {}
                service[operator] = {}
                backlog[operator] = {}
            new_backlogs[operator] = init_backlog[operator].copy()
            for task in init_service[operator].keys():
                new_backlog = sum([init_backlog[operator][key] for key in config[operator][task]]) + int((sum([init_arrival[operator][key] for key in config[operator][task]]) - init_service[operator][task] + 1e-9) * emulate_time * emulation_epoch)
                if new_backlog < 0:
                    new_backlog = 0
                if is_remained_backlog_flag and new_backlog < int(sum([init_arrival[operator][key] for key in config[operator][task]]) * 10 + 1e-9):
                    new_backlog = int(sum([init_arrival[operator][key] for key in config[operator][task]]) * 10 + 1e-9)
                evenly_reduce_backlogs(new_backlogs[operator], new_backlog, config[operator][task])
                if task not in arrival[operator]:
                    arrival[operator][task] = []
                    backlog[operator][task] = []
                    service[operator][task] = []
                arrival[operator][task] += [sum(init_arrival[operator])]
                service[operator][task] += [init_service[operator][task]]
                backlog[operator][task] += [new_backlog]

        y = []
        x = []
        for ts in range(0, time_times):
            x += [emulate_time * emulation_epoch + ts * time_epoch]
            y += [estimateEndToEndLatency(ts * time_epoch, sorted_operators, in_neighbors, new_backlogs, init_arrival, init_service, config, wait_times)]
        plt.plot(x, y, '-', markersize=2, linewidth=1)
        plt.xlabel("t(ms)")
        plt.ylabel("l(t,C)(ms)")
        # legend += ["l(t,C)" + str(emulate_time * emulation_epoch / 1000) + "s"]
        if (not need_so) and last_y0 != -1000 and last_y0 < y[0] and y[0] > latency_bound:
            need_so = True
            need_so_time = emulate_time * emulation_epoch
            need_so_l = y[0]
        if first_so < 0 and y[0] < y[1] and y[1] + latency_spike > latency_bound:
            first_so = emulate_time * emulation_epoch
            first_so_l = y[0]
        last_y0 = y[0]
    if need_so:
        plt.plot([need_so_time], [need_so_l], 'o', color='red', markersize=4)
    if first_so >= 0:
        plt.plot([first_so], [first_so_l], 'o', color='blue', markersize=4)
    plt.legend(legend, ncol=5, loc='upper left')
    plt.grid(True)
    axes = plt.gca()
    axes.set_xlim(0, emulation_times * emulation_epoch + time_times * time_epoch)
    # axes.set_xticks(np.arange(0, emulation_times * emulation_epoch + time_times * time_epoch, 2000.0))

    import os
    if not os.path.exists(outputDir + setting_name):
        os.makedirs(outputDir+ setting_name)
    plt.savefig(outputDir + setting_name + "/" + "LEM_" + "t"+ '.png', bbox_inches='tight')
    plt.close(fig)

    if rate_flag:
        task_column = 1
        for operator in sorted_operators:
            if len(arrival[operator]) > task_column:
                task_column = len(arrival[operator])
        fig, axs = plt.subplots(len(sorted_operators), task_column, figsize=(7, 4), layout='constrained')
        for index in range(0, len(sorted_operators)):
            operator = sorted_operators[index]
            for sec_index in range(0, len(arrival[operator])):
                task = operator + "_t" + str(sec_index + 1)
                if len(sorted_operators) > 1:
                    row = axs[index]
                else:
                    row = axs
                if task_column > 1:
                    ax1 = row[sec_index]
                else:
                    ax1 = row
                ax2 = ax1.twinx()
                if emulation_times == 1:
                    ax1.plot([0, 60000], arrival[operator][task] + arrival[operator][task],
                             color="red", linewidth=1)
                    ax1.plot([0, 60000], service[operator][task] + service[operator][task],
                             color="blue", linewidth=1)
                    ax2.plot([0, 60000], backlog[operator][task] + backlog[operator][task],
                             color="black", linewidth=1)
                else:
                    ax1.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch), arrival[operator][task], color="red", linewidth=2)
                    ax1.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch), service[operator][task], color="blue", linewidth=2)
                    ax2.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch), backlog[operator][task], color="black", linewidth=2)
                ax1.legend(["arrival rate", "service rate"], loc="upper left", ncol=2)
                ax2.legend(["backlog"], loc="upper right")
                ax1.set_ylabel(operator)
                ax1.set_xlim(0, emulation_times * emulation_epoch + time_times * time_epoch)
                ax1.set_xlabel(task)
                # ax1.set_xticks(np.arange(0, emulation_times * emulation_epoch + time_times * time_epoch, 2000.0))
                ax1.set_ylim(0.0, 2.0)
                ax2.set_ylim(0, 2050)
                # if(operator == "op4"):
                #     ax2.set_ylim(0, 100)
                # else:
                #     ax2.set_ylim(0, 4)
        import os
        if not os.path.exists(outputDir + setting_name):
            os.makedirs(outputDir + setting_name)
        plt.savefig(outputDir + setting_name + "/" + "LEM_" + "metrics" + '.png', bbox_inches='tight')
        plt.close(fig)


    if not need_so and first_so >= 0:
        return 1
    if need_so and first_so < 0:
        return 2
    if need_so_time < first_so:
        return 3
    return 0

def moveOutBottleneckKey(sorted_operators: list[str], in_neighbors: dict[str, list[str]], init_backlog: dict[str, list[float]], init_arrival: dict[str, list[float]], new_service: dict[str, dict[str,float]], current_config: dict[str, dict[str, list[int]]], wait_times: dict[str, dict[str,float]]) -> bool:
    bestOperator = ""
    bestKey = -1
    bestl = 1000000000.0
    for operator in sorted_operators:
        new_task = operator + "_t" + str(len(current_config[operator].keys()) + 1)
        current_config[operator][new_task] = []
        new_service[operator][new_task] = sum(new_service[operator].values())/len(new_service[operator].keys())
        for task in current_config[operator].keys():
            if len(current_config[operator][task]) > 1:
                n = len(current_config[operator][task])
                for index in range(0, n):
                    key = current_config[operator][task][0]
                    current_config[operator][new_task] = [key]
                    current_config[operator][task].pop(0)
                    l = estimateEndToEndLatency(0, sorted_operators, in_neighbors, init_backlog, init_arrival,
                                                new_service, current_config, wait_times)
                    if bestKey == -1 or l < bestl:
                        bestl = l
                        bestKey = key
                        bestOperator = operator
                    current_config[operator][task].append(key)
        del current_config[operator][new_task]
        del new_service[operator][new_task]
    if bestKey < 0:
        return False
    new_task = bestOperator + "_t" + str(len(current_config[bestOperator].keys()) + 1)
    for task in current_config[bestOperator].keys():
        if bestKey in current_config[bestOperator][task]:
            current_config[bestOperator][task].remove(bestKey)
    current_config[bestOperator][new_task] = [bestKey]
    new_service[bestOperator][new_task] = sum(new_service[bestOperator].values())/len(new_service[bestOperator].keys())
    return True

def emulateMoveConfiguration(setting, flags):
    emulation_times = flags["emulation_times"]
    latency_spike = flags["latency_spike"]
    latency_bound = flags["latency_bound"]
    rate_flag = flags["rate_flag"]

    setting_name = setting["name"]
    sorted_operators = setting["operators"]
    in_neighbors = setting["in_neighbors"]
    setting_type = setting["type"]
    # init_delta_arrival = setting["delta_arrival"]
    init_backlog = setting["backlog"]
    init_arrival = setting["arrival"]
    init_service = setting["service"]
    config = setting["config"]
    if "wait_time" not in setting:
        wait_times = {}
        for operator, tasks in init_service.items():
            wait_times[operator] = {task: 0.0 for task in tasks}
    else:
        wait_times = setting["wait_time"]


    current_config = config.copy()
    latencys = []
    latency_trend = []

    new_service = init_service.copy()

    arrival = {}
    service = {}
    backlog = {}

    for operator in sorted_operators:
        arrival[operator] = {}
        service[operator] = {}
        backlog[operator] = {}
    index = 0
    while True:
        l0 = estimateEndToEndLatency(0, sorted_operators, in_neighbors, init_backlog, init_arrival, new_service, current_config, wait_times)
        lf = estimateEndToEndLatency(1000000, sorted_operators, in_neighbors, init_backlog, init_arrival, new_service, current_config, wait_times)
        latencys.append(l0)
        latency_trend.append(lf - l0)
        for operator in sorted_operators:
            for task in current_config[operator].keys():
                task_arrival = sum([init_arrival[operator][key] for key in current_config[operator][task]])
                task_backlog = sum([init_backlog[operator][key] for key in current_config[operator][task]])
                if task not in arrival[operator]:
                    arrival[operator][task] = [[], []]
                    service[operator][task] = [[], []]
                    backlog[operator][task] = [[], []]
                arrival[operator][task][0].append(index)
                arrival[operator][task][1].append(task_arrival)
                backlog[operator][task][0].append(index)
                backlog[operator][task][1].append(task_backlog)
                service[operator][task][0].append(index)
                service[operator][task][1].append(init_service[operator][task])
        index += 1
        if not moveOutBottleneckKey(sorted_operators, in_neighbors, init_backlog, init_arrival, new_service, current_config, wait_times):
            break

    move_times = index
    fig = plt.figure(figsize=(7, 4))
    # Have a look at the colormaps here and decide which one you'd like:
    # http://matplotlib.org/1.2.1/examples/pylab_examples/show_colormaps.html
    num_plots = emulation_times
    colormap = plt.cm.gist_ncar
    plt.gca().set_prop_cycle(plt.cycler('color', plt.cm.jet(np.linspace(0, 1, num_plots))))
    legend = []
    plt.plot(range(0, move_times), latencys, 'o-', color='blue', markersize=2, linewidth=1)
    maxTrend = max([abs(x) for x in latency_trend])
    if maxTrend == 0:
        maxTrend = 1

    for i in range(0, move_times):
        plt.plot([i, i], [latencys[i], latencys[i] + latency_trend[i]/abs(latency_trend[i]) *100], '-', color='red', linewidth=1)
    plt.xlabel("move x keys")
    plt.ylabel("l(t,C)(ms)")
    plt.legend(legend, ncol=5, loc='upper left')
    plt.grid(True)
    axes = plt.gca()
    axes.set_xlim(-1, move_times)
    # axes.set_xticks(np.arange(0, emulation_times * emulation_epoch + time_times * time_epoch, 2000.0))
    import os
    if not os.path.exists(outputDir + setting_name):
        os.makedirs(outputDir + setting_name)
    plt.savefig(outputDir + setting_name + "/" + "LEM_" + "C" + '.png', bbox_inches='tight')
    plt.close(fig)

    if rate_flag:
        if rate_flag:
            task_column = 1
            for operator in sorted_operators:
                if len(arrival[operator]) > task_column:
                    task_column = len(arrival[operator])
            fig, axs = plt.subplots(len(sorted_operators), task_column, figsize=(7, 4), layout='constrained')
            for index in range(0, len(sorted_operators)):
                operator = sorted_operators[index]
                for sec_index in range(0, len(arrival[operator])):
                    task = operator + "_t" + str(sec_index + 1)
                    if len(sorted_operators) > 1:
                        row = axs[index]
                    else:
                        row = axs
                    if task_column > 1:
                        ax1 = row[sec_index]
                    else:
                        ax1 = row
                    ax2 = ax1.twinx()

                    ax1.plot(arrival[operator][task][0],
                             arrival[operator][task][1], "d-", color="red", linewidth=2, markersize=4)
                    ax1.plot(service[operator][task][0],
                             service[operator][task][1], "d-", color="blue", linewidth=2, markersize=4)
                    ax2.plot(backlog[operator][task][0],
                             backlog[operator][task][1], "d-", color="black", linewidth=2, markersize=4)
                    #ax1.legend(["arrival rate", "service rate"], loc="upper left", ncol=2)
                    #ax2.legend(["backlog"], loc="upper right")
                    ax1.set_ylabel(operator)
                    ax1.set_xlim(-1, move_times)
                    ax1.set_xticks(np.arange(-1, move_times, 1))
                    ax1.set_xlabel("move x keys of task_"+str(sec_index))
                    # ax1.set_xticks(np.arange(0, emulation_times * emulation_epoch + time_times * time_epoch, 2000.0))
                    ax1.set_ylim(0.0, 2.0)
                    ax2.set_ylim(0, 2050)
                    # if(operator == "op4"):
                    #     ax2.set_ylim(0, 100)
                    # else:
                    #     ax2.set_ylim(0, 4)
            import os
            if not os.path.exists(outputDir + setting_name):
                os.makedirs(outputDir + setting_name)
            plt.savefig(outputDir + setting_name + "/" + "LEM_" + "metrics" + '.png', bbox_inches='tight')
            plt.close(fig)

def testScalingDecisionStrategy():
    flags = {}
    flags["is_remained_backlog_flag"] = True  # False
    flags["time_epoch"] = 1  # ms
    flags["time_times"] = 1000  # 1000
    flags["emulation_epoch"] = 2000  # ms
    flags["emulation_times"] = 10
    flags["latency_spike"] = 500
    flags["latency_bound"] = 2000
    flags["max_task_num"] = 3
    flags["rate_flag"] = True
    flags["arrival_ranges"] = [[250, 800], [801, 950], [951, 1050], [1050, 1200], [1200, 2000]]
    flags["backlog_ranges"] = [[0, 10], [11, 200], [201, 1000], [1001, 2000], [2001, 10000]]
    flags["max_operator_num"] = 4
    flags["max_key_num"] = 3

    late_scaleout = []
    miss_scaleout = []
    no_needscaleout = []
    setting_num = 1
    for i in range(0, setting_num):
        print("Start setting " + str(i))
        if i > 0:
            setting = generateSetting("set_" + str(i), flags)
        else:
            setting = getManualSetting("special")
        ret = emulateOnSetting(setting, flags)
        if ret == 1:
            no_needscaleout += [i]
        elif ret == 2:
            miss_scaleout += [i]
        elif ret == 3:
            late_scaleout += [i]
    print("OK setting: " + str(setting_num - len(late_scaleout) - len(miss_scaleout) - len(no_needscaleout)) + "/" + str(setting_num))
    print("Miss scale out: " + str(miss_scaleout))
    print("Late scale out: " + str(late_scaleout))
    print("Unnecessary scale out: " + str(no_needscaleout))

def analyzeLEM_time():
    flags = {}
    flags["is_remained_backlog_flag"] = True  # False
    flags["time_epoch"] = 1  # ms
    flags["time_times"] = 20000  # 1000
    flags["emulation_epoch"] = 1000  # ms
    flags["emulation_times"] = 1  # 60
    flags["latency_spike"] = 500
    flags["latency_bound"] = 2000
    flags["rate_flag"] = True
    flags["max_task_num"] = 3

    setting_list = ["time_low_load","time_moderate_load", "time_heavy_load", "time_complex"]
    for setting_name in setting_list:
        print("Start setting " + setting_name)
        setting = getManualSetting(setting_name)
        ret = emulateOnSetting(setting, flags)

def analyzeLEM_Configuration():
    flags = {}
    flags["is_remained_backlog_flag"] = True  # False
    flags["time_epoch"] = 1  # ms
    flags["time_times"] = 20000  # 1000
    flags["emulation_epoch"] = 1000  # ms
    flags["emulation_times"] = 1  # 60
    flags["latency_spike"] = 500
    flags["latency_bound"] = 2000
    flags["rate_flag"] = True
    flags["max_task_num"] = 3
    setting_list = ["configuration_low_load", "configuration_moderate_load", "configuration_heavy_load"]
    for setting_name in setting_list:
        print("Start setting " + setting_name)
        setting = getManualSetting(setting_name)
        emulateMoveConfiguration(setting, flags)

analyzeLEM_time()
#analyzeLEM_Configuration()
#testScalingDecisionStrategy()