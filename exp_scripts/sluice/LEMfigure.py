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
    return max((tau[operator] + l[operator] - t) for operator in sorted_operators)



outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/LEM/"
random_seed = 114514
random.seed(random_seed)

type_name = ["steady", "changing", "linear", "bursty"]

def generateSettingOld(name, flags):
    max_operator_num = flags["max_operator_num"]
    max_task_num = flags["max_task_num"]
    max_key_num = flags["max_key_num"]
    arrival_ranges = flags["arrival_ranges"]
    backlog_ranges = flags["backlog_ranges"]
    service_ranges = flags["service_ranges"]
    arrival_change_period_ranges = flags["arrival_period_range"]
    arrival_change_ratio_ranges = flags["arrival_ratio_range"]
    arrival_change_delta_ranges = flags["arrival_delta_range"]

    setting = {}
    setting["name"] = name
    #setting_type = random.randint(0, 0)
    setting_type = flags["type"]
    setting["type"] = setting_type
    if flags["graph_fixed"]:
        num_operator = max_operator_num
    else:
        num_operator = random.randint(1, max_operator_num)
    sorted_operators = ["op" + str(x) for x in range(1, num_operator + 1)]
    setting["operators"] = sorted_operators
    in_neighbors = {}
    arrivals = {}
    services = {}
    backlogs = {}
    config = {}
    period_interval = random.randint(0, len(arrival_change_period_ranges) - 1)
    arrival_change_period = random.randint(arrival_change_period_ranges[period_interval][0], arrival_change_period_ranges[period_interval][1])  # random.randint(5 * 1000, 10 * 1000)
    ratio_interval = random.randint(0, len(arrival_change_ratio_ranges) - 1)
    arrival_change_ratio = random.randint(arrival_change_ratio_ranges[ratio_interval][0], arrival_change_ratio_ranges[ratio_interval][1]) / 100.0 # random.randint(0, 50) / 100.0
    delta_interval = random.randint(0, len(arrival_change_delta_ranges) - 1)
    arrival_change_delta = random.randint(arrival_change_delta_ranges[delta_interval][0], arrival_change_delta_ranges[delta_interval][1]) / 1000.0 * arrival_change_period # random.randint(0, 1000) / 1000.0 * arrival_change_period
    for index in range(0, num_operator):
        operator = sorted_operators[index]
        if index == 0:
            in_neighbors[operator] = []
        else:
            if flags["graph_fixed"]:
                in_neighbors[operator] = [sorted_operators[index - 1]]
            else:
                n_in = random.randint(1, index)
                shuffled = [y for y in range(0, index)]
                in_neighbors[operator] = [sorted_operators[x] for x in shuffled[0: n_in]]
        services[operator] = {}
        arrivals[operator] = []
        backlogs[operator] = []
        if flags["graph_fixed"]:
            task_num = max_task_num
        else:
            task_num = random.randint(1, max_task_num)
        mapping = {}
        key_total_num = 0
        for j in range(1, task_num + 1):
            task = operator + "_t" + str(j)
            service_interval = random.randint(0, len(service_ranges) - 1)
            services[operator][task] = random.randint(service_ranges[service_interval][0], service_ranges[service_interval][1]) / 1000.0 #random.randint(950, 1050) / 1000.0
            if flags["graph_fixed"]:
                key_number = max_key_num
            else:
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
    setting["arrival_period"] = arrival_change_period
    setting["arrival_ratio"] = arrival_change_ratio
    setting["arrival_delta"] = arrival_change_delta
    return setting
def getManualSettingOld(name):
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
    elif name.startswith("so_one_"):
        setting_name = name
        sorted_operators = ["op1", "op2", "op3", "op4"]
        in_neighbors = {
            "op1": [],
            "op2": ["op1"],
            "op3": ["op2"],
            "op4": ["op3"],
        }
        config = {
            "op1": {
                "op1_t1": [0, 1, 2],
                "op1_t2": [3, 4, 5]
            },
            "op2": {
                "op2_t1": [0, 1, 2],
                "op2_t2": [3, 4, 5]
            },
            "op3": {
                "op3_t1": [0, 1, 2],
                "op3_t2": [3, 4, 5]
            },
            "op4": {
                "op4_t1": [0, 1, 2],
                "op4_t2": [3, 4, 5]
            }
        }
        init_backlog = {
            "op1": [0, 0, 0, 0, 0, 0],
            "op2": [0, 0, 0, 0, 0, 0],
            "op3": [0, 0, 0, 0, 0, 0],
            "op4": [0, 0, 0, 0, 0, 0],
        }
        init_service = {
            "op1": {
                "op1_t1": 1.0,
                "op1_t2": 1.0
            },
            "op2": {
                "op2_t1": 1.0,
                "op2_t2": 1.0
            },
            "op3": {
                "op3_t1": 1.0,
                "op3_t2": 1.0
            },
            "op4": {
                "op4_t1": 1.0,
                "op4_t2": 1.0
            }
        }
        type = int(name.split("_")[2])
        case = int(name.split("_")[3])
        if type == 0:
            task_arrival = {
                "op1": {
                    "op1_t1": 0.5,
                    "op1_t2": 0.5,
                },
                "op2": {
                    "op2_t1": 0.5,
                    "op2_t2": 1.25 + (case - 1) * 0.05
                },
                "op3": {
                    "op3_t1": 0.5,
                    "op3_t2": 0.5
                },
                "op4": {
                    "op4_t1": 0.5,
                    "op4_t2": 1.25 + (case - 1) * 0.025
                }
            }
            arrival_change_period = 5000
            arrival_change_ratio = 0.25
            arrival_change_delta = 0.25 * arrival_change_period
        elif type == 1:
            task_arrival = {
                "op1": {
                    "op1_t1": 0.6,
                    "op1_t2": 0.6,
                },
                "op2": {
                    "op2_t1": 0.6,
                    "op2_t2": 0.6,
                },
                "op3": {
                    "op3_t1": 1.25,
                    "op3_t2": 0.6
                },
                "op4": {
                    "op4_t1": 1.25,
                    "op4_t2": 0.6
                }
            }
            arrival_change_period = 5000
            arrival_change_ratio = 1.0 / (4 - case)
            arrival_change_delta = 0.25 * arrival_change_period
        elif type == 2:
            task_arrival = {
                "op1": {
                    "op1_t1": 0.6,
                    "op1_t2": 0.6,
                },
                "op2": {
                    "op2_t1": 0.6,
                    "op2_t2": 0.6,
                },
                "op3": {
                    "op3_t1": 1.25,
                    "op3_t2": 0.6
                },
                "op4": {
                    "op4_t1": 1.25,
                    "op4_t2": 0.6
                }
            }
            arrival_change_period = 3333
            arrival_change_ratio = 0.33
            arrival_change_delta = 0.2 * case * arrival_change_period
        elif type == 3:
            task_arrival = {
                "op1": {
                    "op1_t1": 0.6,
                    "op1_t2": 0.6,
                },
                "op2": {
                    "op2_t1": 0.6,
                    "op2_t2": 0.6,
                },
                "op3": {
                    "op3_t1": 1.25,
                    "op3_t2": 0.6
                },
                "op4": {
                    "op4_t1": 1.25,
                    "op4_t2": 0.6
                }
            }
            arrival_change_period = 2000 + 500 * case
            arrival_change_ratio = 0.20
            arrival_change_delta = 0.2 * arrival_change_period

        def divide_arrival_rates(init_arrival, config):
            key_arrival_rates = {}
            for operator, tasks in init_arrival.items():
                key_arrival_rates[operator] = []
                for task, arrival_rate in tasks.items():
                    num_keys = len(config[operator][task])
                    rate_per_key = arrival_rate / num_keys
                    key_arrival_rates[operator].extend([rate_per_key] * num_keys)

            return key_arrival_rates

        init_arrival = divide_arrival_rates(task_arrival, config)
        setting["arrival"] = init_arrival
        setting["service"] = init_service
        setting["backlog"] = init_backlog
        setting["config"] = config
        setting["operators"] = sorted_operators
        setting["in_neighbors"] = in_neighbors
        setting["name"] = setting_name
        setting["type"] = type
        setting["arrival_period"] = arrival_change_period
        setting["arrival_ratio"] = arrival_change_ratio
        setting["arrival_delta"] = arrival_change_delta

    return setting

def getManualSetting(name):
    setting = {}
    if name.startswith("so_one_"):
        setting_name = name
        sorted_operators = ["op1", "op2", "op3", "op4"]
        in_neighbors = {
            "op1": [],
            "op2": ["op1"],
            "op3": ["op2"],
            "op4": ["op3"],
        }
        config = {
            "op1": {
                "op1_t1": [0, 1, 2],
                "op1_t2": [3, 4, 5],
            },
            "op2": {
                "op2_t1": [0, 1, 2],
                "op2_t2": [3, 4, 5],
            },
            "op3": {
                "op3_t1": [0, 1, 2],
                "op3_t2": [3, 4, 5],
            },
            "op4": {
                "op4_t1": [0, 1, 2],
                "op4_t2": [3, 4, 5],
            }
        }
        init_backlog = {
            "op1": [0, 0, 0, 0, 0, 0],
            "op2": [0, 0, 0, 0, 0, 0],
            "op3": [0, 0, 0, 0, 0, 0],
            "op4": [0, 0, 0, 0, 0, 0],
        }
        operator_service = {
            "op1": 1.0,
            "op2": 1.0,
            "op3": 1.0,
            "op4": 1.0,
        }
        pattern = int(name.split("_")[2])
        case = int(name.split("_")[3])
        if pattern == 0:
            source_arrival = 1.0 + (case - 1) * 0.05
            key_distribution = {
                "op1": [1, 1, 1, 1, 1, 1],
                "op2": [1, 1, 1, 3, 3, 3],
                "op3": [1, 1, 1, 1, 1, 1],
                "op4": [1, 1, 1, 3, 3, 3],
            }
            operator_io = {
                "op1": 2,
                "op2": 0.5,
                "op3": 2,
                "op4": 1.0,
            }

            arrival_change_period = 5000
            arrival_change_ratio = 0.25
            arrival_change_delta = 0.25 * arrival_change_period
        elif pattern == 1:
            source_arrival = 1.2
            key_distribution = {
                "op1": [1, 1, 1, 1, 1, 1],
                "op2": [1, 1, 1, 1, 1, 1],
                "op3": [2, 2, 2, 1, 1, 1],
                "op4": [2, 2, 2, 1, 1, 1],
            }
            operator_io = {
                "op1": 1.0,
                "op2": 1.5,
                "op3": 1.0,
                "op4": 1.0,
            }
            arrival_change_period = 5000
            arrival_change_ratio = 1.0 / (4 - case)
            arrival_change_delta = 0.25 * arrival_change_period
        elif pattern == 2:
            source_arrival = 1.2
            key_distribution = {
                "op1": [1, 1, 1, 1, 1, 1],
                "op2": [1, 1, 1, 1, 1, 1],
                "op3": [2, 2, 2, 1, 1, 1],
                "op4": [2, 2, 2, 1, 1, 1],
            }
            operator_io = {
                "op1": 1.0,
                "op2": 1.5,
                "op3": 1.0,
                "op4": 1.0,
            }
            arrival_change_period = 3333
            arrival_change_ratio = 0.33
            arrival_change_delta = 0.2 * case * arrival_change_period
        elif pattern == 3:
            source_arrival = 1.2
            key_distribution = {
                "op1": [1, 1, 1, 1, 1, 1],
                "op2": [1, 1, 1, 1, 1, 1],
                "op3": [1, 1, 1, 2, 2, 2],
                "op4": [1, 1, 1, 2, 2, 2],
            }
            operator_io = {
                "op1": 1.0,
                "op2": 1.5,
                "op3": 1.0,
                "op4": 1.0,
            }
            arrival_change_period = 2000 + 500 * case
            arrival_change_ratio = 0.20
            arrival_change_delta = 0.2 * arrival_change_period

        setting["backlog"] = init_backlog
        setting["arrival"] = source_arrival
        setting["service"] = operator_service
        setting["io_ratio"] = operator_io
        setting["key_distribution"] = key_distribution

        setting["config"] = config
        setting["operators"] = sorted_operators
        setting["in_neighbors"] = in_neighbors
        setting["name"] = setting_name
        setting["arrival_pattern"] = pattern
        setting["arrival_period"] = arrival_change_period
        setting["arrival_ratio"] = arrival_change_ratio
        setting["arrival_delta"] = arrival_change_delta

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


def get_arrival_rate(initial_arrival, period, ratio, delta, pattern_type: int, time):
    """
    Calculate the arrival rate based on the given pattern and ratio of initial arrival rate.

    :param initial_arrival: Initial arrival rate.
    :param period: The period of the arrival rate change.
    :param ratio: The ratio of the initial arrival rate to be used as amplitude.
    :param delta: The phase shift for the pattern.
    :param pattern_type: The type of change pattern (0: steady, 1: linear, 2: sine, 3: bursty).
    :param time: The current time (epoch).
    :return: The computed arrival rate.
    """
    amplitude = initial_arrival * ratio
    adjusted_time = int(time + delta)
    if pattern_type == 0:
        # Steady pattern
        return initial_arrival

    elif pattern_type == 1:
        phase = (adjusted_time % (period / 2)) / (period / 2)
        if phase < 0.5:
            return initial_arrival - amplitude + 4 * amplitude * phase
        else:
            return initial_arrival + amplitude - 4 * amplitude * (phase - 0.5)

    elif pattern_type == 2:
        # Sine wave pattern
        return initial_arrival + amplitude * np.sin(2 * np.pi * adjusted_time / period)

    elif pattern_type == 3:
        # Bursty pattern - alternates between low and high values
        if adjusted_time % period < period / 2:
            return initial_arrival - amplitude
        else:
            return initial_arrival + amplitude

    return initial_arrival
def emulateMoveTime(setting, flags):
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

    plt.legend(legend, ncol=5, loc='upper left')
    plt.grid(True)
    axes = plt.gca()
    axes.set_xlim(0, emulation_times * emulation_epoch + time_times * time_epoch)
    # axes.set_xticks(np.arange(0, emulation_times * emulation_epoch + time_times * time_epoch, 2000.0))

    import os
    if not os.path.exists(outputDir + setting_name):
        os.makedirs(outputDir + setting_name)
    plt.savefig(outputDir + setting_name + "/" + "LEM_" + "t" + '.png', bbox_inches='tight')
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
                    ax1.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch), arrival[operator][task],
                             color="red", linewidth=2)
                    ax1.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch), service[operator][task],
                             color="blue", linewidth=2)
                    ax2.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch), backlog[operator][task],
                             color="black", linewidth=2)
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

    return 0


def emulateOnSetting(setting, flags):
    is_remained_backlog_flag = flags["is_remained_backlog_flag"]
    time_epoch = flags["time_epoch"]
    time_times = flags["time_times"]
    emulation_epoch = flags["emulation_epoch"]
    emulation_times = flags["emulation_times"] + 1
    latency_spike = flags["latency_spike"]
    latency_bound = flags["latency_bound"]
    rate_flag = flags["rate_flag"]
    check_scale_type = flags["scale_type"]

    setting_name = setting["name"]
    sorted_operators = setting["operators"]
    in_neighbors = setting["in_neighbors"]
    config:dict[str,dict[str:list[int]]] = setting["config"]

    arrival_pattern = setting["arrival_pattern"]
    arrival_change_period = setting["arrival_period"]
    arrival_change_ratio = setting["arrival_ratio"]
    arrival_change_delta = setting["arrival_delta"]

    init_backlog:dict[str, list[int]] = setting["backlog"]
    source_arrival:float = setting["arrival"]
    operator_service:dict[str, float] = setting["service"]
    operator_io:dict[str, float] = setting["io_ratio"]
    key_distribution:dict[str, list[float]] = setting["key_distribution"]

    if "wait_time" not in setting:
        wait_times = {}
        for operator, mapping in config.items():
            wait_times[operator] = {task: 0.0 for task in mapping.keys()}
    else:
        wait_times = setting["wait_time"]

    fig_arrival = {operator:{} for operator in sorted_operators}
    fig_ob_arrival = {operator: {} for operator in sorted_operators}
    fig_service = {operator:{} for operator in sorted_operators}
    fig_backlog = {operator:{} for operator in sorted_operators}
    fig_operator_true_arrival = {operator:[] for operator in sorted_operators}
    fig_operator_ob_arrival = {operator: [] for operator in sorted_operators}
    fig_operator_service = {operator:[] for operator in sorted_operators}

    # Record
    first_so = -1
    first_so_l = 0
    first_so_ll = 0
    first_so_nl = 0
    need_so = False
    need_so_time = 0
    need_so_l = 0
    can_si = False
    can_si_time = 0
    can_si_l = 0
    first_si = -1
    first_si_l = 0
    last_y0 = -1000

    def drawGraph():
        import networkx as nx
        fig = plt.figure()  # figsize=(3, 2))
        ax = plt.subplot(111)
        G = nx.DiGraph()
        edges = []
        for operator in sorted_operators:
            for x in in_neighbors[operator]:
                edges += [(x, operator)]
        G.add_edges_from(edges)
        for layer, nodes in enumerate(nx.topological_generations(G)):
            # `multipartite_layout` expects the layer as a node attribute, so add the
            # numeric layer value as a node attribute
            for node in nodes:
                G.nodes[node]["layer"] = layer

        # Compute the multipartite_layout using the "layer" node attribute
        pos = nx.multipartite_layout(G, subset_key="layer")
        nx.draw_networkx(G, pos=pos, ax=ax, node_size=1600, font_size=20)
        import os
        if not os.path.exists(outputDir + setting_name):
            os.makedirs(outputDir + setting_name)
        plt.savefig(outputDir + setting_name + "/" + "graph.png", bbox_inches='tight')
        plt.close(fig)
    drawGraph()
    x = []
    y = []
    current_key_backlogs = {operator: backlogs.copy() for operator, backlogs in init_backlog.items()}
    for emulate_time in range(0, emulation_times):
        new_source_arrival = get_arrival_rate(source_arrival, arrival_change_period, arrival_change_ratio, arrival_change_delta, arrival_pattern, emulate_time * emulation_epoch)
        operator_true_output_rate = {}
        operator_observed_output_rate = {}
        key_true_arrivals = {}
        key_observed_arrivals = {}
        task_services = {}
        new_key_backlogs = {operator : y.copy() for operator, y in current_key_backlogs.items()}
        print(str(emulate_time) + " source ar " + str(new_source_arrival))
        for operator in sorted_operators:
            # Calculate arrival rate
            if not in_neighbors[operator]:
                total_operator_true_arrival = new_source_arrival
                total_operator_observed_arrival = new_source_arrival
            else:
                total_operator_true_arrival = sum([operator_true_output_rate[t_operator] for t_operator in in_neighbors[operator]])
                total_operator_observed_arrival = sum([operator_observed_output_rate[t_operator] for t_operator in in_neighbors[operator]])
            total_key_weight = sum(key_distribution[operator])
            key_true_arrivals[operator] = [total_operator_true_arrival / total_key_weight * key_distribution[operator][key] for key in range(0, len(key_distribution[operator]))]
            key_observed_arrivals[operator] = [total_operator_observed_arrival / total_key_weight * key_distribution[operator][key] for key in range(0, len(key_distribution[operator]))]

            task_services[operator] = {}
            operator_true_output_rate[operator] = 0
            operator_observed_output_rate[operator] = 0
            fig_operator_true_arrival[operator] += [total_operator_true_arrival]
            fig_operator_ob_arrival[operator] += [total_operator_observed_arrival]
            fig_operator_service[operator] += [len(config[operator].keys()) * operator_service[operator]]
            # Calculate output rate & Update backlog
            for task, mapped_keys in config[operator].items():

                task_backlog = sum(current_key_backlogs[operator][key] for key in config[operator][task])
                task_true_arrival = sum(key_true_arrivals[operator][key] for key in config[operator][task])
                task_observed_arrival = sum(key_observed_arrivals[operator][key] for key in config[operator][task])
                task_service = operator_service[operator]
                task_services[operator][task] = task_service
                task_true_output_rate = task_true_arrival * operator_io[operator]
                print(task + " ob arrival " + str(task_observed_arrival))
                if task_observed_arrival >= task_service:
                    task_observed_output_rate = task_service * operator_io[operator]
                    new_task_backlog = max(task_backlog + (task_observed_arrival - task_service) * emulation_epoch, (task_true_arrival * 10 + 1e-9) if is_remained_backlog_flag else 0)
                else:
                    if task_backlog >= task_true_arrival * 10 + 1e-9:
                        t = min((task_backlog - task_true_arrival * 10 + 1e-9) / (task_service - task_observed_arrival), emulation_epoch)
                    else:
                        t = 0
                    task_observed_output_rate = (task_service * t + task_observed_arrival * (emulation_epoch - t)) / emulation_epoch * operator_io[operator]
                    print(task + " t-epoch " + str(t)+"-"+str(emulation_epoch) + " ob_o " + str(task_observed_output_rate))
                    new_task_backlog = max((task_backlog + (task_observed_arrival - task_service) * emulation_epoch), (task_true_arrival * 10 + 1e-9) if is_remained_backlog_flag else 0)
                def distribute_backlogs(backlogs, new_sum, keys, operator, key_distribution):
                    X = sum([backlogs[key] for key in keys]) - new_sum
                    unit_factor = 1/sum([key_distribution[operator][key] for key in keys])
                    if X >= 0:
                        # Reducing backlog
                        total_reduction = 0.0
                        tkeys = keys
                        while total_reduction < X - 1e-10:
                            # Calculate the amount to reduce based on key distribution
                            reductions = {}
                            for key in tkeys:
                                reductions[key] = (X - total_reduction) * key_distribution[operator][key] * unit_factor

                            # Distribute the reduction according to the distribution
                            for key in tkeys:
                                if backlogs[key] > 0:
                                    reduction = min(reductions[key], backlogs[key])
                                    backlogs[key] -= reduction
                                    total_reduction += reduction

                                    # Stop if we've reduced by X
                                    if total_reduction >= X:
                                        break

                            # Remove any zeros or negative backlogs from consideration in future iterations
                            tkeys = [key for key in tkeys if backlogs[key] > 0]

                            # Break if there are no more positive backlogs left
                            if not tkeys:
                                break
                    else:
                        # Increasing backlog
                        per_key_increase = (-X)
                        for key in keys:
                            increment = per_key_increase * key_distribution[operator][key] * unit_factor
                            backlogs[key] += increment

                    return backlogs
                # def distribute_backlogs(backlogs:list[int], new_task_backlog, keys, operator:str, key_distribution:dict[str, list[float]]):
                #     X = sum([backlogs[key] for key in keys]) - new_task_backlog
                #     if X >= 0:
                #         # Reducing backlog
                #         total_reduction = 0
                #         tkeys = keys
                #         while total_reduction < X:
                #             # Calculate the amount to reduce based on key distribution
                #             reductions = {}
                #             for key in tkeys:
                #                 reductions[key] = max(int((X - total_reduction) * key_distribution[operator][key]), 1)
                #
                #             # Distribute the reduction according to the distribution
                #             for key in tkeys:
                #                 if backlogs[key] > 0:
                #                     reduction = min(reductions[key], backlogs[key])
                #                     backlogs[key] -= reduction
                #                     total_reduction += reduction
                #
                #                     # Stop if we've reduced by X
                #                     if total_reduction >= X:
                #                         break
                #
                #             # Remove any zeros from consideration in future iterations
                #             tkeys = [key for key in tkeys if backlogs[key] > 0]
                #
                #             # Break if there are no more positive backlogs left
                #             if not tkeys:
                #                 break
                #     else:
                #         # Increasing backlog
                #         per_key_increase = int((-X) / len(keys) + 1e-9)
                #         remain = (-X) % len(keys)
                #         for key in keys:
                #             increment = int(per_key_increase * key_distribution[operator][key])
                #             backlogs[key] += increment
                #             if remain > 0:
                #                 backlogs[key] += 1
                #                 remain -= 1
                #     return backlogs
                #print(task + " current backlog: " + str(task_backlog) + " new backlog:" + str(new_task_backlog))
                #print(task + " new backlog " + str(new_task_backlog))
                #print(operator + " old backlog " + str(new_key_backlogs[operator]))
                distribute_backlogs(new_key_backlogs[operator], new_task_backlog, config[operator][task], operator, key_distribution)
                print(operator + " new backlog " + str(new_key_backlogs[operator]))
                print(operator + " new backlogs: " + str(new_key_backlogs[operator]))
                operator_true_output_rate[operator] += task_true_output_rate
                operator_observed_output_rate[operator] += task_observed_output_rate
                #print(task + " ob rate " + str(task_observed_output_rate))
                #print(task + " true rate " + str(task_true_output_rate))
                if task not in fig_arrival[operator]:
                    fig_arrival[operator][task] = []
                    fig_ob_arrival[operator][task] = []
                    fig_backlog[operator][task] = []
                    fig_service[operator][task] = []
                fig_arrival[operator][task] += [task_true_arrival]
                fig_ob_arrival[operator][task] += [task_observed_arrival]
                fig_service[operator][task] += [task_service]
                fig_backlog[operator][task] += [int(task_backlog)]
        integer_backlogs = {
            operator: [ int(x) for x in backlogs] for operator, backlogs in current_key_backlogs.items()
        }
        l0 = estimateEndToEndLatency(0, sorted_operators, in_neighbors, integer_backlogs, key_true_arrivals,
                                     task_services, config, wait_times)
        l1 = estimateEndToEndLatency(emulation_epoch, sorted_operators, in_neighbors, integer_backlogs, key_true_arrivals,
                                     task_services, config, wait_times)
        lT = estimateEndToEndLatency(1000000, sorted_operators, in_neighbors, integer_backlogs, key_true_arrivals,
                                     task_services, config, wait_times)
        x += [emulate_time * emulation_epoch]
        y += [l0]
        if check_scale_type == "scale out":
            if (not need_so) and last_y0 != -1000 and last_y0 < l0 and l0 > latency_bound:
                need_so = True
                need_so_time = emulate_time * emulation_epoch
                need_so_l = l0
            if first_so < 0 and l0 < lT and l1 + latency_spike > latency_bound and emulate_time * emulation_epoch % 100 == 0:
                first_so = emulate_time * emulation_epoch
                first_so_l = l0
                first_so_ll = last_y0
            if first_so >= 0 and first_so == emulate_time * emulation_epoch - 100 and emulate_time * emulation_epoch % 100 == 0:
                first_so_nl = l0
        elif check_scale_type == "scale in":
            if (not can_si) and last_y0 != -1000 and last_y0 >= l0 and l0 + latency_spike < latency_bound:
                can_si = True
                can_si_time = emulate_time * emulation_epoch
                can_si_l = l0
            if can_si and l0 > latency_bound:
                can_si = False
            if first_si < 0 and l0 >= lT and l0 + latency_spike < latency_bound and emulate_time * emulation_epoch % 100 == 0:
                first_si = emulate_time * emulation_epoch
                first_si_l = l0

        current_key_backlogs = new_key_backlogs

    fig = plt.figure(figsize=(5, 3))
    # Have a look at the colormaps here and decide which one you'd like:
    # http://matplotlib.org/1.2.1/examples/pylab_examples/show_colormaps.html
    num_plots = emulation_times
    # colormap = plt.cm.gist_ncar
    # plt.gca().set_prop_cycle(plt.cycler('color', plt.cm.jet(np.linspace(0, 1, num_plots))))
    plt.plot(x, y, '-', color='blue', linewidth=2)
    plt.plot([0, emulation_times * emulation_epoch + time_times * time_epoch], [latency_bound, latency_bound], '--',
             color='red', linewidth=1)
    legend = ["latency curve", "limit"]
    if check_scale_type == "scale out":
        if need_so:
            legend += ["violation point"]
            plt.plot([need_so_time], [need_so_l], 's', color='red', markersize=6)
        if first_so >= 0:
            legend += ["decision time", "add scaling spike", "decision next epoch", "add scaling spike"]
            plt.plot([first_so], [first_so_l], 'd', color='blue', markersize=6)
            plt.plot([first_so], [first_so_l + latency_spike], 's', color='blue', markersize=6)
            plt.plot([first_so + 100], [first_so_nl], 'd', color='orange', markersize=6)
            plt.plot([first_so + 100], [first_so_nl + latency_spike], 's', color='orange', markersize=6)
            plt.plot([first_so, first_so], [first_so_l, first_so_l + latency_spike], '-', color='black', linewidth=1)
            plt.plot([first_so + 100, first_so + 100], [first_so_nl, first_so_nl + latency_spike], '-', color='black',
                     linewidth=1)
    elif check_scale_type == "scale in":
        if can_si:
            legend += ["violation point"]
            plt.plot([can_si_time], [can_si_l], 's', color='red', markersize=4)
        if first_si >= 0:
            legend += ["decision made", "latency + spike"]
            plt.plot([first_si], [first_si_l], 'd', color='blue', markersize=4)
            plt.plot([first_si], [first_si_l + latency_spike], 'd', color='black', markersize=4)
            plt.plot([first_si, first_si], [first_si_l, first_si_l + latency_spike], '-', color='black', linewidth=1)
    plt.ylim(0, 10000)
    plt.legend(legend, ncol=2, loc='upper left')
    plt.grid(True)
    axes = plt.gca()
    axes.set_xlim(0, (emulation_times - 1) * emulation_epoch + time_times * time_epoch)
    # axes.set_xticks(np.arange(0, emulation_times * emulation_epoch + time_times * time_epoch, 2000.0))

    import os
    if not os.path.exists(outputDir + setting_name):
        os.makedirs(outputDir + setting_name)
    plt.savefig(outputDir + setting_name + "/" + "LEM_" + "t" + '.png', bbox_inches='tight')
    plt.close(fig)

    if rate_flag:
        task_column = 1
        for operator in sorted_operators:
            if len(fig_arrival[operator]) > task_column:
                task_column = len(fig_arrival[operator])
        fig, axs = plt.subplots(len(sorted_operators), task_column, figsize=(7, 4), layout='constrained', sharex=True,
                                sharey=True)
        for index in range(0, len(sorted_operators)):
            operator = sorted_operators[index]
            for sec_index in range(0, len(fig_arrival[operator])):
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
                # legend1 = ["arrival rate", "service rate", "strategy scale point"]
                # legend2 = ["backlog"]

                ax1.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch),
                         [x * 1000 for x in fig_arrival[operator][task]], label="true arrival rate", color="red", linewidth=2)
                ax1.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch),
                         [x * 1000 for x in fig_ob_arrival[operator][task]], label="observed arrival rate", color="orange",
                         linewidth=2)
                ax1.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch),
                         [x * 1000 for x in fig_service[operator][task]], label="service rate", color="blue", linewidth=2)
                if check_scale_type == "scale out":
                    if first_so >= 0:
                        ax1.plot([first_so, first_so], [0, 10000], '--', label="strategy scale point", color='red',
                                 markersize=4, linewidth=2)
                elif check_scale_type == "scale in":
                    if first_si >= 0:
                        ax1.plot([first_si, first_si], [0, 10000], '--', label="strategy scale point", color='red',
                                 markersize=4, linewidth=2)
                ax2.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch), fig_backlog[operator][task],
                         label="backlog", color="black", linewidth=2)
                if index == 0 and sec_index == 0:
                    lines_labels = [ax1.get_legend_handles_labels()]
                    lines_labels2 = [ax2.get_legend_handles_labels()]
                    # ax1.legend(legend1, loc="upper left", ncol=3, )
                    # ax2.legend(legend2, loc="lower right")

                ax1.set_title(task)
                # ax1.set_ylabel(operator)
                ax1.set_xlim(0, (emulation_times - 1) * emulation_epoch + time_times * time_epoch)
                # ax1.set_xlabel(task)
                # ax1.set_xticks(np.arange(0, emulation_times * emulation_epoch + time_times * time_epoch, 2000.0))
                ax1.set_ylim(0, 2000)
                ax2.set_ylim(0, 5000)

                # if(operator == "op4"):
                #     ax2.set_ylim(0, 100)
                # else:
                #     ax2.set_ylim(0, 4)
        lines, labels = [sum(lol, []) for lol in zip(*lines_labels)]
        lines2, labels2 = [sum(lol, []) for lol in zip(*lines_labels2)]
        plt.tick_params(labelcolor='none', which='both', top=False, bottom=False, left=False, right=False)
        fig.supxlabel('Time')
        fig.supylabel('Rate/Backlog')
        fig.legend(lines + lines2, labels + labels2, loc='upper center', bbox_to_anchor=(0.5, 1.07), ncol=4)
        import os
        if not os.path.exists(outputDir + setting_name):
            os.makedirs(outputDir + setting_name)
        plt.savefig(outputDir + setting_name + "/" + "LEM_" + "metrics" + '.png', bbox_inches='tight')
        plt.close(fig)


        # print operator
        fig, axs = plt.subplots(len(sorted_operators), 1, figsize=(7, 4), layout='constrained', sharex=True,
                                sharey=True)
        for index in range(0, len(sorted_operators)):
            operator = sorted_operators[index]
            if len(sorted_operators) > 1:
                ax1 = axs[index]
            else:
                ax1 = axs
            # legend1 = ["arrival rate", "service rate", "strategy scale point"]
            # legend2 = ["backlog"]

            ax1.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch),
                     [x * 1000 for x in fig_operator_true_arrival[operator]], label="total true arrival rate", color="red",
                     linewidth=2)
            ax1.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch),
                     [x * 1000 for x in fig_operator_ob_arrival[operator]], label="total observed arrival rate", color="orange",
                     linewidth=2)
            ax1.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch),
                     [x * 1000 for x in fig_operator_service[operator]], label="total service rate",
                     color="blue",
                     linewidth=2)
            if check_scale_type == "scale out":
                if first_so >= 0:
                    ax1.plot([first_so, first_so], [0, 10000], '--', label="strategy scale point", color='red',
                             markersize=4, linewidth=2)
            elif check_scale_type == "scale in":
                if first_si >= 0:
                    ax1.plot([first_si, first_si], [0, 10000], '--', label="strategy scale point", color='red',
                             markersize=4, linewidth=2)
            if index == 0:
                lines_labels = [ax1.get_legend_handles_labels()]
                # ax1.legend(legend1, loc="upper left", ncol=3, )
                # ax2.legend(legend2, loc="lower right")

            ax1.set_title(operator)
            # ax1.set_ylabel(operator)
            ax1.set_xlim(0, (emulation_times - 1) * emulation_epoch + time_times * time_epoch)
            # ax1.set_xlabel(task)
            # ax1.set_xticks(np.arange(0, emulation_times * emulation_epoch + time_times * time_epoch, 2000.0))
            ax1.set_ylim(0, 5000)
        lines, labels = [sum(lol, []) for lol in zip(*lines_labels)]
        plt.tick_params(labelcolor='none', which='both', top=False, bottom=False, left=False, right=False)
        fig.supxlabel('Time')
        fig.supylabel('Rate')
        fig.legend(lines, labels, loc='upper center', bbox_to_anchor=(0.5, 1.07), ncol=2)
        import os
        if not os.path.exists(outputDir + setting_name):
            os.makedirs(outputDir + setting_name)
        plt.savefig(outputDir + setting_name + "/" + "LEM_" + "operator_metrics" + '.png', bbox_inches='tight')
        plt.close(fig)

    if check_scale_type == "scale out":
        if not need_so and first_so >= 0:
            return 1
        if need_so and first_so < 0:
            return 2
        if need_so and need_so_time < first_so:
            return 3
    elif check_scale_type == "scale in":
        if not can_si and first_si >= 0:
            return 1
        if can_si and first_si < 0:
            return 2
        if can_si and can_si_time > first_si:
            return 3
    return 0






def emulateOnSettingOld(setting, flags):
    is_remained_backlog_flag = flags["is_remained_backlog_flag"]
    time_epoch = flags["time_epoch"]
    time_times = flags["time_times"]
    emulation_epoch = flags["emulation_epoch"]
    emulation_times = flags["emulation_times"] + 1
    latency_spike = flags["latency_spike"]
    latency_bound = flags["latency_bound"]
    rate_flag = flags["rate_flag"]
    check_scale_type = flags["scale_type"]

    setting_name = setting["name"]
    sorted_operators = setting["operators"]
    in_neighbors = setting["in_neighbors"]
    setting_type = setting["type"]
    arrival_change_period = setting["arrival_period"]
    arrival_change_ratio = setting["arrival_ratio"]
    arrival_change_delta = setting["arrival_delta"]
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

    # Record
    first_so = -1
    first_so_l = 0
    first_so_ll = 0
    first_so_nl = 0
    need_so = False
    need_so_time = 0
    need_so_l = 0
    can_si = False
    can_si_time = 0
    can_si_l = 0
    first_si = -1
    first_si_l = 0
    last_y0 = -1000
    def drawGraph():
        import networkx as nx
        fig = plt.figure()#figsize=(3, 2))
        ax = plt.subplot(111)
        G = nx.DiGraph()
        edges = []
        for operator in sorted_operators:
            for x in in_neighbors[operator]:
                edges += [(x, operator)]
        G.add_edges_from(edges)
        for layer, nodes in enumerate(nx.topological_generations(G)):
            # `multipartite_layout` expects the layer as a node attribute, so add the
            # numeric layer value as a node attribute
            for node in nodes:
                G.nodes[node]["layer"] = layer

        # Compute the multipartite_layout using the "layer" node attribute
        pos = nx.multipartite_layout(G, subset_key="layer")
        nx.draw_networkx(G, pos=pos, ax=ax, node_size=1600, font_size=20)
        import os
        if not os.path.exists(outputDir + setting_name):
            os.makedirs(outputDir + setting_name)
        plt.savefig(outputDir + setting_name + "/" + "graph.png", bbox_inches='tight')
        plt.close(fig)

    drawGraph()
    new_backlogs = {x:y.copy() for x, y in init_backlog.items()}
    new_arrivals = {x:y.copy() for x, y in init_arrival.items()}
    task_backlog_increments = {operator: {task: 0.0 for task in mapping} for operator, mapping in init_service.items()}
    x = []
    y = []
    for emulate_time in range(0, emulation_times):
        for operator in init_backlog.keys():
            if operator not in arrival:
                arrival[operator] = {}
                service[operator] = {}
                backlog[operator] = {}
            new_arrivals[operator] = [get_arrival_rate(init_arrival[operator][key], arrival_change_period, arrival_change_ratio, arrival_change_delta, setting_type, emulate_time * emulation_epoch) for key in range(0, len(new_arrivals[operator]))]
            for task in init_service[operator].keys():
                # Integrate the arrival rate over time to compute backlog
                for t in range(emulate_time * emulation_epoch, (emulate_time + 1) * emulation_epoch, 10):
                    current_arrival_rate = sum([
                        get_arrival_rate(init_arrival[operator][key], arrival_change_period, arrival_change_ratio, arrival_change_delta, setting_type, t)
                        for key in config[operator][task]
                    ])
                    # Add to backlog based on difference between arrival and service rate during this small time epoch
                    task_backlog_increments[operator][task] += (current_arrival_rate - init_service[operator][task]) * 10
                new_backlog = sum([init_backlog[operator][key] for key in config[operator][task]]) + int(task_backlog_increments[operator][task])
                #new_backlog = sum([init_backlog[operator][key] for key in config[operator][task]]) + int((sum([init_arrival[operator][key] for key in config[operator][task]]) - init_service[operator][task] + 1e-9) * emulate_time * emulation_epoch)
                if new_backlog < 0:
                    new_backlog = 0
                if is_remained_backlog_flag and new_backlog < int(sum([init_arrival[operator][key] for key in config[operator][task]]) * 10 + 1e-9):
                    new_backlog = int(sum([init_arrival[operator][key] for key in config[operator][task]]) * 10 + 1e-9)
                evenly_reduce_backlogs(new_backlogs[operator], new_backlog, config[operator][task])

                # For drawing figure
                if task not in arrival[operator]:
                    arrival[operator][task] = []
                    backlog[operator][task] = []
                    service[operator][task] = []
                arrival[operator][task] += [sum([new_arrivals[operator][key] for key in config[operator][task]])]
                service[operator][task] += [init_service[operator][task]]
                backlog[operator][task] += [new_backlog]
        l0 = estimateEndToEndLatency(0, sorted_operators, in_neighbors, new_backlogs, new_arrivals,
                                      init_service, config, wait_times)
        l1 = estimateEndToEndLatency(emulation_epoch, sorted_operators, in_neighbors, new_backlogs, new_arrivals,
                                      init_service, config, wait_times)
        lT = estimateEndToEndLatency(1000000, sorted_operators, in_neighbors, new_backlogs, new_arrivals,
                                     init_service, config, wait_times)
        x += [emulate_time * emulation_epoch]
        y += [l0]
        if check_scale_type == "scale out":
            if (not need_so) and last_y0 != -1000 and last_y0 < l0 and l0 > latency_bound:
                need_so = True
                need_so_time = emulate_time * emulation_epoch
                need_so_l = l0
            if first_so < 0 and l0 < lT and l1 + latency_spike > latency_bound and emulate_time * emulation_epoch % 100 == 0:
                first_so = emulate_time * emulation_epoch
                first_so_l = l0
                first_so_ll = last_y0
            if first_so >= 0 and first_so == emulate_time * emulation_epoch - 100 and emulate_time * emulation_epoch % 100 == 0:
                first_so_nl = l0
        elif check_scale_type == "scale in":
            if (not can_si) and last_y0 != -1000 and last_y0 >= l0 and l0 + latency_spike < latency_bound:
                can_si = True
                can_si_time = emulate_time * emulation_epoch
                can_si_l = l0
            if can_si and l0 > latency_bound:
                can_si = False
            if first_si < 0 and l0 >= lT and l0 + latency_spike < latency_bound and emulate_time * emulation_epoch % 100 == 0:
                first_si = emulate_time * emulation_epoch
                first_si_l = l0
        last_y0 = l0
    fig = plt.figure(figsize=(5, 3))
    # Have a look at the colormaps here and decide which one you'd like:
    # http://matplotlib.org/1.2.1/examples/pylab_examples/show_colormaps.html
    num_plots = emulation_times
    #colormap = plt.cm.gist_ncar
    #plt.gca().set_prop_cycle(plt.cycler('color', plt.cm.jet(np.linspace(0, 1, num_plots))))
    plt.plot(x, y, '-', color='blue', linewidth=2)
    plt.plot([0, emulation_times * emulation_epoch + time_times * time_epoch], [latency_bound, latency_bound], '--', color='red', linewidth=1)
    legend = ["latency curve", "limit"]
    if check_scale_type == "scale out":
        if need_so:
            legend += ["violation point"]
            plt.plot([need_so_time], [need_so_l], 's', color='red', markersize=6)
        if first_so >= 0:
            legend += ["decision time", "add scaling spike", "decision next epoch", "add scaling spike"]
            plt.plot([first_so], [first_so_l], 'd', color='blue', markersize=6)
            plt.plot([first_so], [first_so_l + latency_spike], 's', color='blue', markersize=6)
            plt.plot([first_so + 100], [first_so_nl], 'd', color='orange', markersize=6)
            plt.plot([first_so + 100], [first_so_nl + latency_spike], 's', color='orange', markersize=6)
            plt.plot([first_so, first_so], [first_so_l, first_so_l + latency_spike], '-', color='black', linewidth=1)
            plt.plot([first_so + 100, first_so + 100], [first_so_nl, first_so_nl + latency_spike], '-', color='black', linewidth=1)
    elif check_scale_type == "scale in":
        if can_si:
            legend += ["violation point"]
            plt.plot([can_si_time], [can_si_l], 's', color='red', markersize=4)
        if first_si >= 0:
            legend += ["decision made", "latency + spike"]
            plt.plot([first_si], [first_si_l], 'd', color='blue', markersize=4)
            plt.plot([first_si], [first_si_l + latency_spike], 'd', color='black', markersize=4)
            plt.plot([first_si, first_si], [first_si_l, first_si_l + latency_spike], '-', color='black', linewidth=1)
    plt.ylim(0, 10000)
    plt.legend(legend, ncol=2, loc='upper left')
    plt.grid(True)
    axes = plt.gca()
    axes.set_xlim(0, (emulation_times - 1) * emulation_epoch + time_times * time_epoch)
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
        fig, axs = plt.subplots(len(sorted_operators), task_column, figsize=(7, 4), layout='constrained',sharex=True, sharey=True)
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
                # legend1 = ["arrival rate", "service rate", "strategy scale point"]
                # legend2 = ["backlog"]

                ax1.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch), [x * 1000 for x in arrival[operator][task]], label="arrival rate", color="red", linewidth=2)
                ax1.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch), [x * 1000 for x in service[operator][task]], label="service rate", color="blue", linewidth=2)
                if check_scale_type == "scale out":
                    if first_so >= 0:
                        ax1.plot([first_so, first_so], [0, 10000], '--', label="strategy scale point",  color='red', markersize=4, linewidth=2)
                elif check_scale_type == "scale in":
                    if first_si >= 0:
                        ax1.plot([first_si, first_si], [0, 10000], '--', label="strategy scale point", color='red', markersize=4, linewidth=2)
                ax2.plot(np.arange(0, emulation_times * emulation_epoch, emulation_epoch), backlog[operator][task], label="backlog", color="black", linewidth=2)
                if index == 0 and sec_index == 0:
                    lines_labels = [ax1.get_legend_handles_labels()]
                    lines_labels2 = [ax2.get_legend_handles_labels()]
                    #ax1.legend(legend1, loc="upper left", ncol=3, )
                    #ax2.legend(legend2, loc="lower right")

                ax1.set_title(task)
                #ax1.set_ylabel(operator)
                ax1.set_xlim(0, (emulation_times - 1) * emulation_epoch + time_times * time_epoch)
                #ax1.set_xlabel(task)
                # ax1.set_xticks(np.arange(0, emulation_times * emulation_epoch + time_times * time_epoch, 2000.0))
                ax1.set_ylim(0, 2000)
                ax2.set_ylim(0, 5000)

                # if(operator == "op4"):
                #     ax2.set_ylim(0, 100)
                # else:
                #     ax2.set_ylim(0, 4)
        lines, labels = [sum(lol, []) for lol in zip(*lines_labels)]
        lines2, labels2 = [sum(lol, []) for lol in zip(*lines_labels2)]
        plt.tick_params(labelcolor='none', which='both', top=False, bottom=False, left=False, right=False)
        fig.supxlabel('Time')
        fig.supylabel('Rate/Backlog')
        fig.legend(lines + lines2, labels + labels2, loc='upper center', bbox_to_anchor=(0.5, 1.07), ncol=4)
        import os
        if not os.path.exists(outputDir + setting_name):
            os.makedirs(outputDir + setting_name)
        plt.savefig(outputDir + setting_name + "/" + "LEM_" + "metrics" + '.png', bbox_inches='tight')
        plt.close(fig)

    if check_scale_type == "scale out":
        if not need_so and first_so >= 0:
            return 1
        if need_so and first_so < 0:
            return 2
        if need_so and need_so_time < first_so:
            return 3
    elif check_scale_type == "scale in":
        if not can_si and first_si >= 0:
            return 1
        if can_si and first_si < 0:
            return 2
        if can_si and can_si_time > first_si:
            return 3
    return 0


def emulateMoveConfiguration(setting, flags):
    def moveOutBottleneckKey(sorted_operators: list[str], in_neighbors: dict[str, list[str]],
                             init_backlog: dict[str, list[float]], init_arrival: dict[str, list[float]],
                             new_service: dict[str, dict[str, float]], current_config: dict[str, dict[str, list[int]]],
                             wait_times: dict[str, dict[str, float]]) -> bool:
        bestOperator = ""
        bestKey = -1
        bestl = 1000000000.0
        for operator in sorted_operators:
            new_task = operator + "_t" + str(len(current_config[operator].keys()) + 1)
            current_config[operator][new_task] = []
            new_service[operator][new_task] = sum(new_service[operator].values()) / len(new_service[operator].keys())
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
        new_service[bestOperator][new_task] = sum(new_service[bestOperator].values()) / len(
            new_service[bestOperator].keys())
        return True
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
    flags["emulation_epoch"] = 100  # ms
    flags["emulation_times"] = 300
    flags["latency_spike"] = 500
    flags["latency_bound"] = 2000
    flags["max_operator_num"] = 4
    flags["max_task_num"] = 3
    flags["graph_fixed"] = False
    flags["max_key_num"] = 3

    flags["rate_flag"] = True
    flags["arrival_ranges"] = [[250, 500], [250, 500], [501, 800], [501, 800], [801, 950], [801, 950], [951, 1050], [1050, 1250]]
    flags["backlog_ranges"] = [[0, 10], [11, 200], [201, 1000], [1001, 2000]]
    flags["service_ranges"] = [[950, 1050]]
    flags["arrival_period_range"] = [[2500, 5000], [5000, 7500], [7500, 12500], [12500, 15000]]
    flags["arrival_ratio_range"] = [[10, 15], [15, 20], [20, 25], [25, 30], [30, 50]]
    flags["arrival_delta_range"] = [[0, 1000]]

    print("Start scale out timely test...")
    late_scaleout = {i:[] for i in range(0, 4)}
    miss_scaleout = {i:[] for i in range(0, 4)}
    no_needscaleout = {i:[] for i in range(0, 4)}

    ## Scale out
    flags["scale_type"] = "scale out"
    flags["graph_fixed"] = True
    flags["emulation_times"] = 500
    flags["emulation_epoch"] = 10
    flags["time_times"] = 0
    # One dimension test
    # flags["backlog_ranges"] = [[0, 0]]
    # flags["service_ranges"] = [[1000, 1000]]
    # flags["arrival_ranges"] = [[501, 800], [951, 1050]]
    # flags["arrival_period_range"] = [[5000, 5000]]
    # flags["arrival_ratio_range"] = [[25, 25]]
    # flags["arrival_delta_range"] = [[0, 0]]
    # flags["type"] = 0
    for type in range(0, 4):
        for case in range(0, 3):
            print("Test so_one_"+ str(type) + "_" + str(case) + "...")
            setting = getManualSetting("so_one_" + str(type) + "_" + str(case))
            ret = emulateOnSetting(setting, flags)

    flags["is_remained_backlog_flag"] = True  # False
    flags["time_epoch"] = 1  # ms
    flags["time_times"] = 0  # 1000
    flags["emulation_epoch"] = 100  # ms
    flags["emulation_times"] = 300
    flags["latency_spike"] = 500
    flags["latency_bound"] = 2000
    flags["max_operator_num"] = 4
    flags["max_task_num"] = 3
    flags["graph_fixed"] = False
    flags["max_key_num"] = 3

    flags["rate_flag"] = True
    flags["arrival_ranges"] = [[250, 500], [250, 500], [501, 800], [501, 800], [801, 950], [801, 950], [951, 1050],
                               [1050, 1250]]
    flags["backlog_ranges"] = [[0, 10], [11, 200], [201, 1000], [1001, 2000]]
    flags["service_ranges"] = [[950, 1050]]
    flags["arrival_period_range"] = [[2500, 5000], [5000, 7500], [7500, 12500], [12500, 15000]]
    flags["arrival_ratio_range"] = [[10, 15], [15, 20], [20, 25], [25, 30], [30, 50]]
    flags["arrival_delta_range"] = [[0, 1000]]
    # setting_num = 100
    # for i in range(0, setting_num):
    #     print("Start scale out setting " + str(i))
    #     flags["scale_type"] = "scale out"
    #     flags["type"] = i % 4
    #     setting = generateSetting("so_set_" + str(i), flags)
    #     #    setting = getManualSetting("special")
    #     ret = emulateOnSetting(setting, flags)
    #     if ret == 1:
    #         no_needscaleout[flags["type"]] += [i]
    #     elif ret == 2:
    #         miss_scaleout[flags["type"]] += [i]
    #     elif ret == 3:
    #         late_scaleout[flags["type"]] += [i]
    # print("OK setting: " + str(setting_num - sum([len(x) for x in late_scaleout.values()]) - sum([len(x) for x in miss_scaleout.values()]) - sum([len(x) for x in no_needscaleout.values()])) + "/" + str(setting_num))
    # print("Miss scale out: " + str([str(len(y)) for x, y in miss_scaleout.items()]) + " , " + str(miss_scaleout.values()))
    # print("Late scale out: " + str([str(len(y)) for x, y in late_scaleout.items()]) + " , " + str(late_scaleout.values()))
    # print("Unnecessary scale out: " + str([str(len(y)) for x, y in no_needscaleout.items()]) + " , " + str(no_needscaleout.values()))

    # print("Start scale in timely test...")
    # early_scalein = {i: [] for i in range(0, 4)}
    # miss_scalein = {i: [] for i in range(0, 4)}
    # cannot_scalein = {i: [] for i in range(0, 4)}
    #
    # setting_num = 100
    # for i in range(0, setting_num):
    #     print("Start scale in setting " + str(i))
    #     flags["scale_type"] = "scale in"
    #     flags["type"] = i % 4
    #     setting = generateSetting("si_set_" + str(i), flags)
    #     #    setting = getManualSetting("special")
    #     ret = emulateOnSetting(setting, flags)
    #     if ret == 1:
    #         cannot_scalein[flags["type"]] += [i]
    #     elif ret == 2:
    #         miss_scalein[flags["type"]] += [i]
    #     elif ret == 3:
    #         early_scalein[flags["type"]] += [i]
    # print("OK setting: " + str(setting_num - sum([len(x) for x in cannot_scalein.values()]) - sum(
    #     [len(x) for x in miss_scalein.values()]) - sum([len(x) for x in early_scalein.values()])) + "/" + str(
    #     setting_num))
    # print("Cannot scale in: " + str([str(len(y)) for x, y in cannot_scalein.items()]) + " , " + str(
    #     cannot_scalein.values()))
    # print(
    #     "Miss scale in: " + str([str(len(y)) for x, y in miss_scalein.items()]) + " , " + str(miss_scalein.values()))
    # print(
    #     "Early scale in: " + str([str(len(y)) for x, y in early_scalein.items()]) + " , " + str(early_scalein.values()))


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
        setting = getManualSettingOld(setting_name)
        ret = emulateMoveTime(setting, flags)

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
        setting = getManualSettingOld(setting_name)
        emulateMoveConfiguration(setting, flags)

#analyzeLEM_time()
#analyzeLEM_Configuration()
testScalingDecisionStrategy()