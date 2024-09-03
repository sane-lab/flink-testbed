import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime


def parseConfig(input_string:str) -> dict[str:dict[str:list[int]]]:
    input_string = input_string.strip()
    input_string = input_string.replace(" ", "")
    input_string = input_string.replace("=", ":")
    # Remove outer curly braces
    input_string = input_string.strip('{}')
    # Split into individual key-value pairs
    import re
    key_value_pairs = re.split(r'},\s*(?=[^}]*:)', input_string)

    result = {}

    for pair in key_value_pairs:
        # Ensure we capture the last closing brace
        if not pair.endswith('}'):
            pair += '}'

        # Split the outer key and the inner dictionary
        outer_key, inner_dict_str = pair.split(':{', 1)

        # Clean and parse the inner dictionary
        inner_dict_str = '{' + inner_dict_str
        inner_dict_str = re.sub(r'(\w+):\[(.*?)\]', r'"\1":[\2]', inner_dict_str)
        inner_dict = eval(inner_dict_str)

        # Convert list elements to integers
        for k, v in inner_dict.items():
            inner_dict[k] = list(map(int, v))

        # Add to the result dictionary
        result[outer_key] = inner_dict

    return result

def parseBacklog(input_string) -> dict[str:int]:
    input_string.strip()
    input_string = input_string.replace(" ", "")
    input_string = input_string.replace("=", ":")
    # Remove outer curly braces
    input_string = input_string.strip('{}')

    # Split into individual key-value pairs
    key_value_pairs = input_string.split(',')

    result = {}

    for pair in key_value_pairs:
        # Split the key and value
        key, value = pair.split(':')

        # Convert value to float and then to int
        result[key] = int(float(value))

    return result
def parseArrivalOrService(input_string) -> dict[str: float]:
    input_string.strip()
    input_string = input_string.replace(" ", "")
    input_string = input_string.replace("=", ":")
    # Remove outer curly braces
    input_string = input_string.strip('{}')

    # Split into individual key-value pairs
    key_value_pairs = input_string.split(',')

    result = {}

    for pair in key_value_pairs:
        # Split the key and value
        key, value = pair.split(':')

        # Convert value to float and then to int
        result[key] = float(value)

    return result
def parseKeyBacklog(input_string) -> dict[str:dict[int,int]]:
    input_string.strip()
    input_string = input_string.replace(" ", "")
    input_string = input_string.replace("=", ":")
    # Remove outer curly braces
    input_string = input_string.strip('{}')
    import re
    # Split into outer key-value pairs
    outer_pairs = re.findall(r'([^\s]+)=\{([^\}]+)\}', input_string)

    result = {}

    for outer_key, inner_str in outer_pairs:
        # Process the inner string into a dict[int: int]
        inner_dict = {}
        inner_pairs = inner_str.split(', ')
        for pair in inner_pairs:
            inner_key, value = pair.split('=')
            inner_dict[int(inner_key)] = int(float(value))

        result[outer_key] = inner_dict

    return result
def parseKeyArrival(input_string) -> dict[str:dict[int,float]]:
    # Step 1: Clean the string by replacing "=" with ":"
    cleaned_str = input_string.replace("=", ":").replace(" ", "")

    # Step 2: Replace curly braces with valid Python syntax
    cleaned_str = cleaned_str.replace("{", "{'").replace(":", "':").replace(",", ",'").replace("={", "':{")
    import ast
    # Step 3: Use ast.literal_eval to safely evaluate the string into a Python dictionary
    parsed_dict = ast.literal_eval(cleaned_str)

    # Step 4: Convert the inner dictionary keys from strings to integers
    for key, inner_dict in parsed_dict.items():
        parsed_dict[key] = {int(k): float(v) for k, v in inner_dict.items()}

    return parsed_dict

def parseKeys(input_string:str) -> list[int]:
    # Remove the square brackets and split the string by commas
    return [int(x) for x in input_string.strip('[]').replace(" ", "").split(',')]
def parseMapping(input_string:str) -> dict[str, list[int]]:
    input_string.strip()
    input_string = input_string.replace(" ", "")
    input_string = input_string.replace("=", ":")

    # Remove the outer curly braces and split by '}, '
    dict_items = input_string.strip('{}').split('],')

    # Initialize the result dictionary
    result = {}

    for item in dict_items:
        # Split each item by the first ':'
        key, value_str = item.split(':[', 1)
        # Remove any extra characters and convert the value to a list of integers
        value = [int(x) for x in value_str.strip('[]{}').split(',')]
        # Add the key-value pair to the dictionary
        result[key] = value

    return result

# For each scaling, retrieve:
# 1) the metrics before it happens,
# 2) the estimated latency related info before it happens.

def read_scaling_from_output(outputPath:str) -> list[dict[str:any]]:

    counter = 0
    scaling_infos = []
    initial_time  = -1
    configuration = {}
    task_backlogs = {}
    task_arrivals = {}
    task_services = {}
    key_arrivals = {}
    key_backlogs = {}
    scaling_out_times = 0
    latency_limit = -1
    with open(outputPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if (initial_time == -1 and len(split) >= 4 and split[0] == "+++" and split[2] == "time:"):
                time = int(split[3])
                initial_time = time
            if (latency_limit == -1 and len(split) >= 9 and split[1] == "[CONTROL]" and split[6] == "current" and split[7] == "limit:"):
                latency_limit = int(split[8].rstrip())
            if (len(split) >= 16 and split[1] == "[MODEL]" and split[6] == "cur_ete_l:" and split[8] == "n_epoch_l:" and split[10] == "trend_l:"):
                current_latency = float(split[7])
                next_epoch_latency = float(split[9])
                far_future_latency = float(split[11])
                current_spike = float(split[13]) - current_latency
            if (len(split) >= 8 and split[1] == "[CONTROL]" and split[6] == "current" and split[7] == "config:"):
                configuration = parseConfig("".join(split[8:]))
            if (len(split) >= 6 and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "backlog:"):
                task_backlogs = parseBacklog("".join(split[6:]))
            if (len(split) >= 6 and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "arrivalRate:"):
                task_arrivals = parseArrivalOrService("".join(split[6:]))
            if (len(split) >= 6 and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "serviceRate:"):
                task_services = parseArrivalOrService("".join(split[6:]))

            if (len(split) >= 6 and split[1] == "[METRICS]" and split[4] == "key" and split[5] == "arrivalRate:"):
                key_arrivals = parseKeyArrival("".join(split[6:]))
            if (len(split) >= 6 and split[1] == "[METRICS]" and split[4] == "key" and split[5] == "backlog:"):
                key_backlogs = parseKeyArrival("".join(split[6:]))

            if (len(split) >= 10 and split[1] == "[CONTROL]" and split[6] == "decides" and split[7] == "to" and split[8] == "scale" and split[9] == "out."):
                # Scale out timing
                time = int(split[3])
                scaling_out_times += 1
                scaling_info = {
                    "time": time,
                    "configuration": configuration,
                    "task_backlog": task_backlogs,
                    "task_arrival": task_arrivals,
                    "task_service": task_services,
                    "key_arrival": key_arrivals,
                    "key_backlog": key_backlogs,
                    "latency_limit": latency_limit,
                    "current_latency" : current_latency,
                    "next_epoch_latency": next_epoch_latency,
                    "far_future_latency": far_future_latency,
                    "current_spike": current_spike,
                }
                scaling_infos += [scaling_info]
    return scaling_infos

def read_scaling_from_log(logPath:str) -> []:
    counter = 0
    scaling_logs = []
    scaling_raw_logs = []

    algorithm_raw_log = []
    algorithm_log = []
    number_of_keys_out_per_task_phase1 = {}
    number_of_keys_in_per_task_phase2 = {}
    key_move_phase1 = [[], []] # stage 1 and stage 2, each element [key, source task, l0/lT], after each step
    key_move_phase2 = [] # each n, [[key, # of task reallocate tries, l0 after move], ...]
    key_reallocate_tries = 0
    start_time = -1
    with open(logPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if (start_time == -1 and len(split) >= 6 and split[3] == "org.apache.flink.runtime.rescale.streamsluice.PipelineLatencyGuarantor"):
                timestamp_str = split[0] + " " + split[1]
                dt_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                # Convert the datetime object to system milliseconds
                milliseconds = int(dt_obj.timestamp() * 1000)
                start_time = milliseconds
            if (len(split) >= 4 and split[3] == "org.apache.flink.runtime.rescale.streamsluice.algorithms.ScaleOut_Sluice" and split[6] == "[ALGO]"):
                timestamp_str = split[0] + " " + split[1]
                dt_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                # Convert the datetime object to system milliseconds
                time = int(dt_obj.timestamp() * 1000)
                algorithm_raw_log += [str(time) + " " + " ".join(split[7:])]
                if (len(split) >= 12 and split[7] == "condition1" and split[8] == "(0," and split[9] == "L):"):
                    l0 = float(split[10].rstrip().replace(",", ""))
                    lb = float(split[11].rstrip())
                    log = "Time: " + str(time) + " condition1 l0=" + str(l0) + ", lb=" + str(lb)
                    algorithm_log += [log]
                    key_reallocate_tries += 1
                if (len(split) >= 12 and split[7] == "Heaviest" and split[8] == "task:"):
                    heaviest_task = split[9]
                    heaviest_key = split[11]
                    number_of_keys_out_per_task_phase1[heaviest_task] = (number_of_keys_out_per_task_phase1[heaviest_task] + 1) if heaviest_task in number_of_keys_out_per_task_phase1 else 1
                    key_move_phase1[0] += [[int(heaviest_key), heaviest_task, l0]]
                    log = "Time: " + str(time) + " heaviest task=" + heaviest_task + " key=" + heaviest_key
                    algorithm_log += [log]
                if (len(split) >= 10 and split[7] == "First" and split[8] == "condition" and split[9] == "reached."):
                    log = "Time: " + str(time) + " first condition reached."
                    algorithm_log += [log]
                if (len(split) >= 10 and split[7] == "Worst" and split[8] == "task" and split[10] == "has" and split[11] == "less" and split[12] == "than" and split[16] == "cannot" and split[17] == "move"):
                    heaviest_task = split[9]
                    log = "Time: " + str(time) + " heaviest task=" + heaviest_task + " only 1 key"
                    key_move_phase1[0] += [[-1, heaviest_task, l0]]
                    algorithm_log += [log]
                if (len(split) >= 14 and split[7] == "condition2" and split[8] == "(0," and split[9] == "L)"):
                    l0 = float(split[12].rstrip().replace(",", ""))
                    lT = float(split[13].rstrip())
                    log = "Time: " + str(time) + " condition2 l0=" + str(l0) + ", lT=" + str(lT)
                    algorithm_log += [log]
                if (len(split) >= 12 and split[7] == "Heaviest" and split[8] == "rate" and split[9] == "task:"):
                    heaviest_task = split[10]
                    heaviest_key = split[12]
                    key_move_phase1[1] += [[heaviest_key, heaviest_task, lT]]
                    number_of_keys_out_per_task_phase1[heaviest_task] = (number_of_keys_out_per_task_phase1[heaviest_task] + 1) if heaviest_task in number_of_keys_out_per_task_phase1 else 1
                    log = "Time: " + str(time) + " heaviest rate task=" + heaviest_task + " key=" + heaviest_key
                    algorithm_log += [log]
                if (len(split) >= 10 and split[7] == "Second" and split[8] == "condition" and split[9] == "reached."):
                    log = "Time: " + str(time) + " second condition reached."
                    algorithm_log += [log]


                if (len(split) >= 10 and split[7] == "Phase" and split[8] == "2" and split[9] == "start," and split[10] == "move" and split[11] == "keys:"):
                    moved_keys = []
                    for index in range(12, len(split)):
                        moved_keys += [int(split[index].replace(",", "").replace("[", "").replace("]", ""))]
                        if split[index].find("],") != -1:
                            break
                    log = "Time: " + str(time) + " moved keys: " + str(moved_keys)
                    algorithm_log += [log]


                # Sort
                if (len(split) >= 10 and split[7] == "base" and split[9] == "sorted" and split[10] == "keys:"):
                    sorted_keys = parseKeys("".join(split[11:]))
                    log = "Time: " + str(time) + " sorted keys: " + str(sorted_keys)
                    algorithm_log += [log]

                # Phase 2
                if (len(split) >= 10 and split[7] == "base" and split[9] == "try" and split[10].startswith("n=")):
                    number_of_keys_in_per_task_phase2 = {}
                    n = int(split[10].strip()[2:])
                    key_move_phase2 += [[]]
                    log = "Time: " + str(time) + " try n=" + str(n)
                    algorithm_log += [log]
                    key_reallocate_tries = 0

                if (len(split) >= 10 and split[8] == "cannot" and split[9] == "reallocate," and split[10] == "skip"):
                    key = int(split[7])
                    log = "Time: " + str(time) + " cannot reallocate key " + str(key)
                    algorithm_log += [log]
                if (len(split) >= 10 and split[8] == "to" and split[9] == "task" and split[11] == "map:"):
                    key = int(split[7])
                    task = split[10]
                    key_move_phase2[-1] += [[key, key_reallocate_tries, l0]]
                    number_of_keys_in_per_task_phase2[task] = (number_of_keys_in_per_task_phase2[task] + 1) if task in number_of_keys_in_per_task_phase2 else 1
                    log = "Time: " + str(time) + " reallocate key " + str(key) + " to " + str(task)
                    algorithm_log += [log]
                    key_reallocate_tries = 0
                if (len(split) >= 10 and split[7] == "Base" and split[8] == "key" and split[10] == "parallelism" and split[12] == "map:"):
                    key = int(split[9])
                    n = int(split[11])
                    log = "Time: " + str(time) + " base key=" + str(key) + " n=" + str(n) + " mapping found."
                    algorithm_log += [log]
                if (len(split) >= 10 and split[7] == "Best" and split[8] == "Mapping:"):
                    mapping = parseMapping("".join(split[9:]))
                    log = "Time: " + str(time) + " best mapping=" + str(mapping)
                    algorithm_log += [log]
                    scaling_logs += [{"moved_keys": moved_keys,
                                      "best_mapping": mapping,
                                      "number_of_keys_out_per_task_phase1": number_of_keys_out_per_task_phase1,
                                      "number_of_keys_in_per_task_phase2": number_of_keys_in_per_task_phase2,
                                      "key_move_phase1": key_move_phase1,
                                      "key_move_phase2": key_move_phase2,
                                      "log": algorithm_log,
                                      "raw_log": algorithm_raw_log}]
                    algorithm_log = []
                    algorithm_raw_log = []
                    number_of_keys_out_per_task_phase1 = {}
                    number_of_keys_in_per_task_phase2 = {}
                    key_move_phase1 = [[], []]
                    key_move_phase2 = []


    return scaling_logs

def extract_scaling_info(rawDir, expName, outputDir):
    streamsluiceOutput = "flink-samza-standalonesession-0-eagle-sane.out"
    streamsluiceLog = "flink-samza-standalonesession-0-eagle-sane.log"

    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".log"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("standalonesession") == 1:
                streamsluiceLog = file
    streamSluiceLogPath = rawDir + expName + "/" + streamsluiceLog
    print("Reading streamsluice log:" + streamSluiceLogPath)
    scaling_logs = read_scaling_from_log(streamSluiceLogPath)

    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("standalonesession") == 1:
                streamsluiceOutput = file
    streamSluiceOutputPath = rawDir + expName + "/" + streamsluiceOutput
    print("Reading streamsluice output:" + streamSluiceOutputPath)
    scaling_infos = read_scaling_from_output(streamSluiceOutputPath)

    import os
    if not os.path.exists(outputDir + "/scaling_info/"):
        os.makedirs(outputDir + "/scaling_info/")
    for index in range(0, min(len(scaling_infos), len(scaling_logs))):
        scaling_info = scaling_infos[index]
        scaling_log = scaling_logs[index]["log"]
        after_scaling_mapping = scaling_logs[index]["best_mapping"]
        number_of_keys_out_per_task_phase1 = scaling_logs[index]["number_of_keys_out_per_task_phase1"]
        number_of_keys_in_per_task_phase2 = scaling_logs[index]["number_of_keys_in_per_task_phase2"]
        bottleneck = next(iter(after_scaling_mapping)).split("_")[0]
        key_arrival = scaling_info["key_arrival"]
        key_backlog = scaling_info["key_backlog"]
        missing_tasks = [task for task in list(scaling_info["task_arrival"].keys() - after_scaling_mapping.keys()) if task.find(bottleneck) >= 0]

        new_tasks_list = sorted(list([task for task in scaling_info["task_arrival"].keys() if task.find(bottleneck) == -1]) + list(after_scaling_mapping.keys()))

        after_scaling_task_arrival = {task: (scaling_info["task_arrival"][task] if task.find(bottleneck) == -1 else (sum([key_arrival[bottleneck][k] for k in after_scaling_mapping[task]])))
                                      for task in new_tasks_list}
        after_scaling_task_backlog = {task: (scaling_info["task_backlog"][task] if task.find(bottleneck) == -1 else (sum([key_backlog[bottleneck][k] for k in after_scaling_mapping[task]])))
                                      for task in new_tasks_list}

        # Bottleneck info only
        scaling_info["key_arrival"] = {key: value for key, value in scaling_info["key_arrival"].items() if key == bottleneck}
        scaling_info["key_backlog"] = {key: value for key, value in scaling_info["key_backlog"].items() if key == bottleneck}
        scaling_info["task_arrival"] = {key: value for key, value in scaling_info["task_arrival"].items() if key.find(bottleneck) >= 0}
        scaling_info["task_backlog"] = {key: value for key, value in scaling_info["task_backlog"].items() if key.find(bottleneck) >= 0}
        scaling_info["task_service"] = {key: value for key, value in scaling_info["task_service"].items() if key.find(bottleneck) >= 0}
        scaling_info["configuration"] = {key: value for key, value in scaling_info["configuration"].items() if key.find(bottleneck) >= 0}
        after_scaling_task_arrival = {key: value for key, value in after_scaling_task_arrival.items() if key.find(bottleneck) >= 0}
        after_scaling_task_backlog = {key: value for key, value in after_scaling_task_backlog.items() if key.find(bottleneck) >= 0}
        initial_configuration = scaling_info["configuration"]

        # Bottleneck algorithm detail
        key_move_phase1 = scaling_logs[index]["key_move_phase1"]
        key_move_phase2 = scaling_logs[index]["key_move_phase2"]
        task_key_counts_per_step_phase1 = []
        task_key_counts_per_step_phase1 += [{
            task: len(keys) for task, keys in initial_configuration[bottleneck].items()
        }]
        backlog_sorted_keys = {key: value for key, value in scaling_info["key_backlog"][bottleneck].items() if value > 4}
        print(len(backlog_sorted_keys))

        with open(outputDir + "/scaling_info/" + str(index + 1) + ".txt", "w") as file:
            file.write("-------------------------------Info When Trigger Scaling-------------------------------\n")
            for key, value in scaling_info.items():
                file.write(str(key) + "\n")
                file.write(str(value) + "\n\n")

            file.write("-------------------------------Info after Scaling-------------------------------\n")
            file.write("task_backlog\n" + str(after_scaling_task_backlog) + "\n")
            file.write("task_arrival\n" + str(after_scaling_task_arrival) + "\n")
            file.write("missing tasks:\n" + str(missing_tasks) + "\n\n")

            file.write("-------------------------------Algorithm Info Trigger Scaling-------------------------------\n")
            for log in scaling_log:
                file.write(log + "\n")
            file.write("keys_out_phase1\n" + str(number_of_keys_out_per_task_phase1) + "\n")
            file.write("keys_in_phase2\n" + str(number_of_keys_in_per_task_phase2) + "\n\n")

            file.write("-------------------------------Algorithm Analysis------------------------------------\n")
            file.write("backlog >0 keys: \n" + str(backlog_sorted_keys) + "\n\n")
            file.write("Phase 1 stage 1:\n")
            # stage 1
            file.write("# of key per task: " + str(task_key_counts_per_step_phase1[0]) + "\n")
            for step in key_move_phase1[0]:
                if step[0] == -1:
                    file.write("heaviest task: " + str(step[1]) + " but only 1 key remained" + "\n")
                else:
                    new_task_key_counts = {task: count if task != step[1] else (count - 1) for task, count in
                                           task_key_counts_per_step_phase1[-1].items()}
                    file.write("move key from task: " + str(step[1]) + ", l0=" + str(step[2]) + "\n")
                    file.write("# of key per task: " + str(new_task_key_counts) + "\n")
                    task_key_counts_per_step_phase1 += [new_task_key_counts]
            # stage 2
            file.write("\nPhase 1 stage 2:\n")
            for step in key_move_phase1[1]:
                if step[0] == -1:
                    file.write("heaviest task: " + str(step[1]) + " but only 1 key remained" + "\n")
                else:
                    new_task_key_counts = {task: count if task != step[1] else (count - 1) for task, count in
                                       task_key_counts_per_step_phase1[-1].items()}
                    file.write("move key " + str(step[0]) + " from task: " + str(step[1]) + "lT" + str(step[2]) + "\n")
                    file.write("# of key per task: " + str(new_task_key_counts) + "\n")
                    task_key_counts_per_step_phase1 += [new_task_key_counts]
            # Phase 2
            file.write("\nPhase 2: \n")
            for n in range(0, len(key_move_phase2)):
                file.write("Try n=" + str(n) + "\n")
                for step in key_move_phase2[n]:
                    key = step[0]
                    reallocate_tries = step[1]
                    l0 = step[2]
                    file.write("try reallocate key: " + str(key) + ", tried " + str(reallocate_tries) + " times, new l0: " + str(l0) + "\n")



rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
expName = "system-streamsluice-streamsluice-true-true-false-when-mixed-1split2join1-520-6000-3000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-6-510-5000-750-3000-100-1-true-1"
extract_scaling_info(rawDir, expName, outputDir + expName + "/")


