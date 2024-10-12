import matplotlib.pyplot as plt
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime

SMALL_SIZE = 25
MEDIUM_SIZE = 30
BIGGER_SIZE = 35

plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=SMALL_SIZE)    # font-size of the tick labels
plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title
MARKERSIZE=4
LINEWIDTH=3

MAXTASKPERFIG=5

# Function to convert time into a relative value from the first timestamp
def convert_to_relative_time(timestamp, initial_timestamp):
    return (timestamp - initial_timestamp) / 1000  # Convert milliseconds to seconds

# Function to parse task metrics (simple dictionaries)
def parse_task_metrics(metrics_str):
    # Replace '=' with ':' to convert to JSON-like format
    metrics_str = metrics_str.replace('=', ':')
    # Add quotes around task keys (e.g., a84740bacf923e828852cc4966f2247c_1 -> "a84740bacf923e828852cc4966f2247c_1")
    metrics_str = re.sub(r'(\w+_\d+)', r'"\1"', metrics_str)
    import json
    return json.loads(metrics_str)

# Function to parse key metrics (nested dictionaries)
def parse_key_metrics(metrics_str):
    # Step 1: Replace `=` with `:` to make it valid Python dictionary syntax
    formatted_str = metrics_str.replace('=', ':')

    # Step 2: Add quotes around the string keys in the outer dictionary
    formatted_str = re.sub(r'(\w+):', r'"\1":', formatted_str)
    import ast
    parsed_dict = ast.literal_eval(formatted_str)
    return parsed_dict

def draw_for_file(rawDir, expName, outputDir):
    # Initialize dictionaries to store data
    task_data = {}
    initialTime = -1
    taskExecutors = [] #"flink-samza-taskexecutor-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("taskexecutor") == 1:
                taskExecutors += [file]
    for taskExecutor in taskExecutors:
        groundTruthPath = rawDir + expName + "/" + taskExecutor
        print("Reading ground truth file:" + groundTruthPath)
        counter = 0
        with open(groundTruthPath) as f:
            lines = f.readlines()
            for i in range(0, len(lines)):
                line = lines[i]
                split = line.rstrip().split()
                counter += 1
                if (counter % 5000 == 0):
                    print("Processed to line:" + str(counter))
                if(split[0] == "GT:"):
                    completedTime = int(split[2].rstrip(","))
                    latency = int(split[3].rstrip(","))
                    arrivedTime = completedTime - latency
                    if (arrivedTime < 0):
                        print("!!!! " + str(i) + "  " + line)
                    if (initialTime == -1 or initialTime > arrivedTime):
                        initialTime = arrivedTime
    print("init time=" + str(initialTime))

    initial_timestamp = initialTime
    streamsluiceOutput = "flink-samza-standalonesession-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("standalonesession") == 1:
                streamsluiceOutput = file
    streamSluiceOutputPath = rawDir + expName + "/" + streamsluiceOutput
    print("Reading streamsluice output:" + streamSluiceOutputPath)
    counter = 0

    # Parse the log file
    with open(streamSluiceOutputPath, 'r') as file:
        for line in file:
            counter += 1
            if counter % 5000 == 0:
                print("processed to line: " + str(counter))

            match_time = re.search(r'\[CONTROL\] time: (\d+)', line)
            if match_time:
                timestamp = int(match_time.group(1))
                if initial_timestamp > timestamp:
                    initial_timestamp = timestamp

                relative_time = convert_to_relative_time(timestamp, initial_timestamp)

            # Parse the task backlog, arrival rate, and service rate
            match_backlog = re.search(r'\[METRICS\] time: .+ task backlog: ({.*})', line)
            match_arrival_rate = re.search(r'\[METRICS\] time: .+ task arrivalRate: ({.*})', line)
            match_service_rate = re.search(r'\[METRICS\] time: .+ task serviceRate: ({.*})', line)

            if match_backlog:
                backlog_data = parse_task_metrics(match_backlog.group(1))
                for task, backlog in backlog_data.items():
                    if task not in task_data:
                        task_data[task] = {'time': [], 'backlog': [], 'arrival_rate': [], 'service_rate': []}
                    task_data[task]['time'].append(relative_time)
                    task_data[task]['backlog'].append(float(backlog))  # Convert to float
            if match_arrival_rate:
                arrival_rate_data = parse_task_metrics(match_arrival_rate.group(1))
                for task, arrival_rate in arrival_rate_data.items():
                    task_data[task]['arrival_rate'].append(float(arrival_rate))

            if match_service_rate:
                service_rate_data = parse_task_metrics(match_service_rate.group(1))
                for task, service_rate in service_rate_data.items():
                    task_data[task]['service_rate'].append(float(service_rate))

    import matplotlib.pyplot as plt

    def draw_metrics_within_time_range(times, arrival_rates, service_rates, backlogs, task_name, start_time, end_time):
        # Filter the data based on the start_time and end_time
        filtered_times = [(t - start_time) for t in times if start_time <= t <= end_time]
        filtered_arrival_rates = [arrival_rates[i] * 1000 for i, t in enumerate(times) if start_time <= t <= end_time]
        filtered_service_rates = [service_rates[i] * 1000 for i, t in enumerate(times) if start_time <= t <= end_time]
        filtered_backlogs = [backlogs[i] for i, t in enumerate(times) if start_time <= t <= end_time]

        # Create the figure and plot the data
        fig, ax1 = plt.subplots(figsize=(12, 5))
        plt.grid(True)
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Rate (Arrival/Service)', color='tab:blue')
        ax1.plot(filtered_times, filtered_arrival_rates, label='Arrival Rate', color='tab:red', linestyle='-')
        ax1.plot(filtered_times, filtered_service_rates, label='Service Rate', color='tab:blue', linestyle='-')
        ax1.tick_params(axis='y')
        ax1.set_xlim(0, end_time - start_time)
        if len(filtered_arrival_rates) == 0 or (max(filtered_arrival_rates) < 5000 and max(filtered_service_rates) < 5000):
            ax1.set_ylim(0, 5000)
        else:
            ax1.set_ylim(0, 20000)
        # Create a second y-axis for backlog
        ax2 = ax1.twinx()
        if len(filtered_backlogs) == 0 or max(filtered_backlogs) < 1000:
            ax2.set_ylim(0, 1000)
        else:
            ax2.set_ylim(0, 10000)
        ax2.set_ylabel('Backlog', color='black')
        ax2.plot(filtered_times, filtered_backlogs, label='Backlog', color='black', linestyle='-')
        ax2.tick_params(axis='y')

        # Add legends and titles
        fig.suptitle(f'Metrics for {task_name}')
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')

        import os

        if not os.path.exists(outputDir + "task/"):
            os.makedirs(outputDir + "task/")

        # plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
        plt.savefig(outputDir + "task/" + task + ".png", bbox_inches='tight')
        plt.close(fig)

    # Plot the task backlog, arrival rate, and service rate curves
    for task, data in task_data.items():
        draw_metrics_within_time_range(data['time'], data['arrival_rate'], data['service_rate'], data['backlog'], task, start_time, start_time + exp_length)

def verify_key_total(rawDir, expName):
    # Initialize variables to store the total task and key metrics
    total_task_backlog = {}
    total_key_backlog = {}
    total_task_arrival_rate = {}
    total_key_arrival_rate = {}
    streamsluiceOutput = "flink-samza-standalonesession-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("standalonesession") == 1:
                streamsluiceOutput = file
    streamSluiceOutputPath = rawDir + expName + "/" + streamsluiceOutput
    print("Reading streamsluice output:" + streamSluiceOutputPath)
    counter = 0
    initial_timestamp = None

    # Parse the log file
    with open(streamSluiceOutputPath, 'r') as file:
        for line in file:
            counter += 1
            if counter % 5000 == 0:
                print("processed to line: " + str(counter))

            match_time = re.search(r'\[CONTROL\] time: (\d+)', line)
            if match_time:
                timestamp = int(match_time.group(1))
                if initial_timestamp is None:
                    initial_timestamp = timestamp
                relative_time = convert_to_relative_time(timestamp, initial_timestamp)

            # Parse task backlog and key backlog
            match_task_backlog = re.search(r'\[METRICS\] time: .+ task backlog: ({.*})', line)
            match_key_backlog = re.search(r'\[METRICS\] time: .+ key backlog: ({.*})', line)
            match_task_arrival_rate = re.search(r'\[METRICS\] time: .+ task arrivalRate: ({.*})', line)
            match_key_arrival_rate = re.search(r'\[METRICS\] time: .+ key arrivalRate: ({.*})', line)



            # Aggregate task backlogs
            if match_task_backlog:
                task_backlog_data = parse_task_metrics(match_task_backlog.group(1))
                total_task_backlog[relative_time] = sum(task_backlog_data.values())

            # Aggregate key backlogs
            if match_key_backlog:
                key_backlog_data = parse_key_metrics(match_key_backlog.group(1))
                key_backlog_total = sum([sum(key_data.values()) for key_data in key_backlog_data.values()])
                total_key_backlog[relative_time] = key_backlog_total

            # Aggregate task arrival rate
            if match_task_arrival_rate:
                task_arrival_data = parse_task_metrics(match_task_arrival_rate.group(1))
                total_task_arrival_rate[relative_time] = sum(task_arrival_data.values())

            # Aggregate key arrival rate
            if match_key_arrival_rate:
                key_arrival_data = parse_key_metrics(match_key_arrival_rate.group(1))
                key_arrival_total = sum([sum(key_data.values()) for key_data in key_arrival_data.values()])
                total_key_arrival_rate[relative_time] = key_arrival_total

    # Validation step: Ensure task and key metrics match
    for time in total_task_backlog:
        task_backlog = total_task_backlog.get(time, 0)
        key_backlog = total_key_backlog.get(time, 0)
        if task_backlog != key_backlog:
            print(f"Backlog mismatch at time {time}: Task Backlog = {task_backlog}, Key Backlog = {key_backlog}")

    for time in total_task_arrival_rate:
        task_arrival = total_task_arrival_rate.get(time, 0)
        key_arrival = total_key_arrival_rate.get(time, 0)
        if abs(task_arrival - key_arrival) > 1e-6:
            print(
                f"Arrival rate mismatch at time {time}: Task Arrival Rate = {task_arrival}, Key Arrival Rate = {key_arrival}")

rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
exps = [
    # "test_metric-streamsluice-ds2-true-false-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-1-50-1-5000-3-600-5000-1000-3000-100-1-false-1",
    #"test_metric1-streamsluice-ds2-true-false-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-1-50-1-5000-3-600-5000-1000-3000-100-1-0-false-1",
    #"test_metric-streamsluice-ds2-false-false-true-false-when-gradient-1op_line-170-4000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-1-50-1-5000-3-600-5000-1000-3000-100-1-false-1",
    # "test_metric-streamsluice-ds2-true-false-true-false-when-linear-1op_line-170-7000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-1-50-1-5000-3-600-5000-1000-3000-100-1-false-1",
    #"test_metric1-streamsluice-ds2-true-false-true-false-when-linear-1op_line-170-7000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-1-50-1-5000-3-600-5000-1000-3000-100-1-0-false-1",
    #"test_metric-streamsluice-ds2-false-false-true-false-when-linear-1op_line-170-7000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-1-50-1-5000-3-600-5000-1000-3000-100-1-false-1",
    # "test_metric-streamsluice-ds2-true-false-true-false-when-gradient-4op_line-170-6000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-1-50-1-5000-3-600-5000-1000-3000-100-1-false-1",
    #"test_metric1-streamsluice-ds2-true-false-true-false-when-gradient-4op_line-170-6000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-1-50-1-5000-3-600-5000-1000-3000-100-1-0-false-1",
    #"test_metric-streamsluice-ds2-false-false-true-false-when-gradient-4op_line-170-6000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-1-50-1-5000-3-600-5000-1000-3000-100-1-false-1",
    # "test_metric-streamsluice-streamsluice-true-false-true-false-when-linear-4op_line-170-8000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-1-50-1-5000-3-600-5000-1000-3000-100-1-true-1",
    # "test_metric-streamsluice-streamsluice-false-false-true-false-when-linear-4op_line-170-8000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-1-50-1-5000-3-600-5000-1000-3000-100-1-true-1",
    # "test_metric-streamsluice-ds2-true-false-true-false-normal-linear-1split2join1-170-4000-4000-4000-1-0.1-1-300-1-5000-2-300-1-5000-3-50-1-5000-6-500-5000-1000-3000-100-1-0.1-false-1",
    # "test_metric-streamsluice-ds2-false-false-true-false-normal-linear-1split2join1-170-4000-4000-4000-1-0.1-1-300-1-5000-2-300-1-5000-3-50-1-5000-6-500-5000-1000-3000-100-1-0.1-false-1",
    # "test_metric-streamsluice-ds2-true-false-true-false-normal-linear-1split2join1-170-4000-4000-4000-1-0.2-1-300-1-5000-2-300-1-5000-3-50-1-5000-6-500-5000-1000-3000-100-1-0.2-false-1",
    # "test_metric-streamsluice-ds2-false-false-true-false-normal-linear-1split2join1-170-4000-4000-4000-1-0.2-1-300-1-5000-2-300-1-5000-3-50-1-5000-6-500-5000-1000-3000-100-1-0.2-false-1",
    #"test_metric1-streamsluice-streamsluice-true-false-true-false-normal-linear-1split2join1-170-4000-4000-4000-1-0.1-1-50-1-5000-2-50-1-5000-3-50-1-5000-6-600-5000-1000-3000-100-1-0.1-true-1",
    #"test_metric-streamsluice-streamsluice-false-false-true-false-normal-linear-1split2join1-170-4000-4000-4000-1-0.1-1-50-1-5000-2-50-1-5000-3-50-1-5000-6-600-5000-1000-3000-100-1-0.1-true-1",
    #"test_metric-streamsluice-streamsluice-true-false-true-false-normal-linear-1split2join1-170-4000-4000-4000-1-0.2-1-50-1-5000-2-50-1-5000-3-50-1-5000-6-600-5000-1000-3000-100-1-0.2-true-1",
    #"test_metric-streamsluice-streamsluice-false-false-true-false-normal-linear-1split2join1-170-4000-4000-4000-1-0.2-1-50-1-5000-2-50-1-5000-3-50-1-5000-6-600-5000-1000-3000-100-1-0.2-true-1",
    "tweet-streamsluice-streamsluice-390-30-1800-1-28-2000-10-500-1-50-1-50-2000-100-true-3-true-1",
]
start_time=0
exp_length=600

for expName in exps:
    #verify_key_total(rawDir, expName)
    draw_for_file(rawDir, expName, outputDir + expName + "/")

