import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

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

def parseBacklog(input_string:str) -> dict[str, dict[str, int]]:
    input_string.strip()
    input_string = input_string.replace(" ", "")
    input_string = input_string.replace("=", ":")
    # Remove outer curly braces
    input_string = input_string.strip('{}')

    # Split into individual key-value pairs
    key_value_pairs = input_string.split(',')

    parsed_dict = {}

    for pair in key_value_pairs:
        # Split the key and value
        key, value = pair.split(':')
        # Convert value to float and then to int
        parsed_dict[key] = float(value)
    from collections import defaultdict
    output_dict = defaultdict(dict)
    for task_name, backlog in parsed_dict.items():
        operator_name = task_name.split('_')[0]
        output_dict[operator_name][task_name] = int(float(backlog))
    output_dict = dict(output_dict)
    return output_dict
def retrieveInitialTime(rawDir, expName) -> int:
    initialTime = -1
    taskExecutors = []  # "flink-samza-taskexecutor-0-eagle-sane.out"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("taskexecutor") == 1:
                taskExecutors += [file]
    fileInitialTimes = {}
    for taskExecutor in taskExecutors:
        groundTruthPath = rawDir + expName + "/" + taskExecutor
        print("Reading ground truth file:" + groundTruthPath)
        fileInitialTime = - 1
        counter = 0
        with open(groundTruthPath) as f:
            lines = f.readlines()
            for i in range(0, len(lines)):
                line = lines[i]
                split = line.rstrip().split()
                counter += 1
                if (counter % 5000 == 0):
                    print("Processed to line:" + str(counter))
                if (split[0] == "GT:"):
                    completedTime = int(split[2].rstrip(","))
                    latency = int(split[3].rstrip(","))
                    arrivedTime = completedTime - latency
                    if (fileInitialTime == -1 or fileInitialTime > arrivedTime):
                        fileInitialTime = arrivedTime
                    if (arrivedTime - fileInitialTime > 20000):
                        break
        if fileInitialTime > -1:
            fileInitialTimes[taskExecutor] = fileInitialTime
            if (initialTime == -1 or initialTime > fileInitialTime):
                initialTime = fileInitialTime
    return initialTime
def extractBacklog(rawDir, expName) -> []:
    initialTime = retrieveInitialTime(rawDir, expName)
    backlogPerOperator = {}
    backlogTimes = []
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
    with open(streamSluiceOutputPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if (split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "backlog:"):
                time = int(split[3])
                backlogTimes += [time]
                backlogs = parseBacklog("".join(split[6:]))
                for operator, task_backlogs in backlogs.items():
                    if operator not in backlogPerOperator:
                        backlogPerOperator[operator] = []
                    backlogPerOperator[operator] += [task_backlogs]
    return [[(x - initialTime) for x in backlogTimes], backlogPerOperator]
def drawBacklog(rawDir, expName, outputDir):
    results = extractBacklog(rawDir, expName)
    backlog_times = results[0]
    backlog_per_operator = results[1]
    # Calculate total backlog and maximum task backlog for the specific operator

    total_backlogs = [sum(task_backlog.values()) for task_backlog in backlog_per_operator[bottleneck_operator]]
    max_task_backlogs = [max(task_backlog.values(), default=0) for task_backlog in backlog_per_operator[bottleneck_operator]]

    # Plot the total backlog curve
    figName = "bottleneck_backlog"
    fig, axs = plt.subplots(1, 1, figsize=(12, 5), layout='constrained')
    fig.tight_layout(rect=[0.02, 0, 0.953, 1])
    axs.grid(True)
    ax1 = axs
    #plt.plot(backlog_times, total_backlogs, label='Total Backlog', color='blue')

    # Plot the maximum task backlog curve
    plt.plot(backlog_times, max_task_backlogs, label='Max Task Backlog', color='red')

    # Add titles and labels
    plt.title(f'Maximum Task Backlog for Bottleneck Operator')
    plt.xlabel('Time')
    plt.ylabel('Backlog')
    plt.legend()
    ax1.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
    ax1.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 60000, 60000))
    ax1.set_xticklabels([int((x - startTime * 1000) / 1000) for x in
                         np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 60000, 60000)])
    ax1.set_xlabel("Time (s)")
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)


exp_length = 120 #480 #360 #1800
startTime = 30 # + 300
rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
expName = "system-streamsluice-ds2-true-true-true-true-when-gradient-2op_line-170-5000-4000-4000-1-0-2-300-1-5000-2-300-1-5000-2-50-1-5000-2-444-5000-1000-3000-100-1-false-1"

bottleneck_operator = "eabd4c11f6c6fbdf011f0f1fc42097b1"
drawBacklog(rawDir, expName, outputDir + expName + "/")