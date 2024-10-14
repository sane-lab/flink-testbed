import math
import os
import numpy as np
import matplotlib
import json

matplotlib.use('Agg')
import matplotlib.pyplot as plt

# Set up matplotlib font sizes
SMALL_SIZE = 25
MEDIUM_SIZE = 30
BIGGER_SIZE = 35

plt.rc('font', size=SMALL_SIZE)
plt.rc('axes', titlesize=SMALL_SIZE)
plt.rc('axes', labelsize=MEDIUM_SIZE)
plt.rc('xtick', labelsize=SMALL_SIZE)
plt.rc('ytick', labelsize=SMALL_SIZE)
plt.rc('legend', fontsize=SMALL_SIZE)
plt.rc('figure', titlesize=BIGGER_SIZE)
MARKERSIZE = 4
LINEWIDTH = 3


def add_latency_limit_marker(plt, latency_limit):
    x = [0, 10000000]
    y = [latency_limit, latency_limit]
    plt.plot(x, y, "--", label="Limit", color='red', linewidth=1.5)


def calculate_latency_limits(p99_latencies):
    filtered_latencies = p99_latencies
    if not filtered_latencies:
        raise ValueError("No latencies found in the specified time range.")

    sorted_latencies = sorted(filtered_latencies)
    target_p95_index = int(len(sorted_latencies) * 0.95) - 1
    latency_limit_95 = sorted_latencies[target_p95_index]
    target_p99_index = int(len(sorted_latencies) * 0.99) - 1
    latency_limit_99 = sorted_latencies[target_p99_index]

    return latency_limit_95, latency_limit_99


def read_ground_truth_latency(raw_dir, exp_name, window_size):
    initial_time = -1
    ground_truth_latency = []
    task_executors = [file for file in os.listdir(raw_dir + exp_name + "/") if
                      file.endswith(".out") and "taskexecutor" in file]

    for task_executor in task_executors:
        ground_truth_path = os.path.join(raw_dir, exp_name, task_executor)
        print("Reading ground truth file:" + ground_truth_path)
        file_initial_time = -1
        counter = 0
        with open(ground_truth_path) as f:
            lines = f.readlines()
            for i in range(0, len(lines)):
                line = lines[i]
                split = line.rstrip().split()
                counter += 1
                if (counter % 5000 == 0):
                    print("Processed to line:" + str(counter))
                if split[0] == "GT:":
                    completed_time = int(split[2].rstrip(","))
                    latency = int(split[3].rstrip(","))
                    arrived_time = completed_time - latency
                    if file_initial_time == -1 or file_initial_time > arrived_time:
                        file_initial_time = arrived_time
                    ground_truth_latency += [[arrived_time, latency]]
        if file_initial_time > 0 and (initial_time == -1 or initial_time > file_initial_time):
            initial_time = file_initial_time

    aggregated_ground_truth_latency = {}
    for pair in ground_truth_latency:
        index = int((pair[0] - initial_time) / window_size)
        if index not in aggregated_ground_truth_latency:
            aggregated_ground_truth_latency[index] = []
        aggregated_ground_truth_latency[index] += [pair[1]]

    average_ground_truth_latency = [[], [], []]
    for index in sorted(aggregated_ground_truth_latency):
        time = index * window_size
        x = int(time)
        if index in aggregated_ground_truth_latency:
            sorted_latency = sorted(aggregated_ground_truth_latency[index])
            size = len(sorted_latency)
            target = min(math.ceil(size * 0.99), size) - 1
            y = sorted_latency[target]
            average_ground_truth_latency[0] += [x]
            average_ground_truth_latency[1] += [y]
            y = sum(sorted_latency) / size
            average_ground_truth_latency[2] += [y]

    return [average_ground_truth_latency, initial_time]


def draw_latency_curves(raw_dir, output_dir, exp_name, window_size, start_time, exp_length, latency_limit):
    exps = [
        ["GroundTruth", exp_name, "blue", "o"]
    ]

    average_ground_truth_latencies = []
    initial_times = []
    for i in range(len(exps)):
        result = read_ground_truth_latency(raw_dir, exps[i][1], window_size)
        average_ground_truth_latencies += [result[0]]
        initial_times += [result[1]]

    success_rate_per_exps = {}
    for i in range(len(exps)):
        total_success = len([x for x in range(len(average_ground_truth_latencies[i][0])) if
                             average_ground_truth_latencies[i][0][x] >= start_time * 1000 and
                             average_ground_truth_latencies[i][0][x] <= (start_time + 1800) * 1000 and
                             average_ground_truth_latencies[i][1][x] <= latency_limit])
        total_windows = len([x for x in range(len(average_ground_truth_latencies[i][0])) if
                             average_ground_truth_latencies[i][0][x] >= start_time * 1000 and
                             average_ground_truth_latencies[i][0][x] <= (start_time + 1800) * 1000])
        success_rate_per_exps[exps[i][0]] = total_success / float(total_windows)

        groundtruth_p99_latency_in_range = [average_ground_truth_latencies[i][1][x] for x in
                                            range(len(average_ground_truth_latencies[i][0])) if
                                            average_ground_truth_latencies[i][0][x] >= start_time * 1000 and
                                            average_ground_truth_latencies[i][0][x] <= (start_time + exp_length) * 1000]
        result_limits = calculate_latency_limits(groundtruth_p99_latency_in_range)
        print(f"in range ground truth P99 limit: {result_limits[1]}, P95 limit: {result_limits[0]}")

    # Plotting the latency curve
    fig, ax = plt.subplots(figsize=(12, 5))
    for i in range(len(exps)):
        average_ground_truth_latency = average_ground_truth_latencies[i]
        sample_factor = 1
        sampled_latency = [[], [], []]
        sampled_latency[0] = [average_ground_truth_latency[0][i] for i in
                              range(0, len(average_ground_truth_latency[0]), sample_factor)]
        sampled_latency[1] = [max([average_ground_truth_latency[1][y] for y in
                                   range(x, min(x + sample_factor, len(average_ground_truth_latency[1])))]) for x in
                              range(0, len(average_ground_truth_latency[0]), sample_factor)]
        sampled_latency[2] = [max([average_ground_truth_latency[2][y] for y in
                                   range(x, min(x + sample_factor, len(average_ground_truth_latency[2])))]) for x in
                              range(0, len(average_ground_truth_latency[0]), sample_factor)]

        plt.plot(sampled_latency[0], sampled_latency[1], '-', color=exps[i][2], markersize=4, linewidth=3,
                 label="Ground Truth P99")
        add_latency_limit_marker(plt, latency_limit)

    handles, labels = plt.gca().get_legend_handles_labels()
    new_labels, new_handles = [], []
    for handle, label in zip(handles, labels):
        if label not in new_labels:
            new_labels.append(label)
            new_handles.append(handle)
    plt.legend(new_handles, new_labels, bbox_to_anchor=(0.45, 1.4), loc='upper center', ncol=3, markerscale=4.)
    plt.ylabel('Latency (ms)')
    axes = plt.gca()
    axes.set_xlim((start_time) * 1000, (start_time + exp_length) * 1000)
    axes.set_xticks(np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + 60000, 60000))
    axes.set_xticklabels([int((x - start_time * 1000) / 1000) for x in
                          np.arange((start_time) * 1000, (start_time + exp_length) * 1000 + 60000, 60000)])
    axes.set_ylim(-1, 500)
    axes.set_yticks(np.arange(0, 550, 50))
    plt.grid(True)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    plt.savefig(output_dir + 'ground_truth_latency_curves.png', bbox_inches='tight')
    plt.close(fig)

    return result_limits[1], success_rate_per_exps['GroundTruth']

def parsePerTaskValue(splits):
    taskValues = {}
    for split in splits:
        split = split.lstrip("{").rstrip("}").rstrip(",")
        words = split.split("=")
        taskName = words[0]
        value = float(words[1])
        taskValues[taskName] = value
    return taskValues

def addScalingMarker(plt, scalingMarker):
    for scaling in scalingMarker:
        time = scaling[0]
        type = scaling[1]
        if type == 2:
            color = "orange"
        elif type == 1:
            color = "green"
        else:
            color = "gray"
        x = [time, time]
        y = [0, 10000000]
        plt.plot(x, y, color=color, linewidth=LINEWIDTH/2.0)

def parseMapping(split):
    mapping = {}
    for word in split:
        word = word.lstrip("{").rstrip("}")
        if "=" in word:
            x = word.split("=")
            job = x[0].split("_")[0]
            task = x[0]
            key = x[1].lstrip("[").rstrip(",").rstrip("]")
            if job not in mapping:
                mapping[job] = {}
            mapping[job][task] = [key]
        else:
            key = word.rstrip(",").rstrip("]")
            mapping[job][task] += [key]
    return mapping

def readParallelism(rawDir, expName, windowSize):
    initialTime = -1
    lastTime = 0
    arrivalRatePerTask = {}
    ParallelismPerJob = {}
    scalingMarkerByOperator = {}
    scalings = []

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
                    if (lastTime < completedTime):
                        lastTime = completedTime
    print("init time=" + str(initialTime) + " last time=" + str(lastTime))

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

    timestamp_to_index = {}

    with open(streamSluiceOutputPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale" and split[
                8] == "operator:"):
                time = int(split[3])
                if (split[7] == "in"):
                    type = 1
                elif (split[7] == "out"):
                    type = 2

                lastScalingOperators = [split[9].lstrip('[').rstrip(']')]
                for operator in lastScalingOperators:
                    if (operator not in scalingMarkerByOperator):
                        scalingMarkerByOperator[operator] = []
                    scalingMarkerByOperator[operator] += [[time - initialTime, type]]
                mapping = parseMapping(split[12:])
                scalings.append(time - initialTime)

            if (len(split) >= 8 and split[0] == "+++" and split[1] == "[CONTROL]" and split[4] == "all" and split[
                5] == "scaling" and split[6] == "plan" and split[7] == "deployed."):
                time = int(split[3])
                # if (time > lastTime):
                #    continue
                for operator in lastScalingOperators:
                    if (operator not in scalingMarkerByOperator):
                        scalingMarkerByOperator[operator] = []
                    scalingMarkerByOperator[operator] += [[time - initialTime, 3]]
                lastScalingOperators = []
                for job in mapping:
                    ParallelismPerJob[job][0].append(time - initialTime)
                    ParallelismPerJob[job][1].append(len(mapping[job].keys()))


            if (split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "backlog:"):
                time = int(split[3])
                backlogs = parsePerTaskValue(split[6:])
                parallelism = {}
                for task in backlogs:
                    job = task.split("_")[0]
                    if job not in parallelism:
                        parallelism[job] = 0
                    parallelism[job] += 1
                for job in parallelism:
                    if job not in ParallelismPerJob:
                        ParallelismPerJob[job] = [[time - initialTime], [parallelism[job]]]
                        print(ParallelismPerJob)

            if (split[0] == "+++" and split[1] == "[METRICS]" and split[4] == "task" and split[5] == "arrivalRate:"):
                time = int(split[3])
                # if (time > lastTime):
                #   continue
                arrivalRates = parsePerTaskValue(split[6:])
                for task in arrivalRates:
                    if task not in arrivalRatePerTask:
                        arrivalRatePerTask[task] = [[], []]
                    import math
                    if not math.isnan(arrivalRates[task]) and not math.isinf(arrivalRates[task]):
                        arrivalRatePerTask[task][0] += [time - initialTime]
                        arrivalRatePerTask[task][1] += [int(arrivalRates[task] * 1000)]

    ParallelismPerJob["TOTAL"] = [[], []]
    for job in ParallelismPerJob:
        if job != "TOTAL":
            for i in range(0, len(ParallelismPerJob[job][0])):
                if i >= len(ParallelismPerJob["TOTAL"][0]):
                    ParallelismPerJob["TOTAL"][0].append(ParallelismPerJob[job][0][i])
                    ParallelismPerJob["TOTAL"][1].append(ParallelismPerJob[job][1][i])
                else:
                    ParallelismPerJob["TOTAL"][1][i] += ParallelismPerJob[job][1][i]

    totalArrivalRatePerJob = {}
    for task in arrivalRatePerTask:
        job = task.split("_")[0]
        n = len(arrivalRatePerTask[task][0])
        if job not in totalArrivalRatePerJob:
            totalArrivalRatePerJob[job] = {}
        for i in range(0, n):
            ax = arrivalRatePerTask[task][0][i]
            index = round(ax / windowSize) * windowSize #math.floor(ax / windowSize) * windowSize
            ay = arrivalRatePerTask[task][1][i]
            if index not in totalArrivalRatePerJob[job]:
                totalArrivalRatePerJob[job][index] = ay
            else:
                totalArrivalRatePerJob[job][index] += ay

    print(expName, ParallelismPerJob.keys())
    return [ParallelismPerJob, totalArrivalRatePerJob, initialTime, scalings]

def draw_parallelism_curve(rawDir, outputDir, exp_name, windowSize, startTime, exp_length, draw_parallelism_flag) -> [float, [list[int], list[float]]]:
    exps = [
        ["Sluice", exp_name, "blue", "o"]
    ]
    parallelismsPerJob = {}
    totalArrivalRatesPerJob = {}
    totalParallelismPerExps = {}
    for expindex in range(0, len(exps)):
        expFile = exps[expindex][1]
        result = readParallelism(rawDir, expFile, windowSize)
        parallelisms = result[0]
        totalArrivalRates = result[1]
        scalings = result[3]
        for job in parallelisms.keys():
            if job == "TOTAL":
                totalParallelismPerExps[expindex] = parallelisms[job]
                for i in range(0, len(parallelisms[job][1])):
                    l = 0
                    r = 0
                    if (i + 1 < len(parallelisms[job][0])):
                        r = parallelisms[job][0][i + 1]
                    l = max(parallelisms[job][0][i], startTime * 1000)
                    r = min(r, (startTime + exp_length) * 1000)
                continue
            if job not in parallelismsPerJob:
                parallelismsPerJob[job] = []
                totalArrivalRatesPerJob[job] = []
            parallelismsPerJob[job] += [parallelisms[job]]
            totalArrivalRatesPerJob[job] += [totalArrivalRates[job]]
    print("Draw total figure...")
    print("TOTAL parallelism: " + str(totalParallelismPerExps))

    figName = "Parallelism"
    nJobs = len(parallelismsPerJob.keys())
    jobList = ["a84740bacf923e828852cc4966f2247c", "eabd4c11f6c6fbdf011f0f1fc42097b1", "d01047f852abd5702a0dabeedac99ff5", "d2336f79a0d60b5a4b16c8769ec82e47", "feccfb8648621345be01b71938abfb72"]
    fig, axs = plt.subplots(1, 1, figsize=(12, 5), layout='constrained')
    # Add super label
    #fig.supylabel('# of Slots')
    #supylabel2(fig, "Arrival Rate (tps)")
    fig.tight_layout(rect=[0.02, 0, 0.953, 1])
    axs.grid(True)
    if(draw_parallelism_flag):
        ax1 = axs
        ax2 = ax1.twinx()
    else:
        ax2 = axs
        ax2.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
        ax2.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 60000, 60000))
        ax2.set_xticklabels([int((x - startTime * 1000) / 1000) for x in
                             np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 60000, 60000)])
        ax2.set_xlabel("Time (s)")


    ax2.set_ylabel("Arrival Rate (tps)")

    job = jobList[0]
    ax = sorted(totalArrivalRatesPerJob[job][0].keys())
    ay = [totalArrivalRatesPerJob[job][0][x] / (windowSize / 100) for x in ax]
    arrival_curves = [ax, ay]
    ax2.plot(ax, ay, '-', color='red', markersize=MARKERSIZE / 2, label="Arrival Rate")
    #ax2.set_ylabel('Rate (tps)')
    ax2.set_ylim(0, 30000)
    ax2.set_yticks(np.arange(0, 35000, 5000))
    # legend = ["OP_" + str(jobIndex + 1) +"Arrival Rate"]
    legend = ["Arrival Rate"]
    # ax2.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
    # ax2.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 300000, 300000))
    # ax2.set_xticklabels([int((x - startTime * 1000) / 60000) for x in
    #                      np.arange(startTime * 1000, (startTime + 3600) * 1000 + 300000, 300000)])
    ax2.legend(legend, loc='upper right', bbox_to_anchor=(1.1, 1.3), ncol=1)
    average_parallelism = 0.0
    if(draw_parallelism_flag):
        ax1.set_ylabel("# of Slots")
        legend = []
        scalingPoints = [[], []]
        for expindex in range(0, len(exps)):
            if(exps[expindex][0] == "Static"):
                continue
            print("Draw exps " + exps[expindex][0] + " curve...")
            totalParallelism = 0
            Parallelism = totalParallelismPerExps[expindex]
            # print(job + " " + str(expindex) + " " + str(Parallelism))
            legend += [exps[expindex][0]]
            line = [[], []]
            for i in range(0, len(Parallelism[0])):
                x0 = Parallelism[0][i]
                y0 = Parallelism[1][i]
                if i + 1 >= len(Parallelism[0]):
                    x1 = 10000000
                    y1 = y0
                else:
                    x1 = Parallelism[0][i + 1]
                    y1 = Parallelism[1][i + 1]
                l = max(x0, startTime * 1000)
                r = min(x1, (startTime + exp_length) * 1000)
                if(exps[expindex][0] == 'Sluice' and l < r):
                    totalParallelism += (r - l) * y0
                    for scalingTime in scalings:
                        if scalingTime >= l and scalingTime <= r:
                            scalingPoints[0] += [scalingTime]
                            scalingPoints[1] += [y0]
                line[0].append(x0)
                line[0].append(x1)
                line[1].append(y0)
                line[1].append(y0)
                line[0].append(x1)
                line[0].append(x1)
                line[1].append(y0)
                line[1].append(y1)
            if exps[expindex][0] == 'Sluice':
                linewidth = LINEWIDTH
            else:
                linewidth = LINEWIDTH / 2.0
            ax1.plot(line[0], line[1], color=exps[expindex][2], linewidth=linewidth, label='# of Slots')
            average_parallelism = totalParallelism / (exp_length * 1000)
            print("Average parallelism " + exps[expindex][0] + " : " + str(totalParallelism / (exp_length * 1000)))
        ax1.plot(scalingPoints[0], scalingPoints[1], 'o', color="orange", mfc='none', markersize=MARKERSIZE * 2, label="Scaling")
        ax1.legend(legend, loc='upper left', bbox_to_anchor=(-0.1, 1.3), ncol=3, markerscale=4.)
        # ax1.set_ylabel('OP_'+str(jobIndex+1)+' Parallelism')
        ax1.set_ylim(4, 32) #17)
        ax1.set_yticks(np.arange(4, 34, 2)) #18, 1))

        ax1.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
        ax1.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 60000, 60000))
        ax1.set_xticklabels([int((x - startTime * 1000) / 1000) for x in
                             np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 60000, 60000)])
        ax1.set_xlabel("Time (s)")

    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    # plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)
    return average_parallelism, arrival_curves

def draw_arrival_curve(outputDir, arrival_curves, startTime, exp_length):
    # Create a figure for P99 Latency Limits
    fig, axs = plt.subplots(figsize=(12, 5))
    print(arrival_curves)
    for label, arrival_curve in arrival_curves.items():
        plt.plot(arrival_curve[0], arrival_curve[1], '-', label=label, markersize=MARKERSIZE / 2)
    ax1 = axs
    ax1.set_xlim(startTime * 1000, (startTime + exp_length) * 1000)
    ax1.set_xticks(np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 60000, 60000))
    ax1.set_xticklabels([int((x - startTime * 1000) / 1000) for x in
                         np.arange(startTime * 1000, (startTime + exp_length) * 1000 + 60000, 60000)])
    ax1.set_ylim(0, 25000)
    ax1.set_yticks(np.arange(0, 30000, 5000))
    ax1.set_xlabel("Time (s)")
    plt.ylabel('Arrival Rate (tps)')
    plt.title('Arrival Rate Curve')
    plt.legend()
    plt.grid(True)
    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    # plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.savefig(outputDir + "arrival_curve.png", bbox_inches='tight')
    plt.close(fig)

def main():
    raw_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
    output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
    overall_output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/autotuner_new/"
    window_size = 100
    start_time = 60 #30 #60
    exp_length = 300



    setting1 = {
        "1 op": [
            "setting1-true-streamsluice-ds2-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-false-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-90-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-125-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-250-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1",
        ],
        "2 ops": [
            "setting1-true-streamsluice-ds2-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-false-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-190-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-225-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1",
        ],
        "3 ops": [
            "setting1-true-streamsluice-ds2-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-false-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-290-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-325-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1",
            "setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1",
        ],
    }

    setting2 = {
        "arrival -> 10000": [
            "setting2-true-streamsluice-ds2-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-false-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-290-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-325-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1",
        ],
        "arrival -> 15000": [
            "setting2-true-streamsluice-ds2-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-false-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-290-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-325-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1",
        ],
        "arrival -> 20000": [
            "setting2-true-streamsluice-ds2-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-false-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-290-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-325-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1",
            "setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1",
        ],
    }

    setting3 = {
        "Period 120s": [
            "setting3-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-false-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-325-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-400-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-450-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        ],
        "Period 90s": [
            "setting3-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-false-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-325-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-400-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-450-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        ],
        "Period 60s": [
            "setting3-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-false-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-325-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-400-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-450-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1",
            "setting3-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        ],
    }
    setting4 = {
        "Amplitude 25%": [
            "setting4-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-13750-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1500-3000-100-1-false-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-13750-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-350-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-13750-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-500-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-13750-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-750-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-13750-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1000-3000-100-1-true-1",
        ],
        "Amplitude 50%": [
            "setting4-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1000-3000-100-1-false-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-290-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-325-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-350-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-500-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-750-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1000-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1250-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1500-3000-100-1-true-1",

        ],
        "Amplitude 75%": [
            "setting4-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-17500-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1000-3000-100-1-false-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-17500-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-290-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-17500-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-325-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-17500-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-350-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-17500-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-500-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-17500-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-750-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-17500-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1000-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-17500-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1250-3000-100-1-true-1",
            "setting4-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-17500-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1500-3000-100-1-true-1",
        ],
    }

    setting5 = {
        "Amplitude 25%": [
            "setting5-true-streamsluice-ds2-false-true-true-false-when-gradient-1split2join1-390-13750-45-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-false-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-13750-45-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-13750-45-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-13750-45-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-13750-45-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1"
        ],
        "Amplitude 50%": [
            "setting5-true-streamsluice-ds2-false-true-true-false-when-gradient-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-false-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-325-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        ],
        "Amplitude 75%": [
            "setting5-true-streamsluice-ds2-false-true-true-false-when-gradient-1split2join1-390-17500-45-2500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-false-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-17500-45-2500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-17500-45-2500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-325-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-17500-45-2500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-17500-45-2500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-17500-45-2500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-17500-45-2500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-17500-45-2500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1",
            "setting5-true-streamsluice-streamsluice-false-true-true-false-when-gradient-1split2join1-390-17500-45-2500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1"
        ],
    }

    setting6 = {
        "State 100MB": [
            "setting6-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-false-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-310-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-320-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-330-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-340-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        ],
        "State 200MB": [
            "setting6-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-1000-3000-100-1-false-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-290-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-310-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-320-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-330-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-340-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-350-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-500-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-750-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-1000-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-1250-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-1500-3000-100-1-true-1",
        ],
        "State 300MB": [
            "setting6-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-1000-3000-100-1-false-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-290-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-310-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-320-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-330-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-340-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-350-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-500-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-750-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-1000-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-1250-3000-100-1-true-1",
            "setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-1500-3000-100-1-true-1",
        ],
    }

    setting7 = {
        "1 Bottleneck": [
            "setting7-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-false-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-310-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-320-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-330-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-340-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1",
        ],
        "2 Bottleneck": [
            "setting7-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-1000-3000-100-1-false-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-290-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-310-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-320-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-330-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-340-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-350-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-500-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-750-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-1000-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-1250-3000-100-1-true-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-1500-3000-100-1-true-1",

        ],
        "3 Bottleneck": [
            "setting7-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-3-222-1-5000-5-333-1-5000-15-500-5000-1000-3000-100-1-false-1",
            "setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-3-222-1-5000-5-333-1-5000-15-500-5000-1000-3000-100-1-true-1",
        ],
    }

    settings = {
       # "setting_1": setting1,
        "setting_2": setting2,
       # "setting_3": setting3,
       # "setting_4": setting4,
       # "setting_5": setting5,
       #  "setting_6": setting6,
       #  "setting_7": setting7,
    }

    for setting_name, setting in settings.items():
        print("Start draw setting " + setting_name)

        results = {
            'bar_per_labels': {},
            'p99limits_per_labels': {},
            'successrate_per_labels': {},
            'resource_per_labels': {}
        }

        latency_bar_per_labels = {}
        p99limits_per_labels = {}
        successrate_per_labels = {}
        resource_per_labels = {}
        arrival_curves = {}

        for label, exps in setting.items():
            print("Start draw label" + label)
            latency_bar_per_labels[label] = []
            p99limits_per_labels[label] = []
            successrate_per_labels[label] = []
            resource_per_labels[label] = []

            for exp_name in exps:
                if exp_name.split('-')[-2] == "false": # Show workload
                    avg_parallelism, arrival_curves[label] = draw_parallelism_curve(raw_dir, overall_output_dir + "results/" + setting_name + "/", exp_name, window_size * 10, start_time, exp_length, False)
                else:
                    latency_bar = int(exp_name.split('-')[-6])

                    p99_latency_limit, latency_bar_success_rate = draw_latency_curves(raw_dir, output_dir + exp_name + '/', exp_name, window_size,
                                                                                  start_time, exp_length, latency_bar)
                    print(f"P99 Latency Limit: {p99_latency_limit}")
                    print(f"Latency Bar Success Rate: {latency_bar_success_rate}")

                    avg_parallelism, trash = draw_parallelism_curve(raw_dir, output_dir + exp_name + '/', exp_name, window_size, start_time, exp_length, True)
                    print(f"Average parallelism: {avg_parallelism}")

                    latency_bar_per_labels[label] += [latency_bar]
                    p99limits_per_labels[label] += [p99_latency_limit]
                    successrate_per_labels[label] += [latency_bar_success_rate]
                    resource_per_labels[label] += [avg_parallelism]
            results['bar_per_labels'][label] = latency_bar_per_labels[label]
            results['p99limits_per_labels'][label] = p99limits_per_labels[label]
            results['successrate_per_labels'][label] = successrate_per_labels[label]
            results['resource_per_labels'][label] = resource_per_labels[label]
        with open(os.path.join(overall_output_dir, f'{setting_name}.json'), 'w') as f:
            json.dump(results, f, indent=4)
        draw_arrival_curve(overall_output_dir + "results/" + setting_name + "/", arrival_curves, start_time, exp_length)



if __name__ == "__main__":
    main()

