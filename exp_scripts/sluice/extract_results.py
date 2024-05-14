import math
import os

OPERATOR_NAMING = {
    "0a448493b4782967b150582570326227": "Stateful Map",
    "c21234bcbf1e8eb4c61f1927190efebd": "Splitter",
    "22359d48bcb33236cf1e31888091e54c": "Counter",
    "a84740bacf923e828852cc4966f2247c": "OP2",
    "eabd4c11f6c6fbdf011f0f1fc42097b1": "OP3",
    "d01047f852abd5702a0dabeedac99ff5": "OP4",
    "d2336f79a0d60b5a4b16c8769ec82e47": "OP5",
    "36fcfcb61a35d065e60ee34fccb0541a": "OP6",
    "c395b989724fa728d0a2640c6ccdb8a1": "OP7",
    "TOTAL": "TOTAL",
}

def parsePerTaskValue(splits):
    taskValues = {}
    for split in splits:
        split = split.lstrip("{").rstrip("}").rstrip(",")
        words = split.split("=")
        taskName = words[0]
        value = float(words[1])
        taskValues[taskName] = value
    return taskValues

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

def extract(raw_dir, expName, start_time, end_time, limit):

    initialTime = -1
    lastTime = -1
    windowSize = 1000

    groundTruthLatencyPerTuple = {}
    groundTruthLatency = []
    ParallelismPerJob = {}

    taskExecutors = []  # "flink-samza-taskexecutor-0-eagle-sane.out"
    import os
    for file in os.listdir(raw_dir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("taskexecutor") == 1:
                taskExecutors += [file]
    fileInitialTimes = {}
    for taskExecutor in taskExecutors:
        groundTruthPath = raw_dir + expName + "/" + taskExecutor
        print("Reading ground truth file:" + groundTruthPath)
        fileInitialTime = - 1
        counter = 0
        with open(groundTruthPath) as f:
            lines = f.readlines()
            for i in range(0, len(lines)):
                line = lines[i]
                split = line.rstrip().split(",")
                counter += 1
                if (counter % 5000 == 0):
                    print("Processed to line:" + str(counter))
                if (split[0].startswith("GT:")):
                    completedTime = int(split[1].replace(" ", ""))
                    latency = int(split[2].replace(" ", ""))
                    arrivedTime = completedTime - latency
                    if (fileInitialTime == -1 or fileInitialTime > arrivedTime):
                        fileInitialTime = arrivedTime
                    if (completedTime > lastTime):
                        lastTime = completedTime
                    tupleId = split[3].rstrip()
                    if tupleId not in groundTruthLatencyPerTuple:
                        groundTruthLatencyPerTuple[tupleId] = [arrivedTime, latency]
                    elif groundTruthLatencyPerTuple[tupleId][1] < latency:
                        groundTruthLatencyPerTuple[tupleId][1] = latency
        fileInitialTimes[taskExecutor] = fileInitialTime
        if (initialTime == -1 or initialTime > fileInitialTime):
            initialTime = fileInitialTime
    print("FF: " + str(fileInitialTimes))
    for value in groundTruthLatencyPerTuple.values():
        groundTruthLatency += [value]

    streamsluiceOutput = "flink-samza-standalonesession-0-eagle-sane.out"
    import os
    for file in os.listdir(raw_dir + expName + "/"):
        if file.endswith(".out"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("standalonesession") == 1:
                streamsluiceOutput = file
    streamSluiceOutputPath = raw_dir + expName + "/" + streamsluiceOutput
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
            if (len(split) >= 7 and split[0] == "+++" and split[1] == "[MODEL]" and split[6] == "cur_ete_l:"):
                estimateTime = int(split[3].rstrip('\n'))
                if (initialTime == -1 or initialTime > estimateTime):
                    initialTime = estimateTime
                if (lastTime < estimateTime):
                    lastTime = estimateTime

            if (len(split) >= 10 and split[0] == "+++" and split[1] == "[CONTROL]" and split[6] == "scale" and split[
                8] == "operator:"):
                mapping = parseMapping(split[12:])

            if (len(split) >= 8 and split[0] == "+++" and split[1] == "[CONTROL]" and split[4] == "all" and split[
                5] == "scaling" and split[6] == "plan" and split[7] == "deployed."):
                time = int(split[3])
                # if (time > lastTime):
                #    continue
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

    run_time = lastTime - initialTime

    sorted_latency = []
    aggregatedGroundTruthLatency = {}
    for pair in groundTruthLatency:
        index = int((pair[0] - initialTime) / windowSize)
        if index >= start_time * 1000 / windowSize and index <= end_time * 1000 / windowSize:
            if index not in aggregatedGroundTruthLatency:
                aggregatedGroundTruthLatency[index] = []
            aggregatedGroundTruthLatency[index] += [pair[1]]
            sorted_latency += [pair[1]]
    sorted_latency.sort()
    totalLatency = sum(sorted_latency)
    totalTuples = len(sorted_latency)
    average_latency = totalLatency/totalTuples
    p99_latency = sorted_latency[int(totalTuples * 0.99)]

    success_window = 0
    total_window = len(aggregatedGroundTruthLatency)
    averageGroundTruthLatency = [[], []]
    for index in sorted(aggregatedGroundTruthLatency):
        time = index * windowSize
        x = int(time)
        if index in aggregatedGroundTruthLatency:
            sortedLatency = sorted(aggregatedGroundTruthLatency[index])
            size = len(sortedLatency)
            target = min(math.ceil(size * 0.99), size) - 1
            y = sortedLatency[target]
            averageGroundTruthLatency[0] += [x]
            averageGroundTruthLatency[1] += [y]
            if y <= limit:
                success_window += 1
    success_rate = success_window/total_window

    total_parallelism = 0.0
    weightedTotalParallelismFlag = False
    parallelismWeight = {
        "OP2": 10,
        "OP3": 5,
        "OP4": 2,
        "OP5": 3,
    }
    ParallelismPerJob["TOTAL"] = [[], []]
    for job in ParallelismPerJob:
        if job != "TOTAL":
            for i in range(0, len(ParallelismPerJob[job][0])):
                if i >= len(ParallelismPerJob["TOTAL"][0]):
                    if weightedTotalParallelismFlag:
                        ParallelismPerJob["TOTAL"][0].append(
                            parallelismWeight[OPERATOR_NAMING[job]] * ParallelismPerJob[job][0][i])
                        ParallelismPerJob["TOTAL"][1].append(
                            parallelismWeight[OPERATOR_NAMING[job]] * ParallelismPerJob[job][1][i])
                    else:
                        ParallelismPerJob["TOTAL"][0].append(ParallelismPerJob[job][0][i])
                        ParallelismPerJob["TOTAL"][1].append(ParallelismPerJob[job][1][i])
                else:
                    if weightedTotalParallelismFlag:
                        ParallelismPerJob["TOTAL"][1][i] += parallelismWeight[OPERATOR_NAMING[job]] * \
                                                            ParallelismPerJob[job][1][i]
                    else:
                        ParallelismPerJob["TOTAL"][1][i] += ParallelismPerJob[job][1][i]

    Parallelism = ParallelismPerJob["TOTAL"]
    totalParallelismInRange = 0
    totalTime = 0
    for i in range(0, len(Parallelism[0])):
        x0 = Parallelism[0][i]
        y0 = Parallelism[1][i]
        if i + 1 >= len(Parallelism[0]):
            x1 = 10000000
            y1 = y0
        else:
            x1 = Parallelism[0][i + 1]
            y1 = Parallelism[1][i + 1]
        if x0 < start_time * 1000:
            x0 = start_time * 1000
        if x1 > end_time * 1000:
            x1 = end_time * 1000
        if x0 < x1:
            totalParallelismInRange += y0 * (x1 - x0)
            totalTime += (x1 - x0)
    average_parallelism = totalParallelismInRange / float(totalTime)
    return average_latency, p99_latency, success_rate, average_parallelism, run_time


input_file = "exp_list.txt"
output_file = "overall_results.txt"
raw_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
fout = open(output_file, "a")
start_time = 330
end_time = start_time + 3600
with open(input_file) as fin:
    lines = fin.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        if line.startswith("tweet"):
            limit = 2000
        elif line.startswith("stock"):
            limit = 2000
        elif line.startswith("linear"):
            limit = 2000
        results = extract(raw_dir, line.rstrip("\n").rstrip("\r"), start_time, end_time, limit)
        fout.write(line.rstrip("\n").rstrip("\r") + " " + " ".join([str(x) for x in results]) + "\n")
fout.close()