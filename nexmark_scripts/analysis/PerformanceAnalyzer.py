import os

# the average latency
def averageLatency(lines, expName):
    # get all latency of all files, calculate the average
    totalLatency = 0
    count = 0
    for line in lines:
        if line.split(": ")[-1][:-1] != "NaN":
            totalLatency += float(line.split(": ")[-1][:-1])
            count += 1

    if count > 0:
        print(expName + " avg latency", ": ", totalLatency / count)
    else:
        print(expName + " avg latency", ": ", 0)

# the average reconfig time
def averageCompletionTime(lines, expName):
    totalCompletionTime = 0
    count = 0
    for line in lines:
        if line.split(" : ")[0] == "++++++endToEndTimer":
            totalCompletionTime += int(line.split(" : ")[1][:-3])
            count += 1
    if count > 0:
        print(expName + " avg completion time", ": ", totalCompletionTime/count)
    else:
        print(expName + " avg completion time", ": ", 0)
# reconfig time breakdown

root = "/data"

for expName in os.listdir(root):
    for file in os.listdir(os.path.join(root, expName)):
        file_path = os.path.join(root, expName, file)
        if file == "timer.output":
            averageCompletionTime(open(file_path).readlines(), expName)
        elif file == "Splitter FlatMap-0.output":
            averageLatency(open(file_path).readlines(), expName)


