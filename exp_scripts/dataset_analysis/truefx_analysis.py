import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime
import random
startTime = -1
dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/truefx/"
fileName = "EURJPY-2023-03.csv"
translateFile = "EURJPY-2023-03-4hr.txt"
counter = 0
eventRecords = []
randomseed = 12345
rand = random
rand.seed(randomseed)
with open(dir + fileName) as f:
    #lines = f.readlines()
    for line in f:
        counter += 1
        if counter > 1:
            split = line.rstrip().split(",")
            time = datetime.datetime.strptime(split[1], "%Y%m%d %H:%M:%S.%f")
            time = int(time.timestamp())
            for scale_factor in range(0, 1):
                eventRecords += [[time, rand.randint(0, 10000) , split[2], split[3]]]
            if startTime == -1 or startTime > time:
                startTime = time
            if (time - startTime > 144000):
                break
        if counter % 10000 == 0:
            print(counter)


arrivedPerWindow = {}
distribution = {}
for record in eventRecords:
    index = int((record[0] - startTime) / 10)
    if index not in arrivedPerWindow:
        arrivedPerWindow[index] = 0
    arrivedPerWindow[index] += 1
    if record[1] not in distribution:
        distribution[record[1]] = 0
    distribution[record[1]] += 1


arrivalRate = [[], []]
for index in arrivedPerWindow.keys():
    arrivalRate[0] += [index]
    arrivalRate[1] += [arrivedPerWindow[index]]
fig = plt.figure(figsize=(12, 6))
plt.plot(arrivalRate[0], arrivalRate[1], '*', color='blue', markersize=1)
plt.xlabel('Time (s)')
plt.ylabel('# of Records at Time')
import os
if not os.path.exists(dir):
    os.makedirs(dir)
plt.savefig(dir + fileName + "arrivalRate.png", bbox_inches='tight')
plt.close(fig)


f = open(dir + translateFile, "w")
for record in eventRecords:
    f.write(str(record[0]) + " " + str(record[1]) + " " + record[2] + " " + record[3] + "\n")
f.close()

# import pandas
# df = pd.DataFrame({
#     'ID': sorted(distribution.keys()),
#     'Times': [distribution[x] for x in sorted(distribution.keys())]})
# sorted_df = df.sort_values(by="Times", ascending=False)
# fig = plt.figure(figsize=(16, 9))
# p = plt.bar(sorted_df['ID'], sorted_df['Times'], 0.6, label="Average")
# import os
# if not os.path.exists(dir):
#     os.makedirs(dir)
# plt.savefig(dir + "distribution.png")
# plt.close(fig)