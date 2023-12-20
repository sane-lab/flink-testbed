import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime

startTime = -1
dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/fraud_detection/"
fileName = "card_transaction.v1.csv"
counter = 0
eventRecords = []
with open(dir + fileName) as f:
    #lines = f.readlines()
    for line in f:
        counter += 1
        if counter > 1:
            split = line.rstrip().split(",")
            time = datetime.datetime.strptime(split[2] + "-" + split[3] + "-" + split[4] + " " + split[5], "%Y-%m-%d %H:%M")
            time = int(int(time.timestamp()) / 60 / 1000)
            for scale_factor in range(0, 1):
                eventRecords += [[time, int(split[8]) % 128]]
            if startTime == -1 or startTime > time:
                startTime = time
        if counter % 10000 == 0:
            print(counter)
        #if(counter > 5000000):
        #    break

arrivedPerWindow = {}
distribution = {}
for record in eventRecords:
    index = int((record[0] - startTime))
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
fig = plt.figure(figsize=(12, 8))
plt.plot(arrivalRate[0], arrivalRate[1], '*', color='blue', markersize=1)
plt.xlabel('Time (s)')
plt.ylabel('# of Records at Time')
import os
if not os.path.exists(dir):
    os.makedirs(dir)
plt.savefig(dir + "arrivalRate.png", bbox_inches='tight')
plt.close(fig)

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