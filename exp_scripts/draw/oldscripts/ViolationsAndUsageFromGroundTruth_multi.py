# -*- coding: utf-8 -*-
import matplotlib
matplotlib.use('Agg')
import sys

jobIds = ["c21234bcbf1e8eb4c61f1927190efebd", "22359d48bcb33236cf1e31888091e54c"]
substreamArrivalAndCompletedTimeForVertex = {}


userLatency = 2000
userWindow = 1000
base = 1000 #timeslot size
peakIntervals = [[0, 200], [7200, 7290]]
calculateInterval = [0, 860]  #The interval we calculate violation percentage from 1st tuple completed
#totalLength = 7100
substreamArrivalAndCompletedTime = {} # Dict { substreamId : [[Arrival, Completed]...]}

from os import listdir
#figureName = 'stock_5_64_5_L4T4a0.5_64'
#figureName = '1h_32_L1T10A0.3333333'
figureName = sys.argv[1]
# inputDir = '/home/samza/workspace/flink-extended/build-target/log/'
inputDir = '/Volumes/dragon/workspace/flink-related/flink-extended-ete/build-target/log/'
outputDir = 'figures/' + figureName + '/'
keyAverageLatencyFlag = True
keyAverageLatencyThreshold = 0.2
keyLatencyIntervalFlag = False
calibrateFlag = False
import sys
startTime = sys.maxint
totalTime = 0
totalViolation = 0
totalMessages = 0
violationInPeak = []
totalInPeak = []

#Translate time from second to user window index
for peakI in range(0, len(peakIntervals)):
    violationInPeak += [0]
    totalInPeak += [0]
    peakIntervals[peakI]= [peakIntervals[peakI][0] * base / userWindow, peakIntervals[peakI][1] * base / userWindow]
xaxes = [calculateInterval[0] * 1000 / userWindow, calculateInterval[-1] * 1000 / userWindow]

maxMigrationTime = 0
maxMigrationExecutor = ""
migrationTimes = []
for fileName in listdir(inputDir):
    if fileName == "flink-samza-taskexecutor-0-dragon-sane.out":
        inputFile = inputDir + fileName
        counter = 0
        print("Processing file " + inputFile)
        startPoint = []
        endPoint = []
        startLogicTime = sys.maxint
        startOETime = sys.maxint
        t1 = 0
        with open(inputFile) as f:
            lines = f.readlines()
            for i in range(0, len(lines)):
                line = lines[i]
                split = line.rstrip().split(' ')

                counter += 1
                if (counter % 5000 == 0):
                    print("Processed to line:" + str(counter))
                if(calibrateFlag and split[0] == 'start' and split[1] == 'time:'):
                    t = int(split[2])
                    if(startOETime > t):
                        startOETime = t

                if(split[0] == 'GT:'):
                    keygroup = str(int(split[1].lstrip('A')) % 64)
                    arriveTime = int(split[3].rstrip(","))
                    latency = int(split[4])
                    totalMessages += 1
                    if (latency > userLatency):
                        totalViolation += 1
                    if (not calibrateFlag and arriveTime < startTime):
                        startTime = arriveTime
                    if(keygroup not in substreamArrivalAndCompletedTime):
                        substreamArrivalAndCompletedTime[keygroup] = []
                    if (calibrateFlag):
                        if(startLogicTime > arriveTime):
                            startLogicTime = arriveTime
                        if(startTime > startLogicTime):
                            startTime = startLogicTime
                        completeTime = int(split[4]) + arriveTime #int(split[4]) - startOETime + startLogicTime
                        arrivalTime = arriveTime
                        #skip 11:30 ~ 1:00
                        if(arriveTime > 1284377500000):
                            arrivalTime -= 5400000
                        if(startLogicTime > 1284377500000):
                            completeTime -= 5400000
                        #print(arrivalTime, completeTime)
                        substreamArrivalAndCompletedTime[keygroup].append([str(arrivalTime), str(completeTime)])
                    else:
                        substreamArrivalAndCompletedTime[keygroup].append([split[3].rstrip(','), split[4]])

                if (split[0] == 'GroundTruth' and split[2] == 'keygroup:'):
                    jobId = split[1]
                    if (jobId in jobIds):
                        if (jobId not in substreamArrivalAndCompletedTimeForVertex):
                            substreamArrivalAndCompletedTimeForVertex[jobId] = {}
                        if (not calibrateFlag and int(split[5]) < startTime):
                            startTime = int(split[5])
                        if (split[3] not in substreamArrivalAndCompletedTimeForVertex[jobId]):
                            substreamArrivalAndCompletedTimeForVertex[jobId][split[3]] = []
                        if (calibrateFlag):
                            if (startLogicTime > int(split[5])):
                                startLogicTime = int(split[5])
                            if (startTime > startLogicTime):
                                startTime = startLogicTime
                            completeTime = int(split[7]) - startOETime + startLogicTime

                            arrivalTime = int(split[5])
                            # skip 11:30 ~ 1:00
                            if (int(split[5]) > 1284377500000):
                                arrivalTime -= 5400000
                            if (startLogicTime > 1284377500000):
                                completeTime -= 5400000
                            # print(arrivalTime, completeTime)
                            substreamArrivalAndCompletedTimeForVertex[jobId][split[3]].append([str(arrivalTime), str(completeTime)])
                        else:
                            substreamArrivalAndCompletedTimeForVertex[jobId][split[3]] .append([split[5], split[7]])

                if(split[0] == 'Entering'):
                    startPoint += [int(split[3])]
                if(split[0] == 'Shutdown'):
                    endPoint += [int(split[2])]
        migrationTime = []
        for i in range(0, len(endPoint)):
            if(i + 1< len(startPoint)):
                migrationTime += [startPoint[i + 1] - endPoint[i]]
                migrationTimes += [migrationTime[-1]/1000.0]
        if(len(migrationTime) > 0):
            mmaxMigrationTime = max(migrationTime)
            if(mmaxMigrationTime > maxMigrationTime):
                maxMigrationTime = mmaxMigrationTime
                maxMigrationExecutor = fileName
            print(fileName, mmaxMigrationTime)
            print(startPoint, endPoint)

print(maxMigrationTime, maxMigrationExecutor)

print(totalMessages, totalViolation)
print("Success rate: " + str(1.0 - float(totalViolation)/totalMessages))
exit()
# #Draw migration length histogram
# if(True):
#     print("Draw migration length histogram...")
#     import os
#     outputFile = outputDir + 'migrationTimes.png'
#     if not os.path.exists(outputDir):
#         os.makedirs(outputDir)
#     import numpy as np
#     import matplotlib.pyplot as plt
#
#     legend = ['Migration Times Length']
#     fig = plt.figure(figsize=(45, 30))
#     bins = np.arange(0, 20, 1).tolist() + np.arange(20, 100, 10).tolist()
#     plt.hist(migrationTimes, bins=bins)
#     axes = plt.gca()
#     axes.set_xticks(bins)
#     axes.set_yticks(np.arange(0, 200, 10).tolist())
#     plt.grid(True)
#     plt.xlabel('Migration Length(s)')
#     plt.ylabel('# of Migration')
#     plt.title('Migration Time Length')
#     plt.savefig(outputFile)
#     plt.close(fig)

#print(startTime)
#exit(0)
substreamTime = []
substreamViolation = []

substreamLatency = []

totalViolationSubstream = {}
# Draw average latency
for substream in sorted(substreamArrivalAndCompletedTime):
    print("Calculate substream " + substream)
    substreamWindowCompletedAndTotalLatency = {}
    latencys = []
    for pair in substreamArrivalAndCompletedTime[substream]:
        arriveTime = int(pair[0])
        completeTime = int(pair[1])
        latency = completeTime - arriveTime
        if(latency < 0):
            #print("What? " + str(substream) + " " + str(pair))
            latency = 0


        if((completeTime - startTime)/userWindow >= xaxes[0] and (completeTime - startTime)/userWindow <= xaxes[1]):
            latencys += [latency]

        timeslot = (completeTime - startTime)/userWindow

        if timeslot not in totalViolationSubstream:
            totalViolationSubstream[timeslot] = []

        if (latency > 1000):
            if substream not in totalViolationSubstream[timeslot]:
                totalViolationSubstream[timeslot].append(substream)

        if(timeslot not in substreamWindowCompletedAndTotalLatency):
            substreamWindowCompletedAndTotalLatency[timeslot] = [1, latency]
        else:
            substreamWindowCompletedAndTotalLatency[timeslot][0] += 1
            substreamWindowCompletedAndTotalLatency[timeslot][1] += latency
    #print(substreamWindowCompletedAndTotalLatency)
    substreamLatency.append(latencys)
    x = []
    y = []
    thisTime = (xaxes[1] - xaxes[0] + 1)
    for peak in range(0, len(peakIntervals)):
        totalInPeak[peak] += (peakIntervals[peak][1] - peakIntervals[peak][0] + 1)
    #thisTime = 0
    thisViolation = 0
    thisViolationInterval = []
    for time in sorted(substreamWindowCompletedAndTotalLatency):
        latency = substreamWindowCompletedAndTotalLatency[time][1]
        number = substreamWindowCompletedAndTotalLatency[time][0]
        #print(time)
        x += [time]
        if(number > 0):
            #thisTime += 1
            avgLatency = float(latency) / number
            y += [avgLatency]
            if(time >= xaxes[0] and time <= xaxes[1]):
                if(avgLatency > userLatency):
                    thisViolation += 1
                    if(len(thisViolationInterval) > 0 and thisViolationInterval[-1][1] == time - 1):
                        thisViolationInterval[-1][1] = time
                    else:
                        thisViolationInterval.append([time, time])
            #Calculate peak interval
            for i in range(0, len(peakIntervals)):
                if(time >= peakIntervals[i][0] and time <= peakIntervals[i][1]):
                    if(avgLatency > userLatency):
                        violationInPeak[i] += 1
    substreamTime += [thisTime]
    substreamViolation += [thisViolation]
    percentage = 0.0
    if(thisTime > 0):
        percentage = thisViolation / float(thisTime)
    print(str(substream), percentage, thisTime)
    totalTime += thisTime
    totalViolation += thisViolation

    if(keyAverageLatencyFlag):
        print("Draw ", substream, " violation percentage...")
        import os
        outputFile = outputDir + 'windowLatency/' + substream + '.png'
        if not os.path.exists(outputDir + 'windowLatency'):
            os.makedirs(outputDir + 'windowLatency')
        import numpy as np
        import matplotlib.pyplot as plt
        legend = ['Window Average Latency']
        fig = plt.figure(figsize=(45, 30))
        plt.plot(x, y, 'bs')

        # Add user requirement
        userLineX = [xaxes[0], xaxes[1]]
        userLineY = [userLatency, userLatency]
        userLineC = 'r'
        plt.plot(userLineX, userLineY, linewidth=3.0, color=userLineC, linestyle='--')

        plt.legend(legend, loc='upper left')
        # print(arrivalRateT, arrivalRate)
        plt.grid(True)
        axes = plt.gca()
        axes.set_xlim(xaxes)
        axes.set_ylim([1, 10**6])
        axes.set_yscale('log')
        plt.xlabel('Timeslot Index')
        plt.ylabel('Average Latency')
        plt.title('Window Average Latency')
        plt.savefig(outputFile)
        plt.close(fig)
    if(keyLatencyIntervalFlag):
        x = []
        for i in range(0, len(thisViolationInterval)):
            #print(thisViolationInterval[i])
            x += [thisViolationInterval[i][1] - thisViolationInterval[i][0] + 1]
        import os
        outputFile = outputDir + 'latencyInterval/' + substream + '.png'
        if not os.path.exists(outputDir + 'latencyInterval'):
            os.makedirs(outputDir + 'latencyInterval')
        import numpy as np
        import matplotlib.pyplot as plt
        legend = ['Latency Interval']
        fig = plt.figure(figsize=(45, 30))
        plt.hist(x, bins=range(0,200))
        axes = plt.gca()
        axes.set_xticks(range(0,200))
        axes.set_yticks(np.arange(0, 200, 5).tolist())
        plt.grid(True)
        plt.xlabel('Latency Interval Length')
        plt.ylabel('# of Interval')
        plt.title('Latency Interval')
        plt.savefig(outputFile)
        plt.close(fig)

# draw substream violation

import numpy as np
import matplotlib.pyplot as plt
import os
outputFile = outputDir + 'violation.png'
legend = ['Total substream violation']
figList = []
for substreamList in totalViolationSubstream:
    print(substreamList)
    figList.append(len(totalViolationSubstream[substreamList]))
plt.plot(figList)
plt.xlabel('Timeslot Index')
plt.ylabel('#substream violation')
plt.title('Total substream violation')
plt.savefig(outputFile)

#Draw substream violation percetage histogram
if(True):
    print("Draw overall violation percentage figure...")
    import os
    outputFile = outputDir + 'keyViolationPercentage.png'
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    import numpy as np
    import matplotlib.pyplot as plt
    legend = ['Violation Percentage']
    fig = plt.figure(figsize=(45, 30))
    x = []
    for i in range(0, len(substreamTime)):
        x += [substreamViolation[i] / float(substreamTime[i])]
    bins = np.arange(0, 0.2, 0.01).tolist() + np.arange(0.2, 1, 0.05).tolist()
    plt.hist(x, bins=bins)
    axes = plt.gca()
    axes.set_xticks(bins)
    axes.set_yticks(np.arange(0, 1000, 50).tolist())
    plt.grid(True)
    plt.xlabel('Violation Percentage')
    plt.ylabel('# of Keys')
    plt.title('Keys Violation Percentage')
    plt.savefig(outputFile)
    plt.close(fig)

avgViolationPercentage = totalViolation / float(totalTime)
sumDeviation = 0.0
print('avg success rate=', 1 - avgViolationPercentage)
print('total violation number=' + str(totalViolation))
violationNotPeak = totalViolation
timeNotPeak = totalTime
if(totalViolation > 0):
    for peakI in range(0, len(peakIntervals)):
        print('violation percentage in peak '+str(peakI) + ' is ' + str(violationInPeak[peakI]/float(totalViolation)) + ', number is ' + str(violationInPeak[peakI]))
        violationNotPeak -= violationInPeak[peakI]
        timeNotPeak -= totalInPeak[peakI]
print('Execept peak avg success rate=', 1 - violationNotPeak/float(timeNotPeak))
# Calculate avg latency
if(False):
    print("Calculate avg lantecy")
    sumLatency = 0
    totalTuples = 0
    for i in range(0, len(substreamLatency)):
        #print(substreamLatency[i])
        sumLatency += sum(substreamLatency[i])
        totalTuples += len(substreamLatency[i])

    avgLatency = sumLatency / float(totalTuples)
    print('avg latency=', avgLatency)

    # Calculate standard error
    sumDeviation = 0.0
    print("Calculate standard deviation")
    for i in range(0, len(substreamLatency)):
        for j in range(0, len(substreamLatency[i])):
            sumDeviation += (((substreamLatency[i][j] - avgLatency) ** 2) / (totalTuples-1)) ** (0.5)
    print('Standard deviation=', sumDeviation)
    print('Standard error=', sumDeviation/(totalTuples) ** (0.5))
