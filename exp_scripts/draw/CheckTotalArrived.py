import os.path
import sys

input_file = '/Volumes/dragon/workspace/flink-related/flink-extended-streamswitch/flink/build-target/log/flink-samza-standalonesession-0-dragon-sane.out'

jobId = "c21234bcbf1e8eb4c61f1927190efebd"

partitionArrived = {}
partitionArrivedT = {}
partitionCompleted = {}
partitionCompletedT = {}
totalArrived = {}
totalCompleted = {}
initialTime = -1
base = 1
def parsePartitionArrived(split, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[9:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    partitions = info.split(',')
    total = 0
    for partition in partitions:
        if(len(partition)>0):
            Id = partition.split('=')[0]
            value = partition.split('=')[1]
            if(value == 'NaN'): value = '0'
            if (value == 'null'): value = '0'
            total += int(value)
            if(Id not in partitionArrived):
                partitionArrived[Id] = []
                partitionArrivedT[Id] = []
            partitionArrived[Id] += [int(value)]
            partitionArrivedT[Id] += [(long(time) - initialTime)/base ]
            if( (long(time) - initialTime)/base  not in totalArrived):
                totalArrived[(long(time) - initialTime)/base ] = 0
            totalArrived[(long(time) - initialTime)/base ] += int(value)

def parsePartitionCompleted(split, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[9:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    partitions = info.split(',')
    total = 0
    for partition in partitions:
        if(len(partition)>0):
            Id = partition.split('=')[0]
            value = partition.split('=')[1]
            if(value == 'NaN'): value = '0'
            if(value == 'null'): value = '0'
            total += int(value)
            if(Id not in partitionCompleted):
                partitionCompleted[Id] = []
                partitionCompletedT[Id] = []
            partitionCompleted[Id] += [int(value)]
            partitionCompletedT[Id] += [(long(time) - initialTime)/base ]
            if( (long(time) - initialTime)/base  not in totalCompleted):
                totalCompleted[(long(time) - initialTime)/base ] = 0
            totalCompleted[(long(time) - initialTime)/base ] += int(value)


print("Reading from file:" + input_file)
counter = 0


with open(input_file) as f:
    lines = f.readlines()
    for i in range(0, len(lines)):
        line = lines[i]
        split = line.rstrip().split(' ')
        counter += 1
        if(counter % 100 == 0):
            print("Processed to line:" + str(counter))

        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'State,') and split[4] == 'jobid:' and split[5] == jobId and split[7] == 'Partition' and split[8] == 'Arrived:'):
            parsePartitionArrived(split, base)
        if ((split[0] == 'DelayEstimateModel,' or split[0] == 'State,') and split[4] == 'jobid:' and split[5] == jobId and split[7] == 'Partition' and split[8] == 'Completed:'):
            parsePartitionCompleted(split, base)

print(totalArrived)
print(totalCompleted)