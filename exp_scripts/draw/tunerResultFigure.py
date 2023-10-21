import sys
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def parseSetting(fileName):
    split = fileName.split('-')
    setting = [int(split[4]), int(split[5]), int(split[11]), float(split[7]), int(split[8]), int(split[12]), int(split[16]), int(split[20])]
    return setting

def retrieveTuneResult(file):
    print("Read tune result " + file)
    tuneTrace = {}
    bestL = 1000
    with open(file) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            if len(split) == 0:
                continue
            if split[0] == "Auto" and split[2] == "round":
                currentL = int(split[-1])
            if split[-1] == "false" and len(split) > 2:
                L = int(split[0])
                spike = int(split[1])
                successRate = float(split[2])
                maxParallelism = int(split[3])
                avgParallelism = float(split[4])
                maxSpike = int(split[5])
                avgSpike = float(split[6])
                if L not in tuneTrace or tuneTrace[L][0] < successRate:
                    tuneTrace[L] = [successRate, avgParallelism, maxSpike]
            if line.count("best limit:"):
                bestL = int(split[-1])

    result = [bestL, tuneTrace]
    return result

def checkIsThisDimension(dimension, dimensions, baseline, tuneResult):
    for other in dimensions:
        if other != dimension and baseline[other] != tuneResult[other]:
            return False
    return True

outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/"

host = "samza@camel-sane.d2.comp.nus.edu.sg"
targetFiles = [
    #"~/workspace/flink-related/flink-testbed-sane/streamsluice_scripts/scripts/tune_log"
    "/data/streamsluice/autotuner/*"
]
saveDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/tune_result/"
import os
if not os.path.exists(saveDir):
    os.makedirs(saveDir)

#print("scp....")
#import subprocess
#for targetFile in targetFiles:
#    subprocess.run(["scp", host + ":" + targetFile, saveDir])

tuneResultPerWorkload = []
import os
for file in os.listdir(saveDir):
    if os.path.isfile(saveDir + "/" + file) and not file.startswith("."):
        setting = parseSetting(file)
        #if setting[4] == 3 and setting[5] == 5 and setting[6] == 9 and setting[7] == 17:
        tuneResult = {}
        tuneResult["name"] = file
        tuneResult["range"] = setting[0]
        tuneResult["period"] = setting[1]
        tuneResult["statesize"] = setting[2]
        tuneResult["skew"] = setting[3]
        print(file, setting[3])
        result = retrieveTuneResult(saveDir + "/" + file)
        tuneResult["best-L"] = result[0]
        tuneResult["tune-trace"] = result[1]
        tuneResultPerWorkload += [tuneResult]


dimensions = ["range", "period", "statesize", "skew"]
baseline = {}
baseline["range"] = 13000
baseRange = 10000
baseline["period"] = 30
baseline["statesize"] = 1000
baseline["skew"] = 0
i_dimension = 0
for dimension in dimensions:
    print("For dimension " + dimension)
    orderResult = {}
    for tuneResult in tuneResultPerWorkload:
        if checkIsThisDimension(dimension, dimensions, baseline, tuneResult):
            print(tuneResult["name"])
            orderResult[tuneResult[dimension]] = [tuneResult["best-L"], tuneResult["tune-trace"]]
            #x += [str(tuneResult[dimension])]
            #y += [tuneResult["best-L"]]
            #traces += [tuneResult["tune-trace"]]
    x = []
    y = []
    traces = []
    for key in sorted(orderResult.keys()):
        if dimension == 'range':
            avg = key
            upper = avg * 2 - baseRange
            x += [str(baseRange) + '-\n' + str(upper)]
        elif dimension == 'statesize':
            x += [str(key)]
        elif dimension == 'period':
            x += [str(key)]
        elif dimension == 'skew':
            x += [str(key)]
        y += [orderResult[key][0]]
        traces += [orderResult[key][1]]

    print("Draw dimension " + dimension)
    fig = plt.figure(figsize=(6, 3))
    figName = "limit_" + dimension
    p = plt.bar(x, y, width=0.8, bottom=None, align='center')
    plt.bar_label(p, label_type='center')
    #plt.plot(x, y, "*", markersize=6)
    #plt.title("Best latency guarante can achieved under different " + dimension)
    plt.ylabel("Best Latency Limit(ms)")
    #plt.xlabel(dimension)

    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figName + ".pdf", bbox_inches='tight')
    plt.close(fig)

    # print("Draw trace..")
    # figName = "tuningSuccessRateCurve_" + dimension
    # fig = plt.figure(figsize=(11, 5))
    # legend = []
    # xrange = [0, 4000]
    # for i in range(0, len(x)):
    #     limits = sorted(traces[i].keys())
    #     successRate = [traces[i][x][0] for x in limits]
    #     plt.plot(limits, successRate, "*-", markersize=12)
    #     legend += [x[i]]
    # requiredLinex = [0, 100000]
    # requiredLiney = [0.99, 0.99]
    # plt.plot(requiredLinex, requiredLiney, '-', color='r')
    # plt.title("Success rate under different latency limits under " + dimension)
    # plt.legend(legend)
    # plt.ylabel("Success Rate")
    # plt.xlabel("Latency Limit")
    # axes = plt.gca()
    # axes.set_xlim(xrange[0], xrange[1])
    # # axes.set_yscale('log')
    # # plt.yscale('log')
    # # axes.invert_yaxis()
    # import os
    # if not os.path.exists(outputDir):
    #     os.makedirs(outputDir)
    # plt.savefig(outputDir + figName + ".pdf", bbox_inches='tight')
    # plt.close(fig)
    #
    # print("Draw parallelism...")
    # figName = "tuningParallelismCurve_" + dimension
    # fig = plt.figure(figsize=(10, 7))
    # legend = []
    # for i in range(0, len(x)):
    #     limits = sorted(traces[i].keys())
    #     avgParalellism = [traces[i][x][1] for x in limits]
    #     plt.plot(limits, avgParalellism, "*-", markersize=6)
    #     legend += [x[i]]
    # plt.title("Avg parallelism under different latency limits under " + dimension)
    # plt.legend(legend)
    # plt.ylabel("Parallelism")
    # plt.xlabel("Latency Limit")
    # axes = plt.gca()
    # #axes.set_yscale('log')
    # #plt.yscale('log')
    # #axes.invert_yaxis()
    # import os
    # if not os.path.exists(outputDir):
    #     os.makedirs(outputDir)
    # plt.savefig(outputDir + figName + ".pdf", bbox_inches='tight')
    # plt.close(fig)
    #
    # print("Draw spike...")
    # figName = "tuningMaxSpikeCurve_" + dimension
    # fig = plt.figure(figsize=(10, 7))
    # legend = []
    # for i in range(0, len(x)):
    #     limits = sorted(traces[i].keys())
    #     maxSpike = [traces[i][x][2] for x in limits]
    #     plt.plot(limits, maxSpike, "*-", markersize=6)
    #     legend += [x[i]]
    # plt.title("Max spike under different latency limits under " + dimension)
    # plt.legend(legend)
    # plt.ylabel("Spike")
    # plt.xlabel("Latency Limit")
    # axes = plt.gca()
    # # axes.set_yscale('log')
    # # plt.yscale('log')
    # # axes.invert_yaxis()
    # import os
    # if not os.path.exists(outputDir):
    #     os.makedirs(outputDir)
    # plt.savefig(outputDir + figName + ".pdf", bbox_inches='tight')
    # plt.close(fig)





