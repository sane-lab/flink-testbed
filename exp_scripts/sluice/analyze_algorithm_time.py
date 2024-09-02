import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime

def analyze_algorithm_time(rawDir, expName, outputDir):
    streamsluiceOutput = "flink-samza-standalonesession-0-eagle-sane.log"
    import os
    for file in os.listdir(rawDir + expName + "/"):
        if file.endswith(".log"):
            # print(os.path.join(rawDir + expName + "/", file))
            if file.count("standalonesession") == 1:
                streamsluiceOutput = file
    streamSluiceOutputPath = rawDir + expName + "/" + streamsluiceOutput
    print("Reading streamsluice output:" + streamSluiceOutputPath)
    counter = 0

    start_time = -1
    trigger_times = [-1]
    etel_times = []
    choose_times = [[]]
    phase_1_times = []
    prioritization_times = [[]]
    reallocate_times = [[]]
    fit_times = [[]]
    phase_2_times = []
    phase_2_ete_l_calls = [[]]
    phase_2_flag = False

    reallocate_now = 0
    try_n = 0

    with open(streamSluiceOutputPath) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split()
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if (start_time == -1 and len(split) >= 6 and split[3] == "org.apache.flink.runtime.rescale.streamsluice.PipelineLatencyGuarantor"):
                timestamp_str = split[0] + " " + split[1]
                dt_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                # Convert the datetime object to system milliseconds
                milliseconds = int(dt_obj.timestamp() * 1000)
                start_time = milliseconds


            if (len(split) >= 12 and split[5] == "+++" and split[6] == "[MODEL]" and split[7] == "ete" and split[8] == "end" and split[9] == "total" and split[10] == "time:"):
                etel_times += [int(split[11]) / 1000000.0]
                if phase_2_flag:
                    phase_2_ete_l_calls[-1] += [int(split[11]) / 1000000.0]
            if (len(split) >= 7 and split[5] == "+++" and split[6] == "[ALGO]"):

                if (trigger_times[-1] == -1):
                    timestamp_str =  split[0] + " " + split[1]
                    dt_obj = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
                    # Convert the datetime object to system milliseconds
                    milliseconds = int(dt_obj.timestamp() * 1000)
                    trigger_times[-1] = milliseconds


                if (len(split) >= 10 and split[7] == "choose" and split[8] == "nanotime:"):
                    choose_times[-1] += [int(split[9]) / 1000000.0]
                if (len(split) >= 12 and split[7] == "Phase" and split[8] == "1" and split[9] == "total" and split[10] == "nano:"):
                    phase_1_times += [int(split[11]) / 1000000.0]
                    phase_2_flag = True
                if (len(split) >= 10 and split[7] == "prioritization" and split[8] == "nanotime:"):
                    prioritization_times[-1] += [int(split[9]) / 1000000.0]
                if (len(split) >= 11 and split[7] == "base" and split[9] == "try"):
                    if(len(phase_1_times) == 4):
                        base = split[8]
                        try_n = int(split[10][2:].rstrip())
                        print(str(try_n) + " reallocate: " + str(reallocate_now))
                        reallocate_now = 0
                if (len(split) >= 11 and split[7] == "reallocate" and split[8] == "key" and split[9] == "nanotime:"):
                    reallocate_times[-1] += [int(split[10]) / 1000000.0]
                    reallocate_now += 1
                if (len(split) >= 10 and split[7] == "fit" and split[8] == "nanotime:"):
                    fit_times[-1] += [int(split[9]) / 1000000.0]
                if (len(split) >= 12 and split[7] == "Phase" and split[8] == "2" and split[9] == "total" and split[
                    10] == "nano:"):
                    phase_2_times += [int(split[11]) / 1000000.0]

                if (len(split) >= 12 and split[7] == "Best" and split[8] == "Mapping:"):
                    if (len(phase_1_times) == 11):
                        print(str(try_n) + " reallocate: " + str(reallocate_now))
                    choose_times += [[]]
                    prioritization_times += [[]]
                    reallocate_times += [[]]
                    fit_times += [[]]
                    trigger_times += [-1]
                    phase_2_flag = False
                    phase_2_ete_l_calls += [[]]
                    reallocate_now = 0

    del trigger_times[-1]
    del choose_times[-1]
    del prioritization_times[-1]
    del reallocate_times[-1]
    del fit_times[-1]
    del phase_2_ete_l_calls[-1]

    print([(trigger_times[i]-start_time)/1000.0 for i in range(0, len(trigger_times))])
    aggregated_choose_times = [sum(x) for x in choose_times]
    aggregated_prioritization_times = [sum(x) for x in prioritization_times]
    aggregated_reallocate_times = [sum(x) for x in reallocate_times]
    aggregated_fit_times = [sum(x) for x in fit_times]

    runs = ["Run " + str(i) for i in range(1, len(phase_2_times) + 1)]

    figName = "time_taken_aggregated"
    fig, axs = plt.subplots(1, 1, figsize=(10, 6), layout='constrained')
    ax1 = axs
    ax1.bar(runs, aggregated_choose_times, label='Choosing Heaviest Task & Key')
    ax1.bar(runs, aggregated_prioritization_times, bottom=aggregated_choose_times, label='Prioritization')
    ax1.bar(runs, aggregated_reallocate_times, bottom=[i + j for i, j in zip(aggregated_choose_times, aggregated_prioritization_times)],
            label='Reallocation Key')
    ax1.set_xlabel('Runs')
    ax1.set_ylabel('Cumulative Time (ms)')
    ax1.set_title('Cumulative Time Taken by Different Parts of the Algorithm per Run')
    ax1.legend()

    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)

    print(phase_1_times)
    print(phase_2_times)
    figName = "total_time_taken"
    fig, axs = plt.subplots(1, 1, figsize=(10, 6), layout='constrained')
    ax1 = axs
    ax1.bar(runs, phase_1_times[:len(phase_2_times)], label='Phase 1')
    ax1.bar(runs, phase_2_times, bottom=phase_1_times[:len(phase_2_times)], label='Phase 2')
    ax1.set_xlabel('Runs')
    ax1.set_ylabel('Cumulative Time (ms)')
    ax1.set_title('Cumulative Time Taken by Different Parts of the Algorithm per Run')
    ax1.legend()

    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)

    figName = "time_ete_l"
    fig, axs = plt.subplots(1, 1, figsize=(10, 6), layout='constrained')
    ax1 = axs
    ax1.plot(np.arange(0, len(etel_times), 1), etel_times, "o", label='ete l')
    ax1.set_xlabel('Occurrences')
    ax1.set_ylabel('Time (ms)')
    ax1.set_title('LEM estimate ete l time taken')
    ax1.legend()

    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)
    print("Average time for estimating ete_l: " + str(sum(etel_times)/len(etel_times)))
    print("Max/min time for ete_l: " + str(max(etel_times)) + ", " + str(min(etel_times)))

    figName = "time_ete_l_1run"
    fig, axs = plt.subplots(1, 1, figsize=(10, 6), layout='constrained')
    ax1 = axs
    ax1.plot(np.arange(0, len(phase_2_ete_l_calls[4]), 1), phase_2_ete_l_calls[4], "o", label='ete l')
    ax1.set_xlabel('Occurrences')
    ax1.set_ylabel('Time (ms)')
    ax1.set_title('LEM estimate ete l time taken')
    ax1.legend()

    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)
    print("Times for estimating ete_l: " + str(len(phase_2_ete_l_calls[4])))
    print("Average time for estimating ete_l: " + str(sum(phase_2_ete_l_calls[4]) / len(phase_2_ete_l_calls[4])))
    print("Max/min time for ete_l: " + str(max(phase_2_ete_l_calls[4])) + ", " + str(min(phase_2_ete_l_calls[4])))

    figName = "time_choose"
    fig, axs = plt.subplots(1, 1, figsize=(10, 6), layout='constrained')
    ax1 = axs
    # flat_list = [
    #     x
    #     for xs in choose_times
    #     for x in xs
    # ]
    flat_list = choose_times[4]
    ax1.plot(np.arange(0, len(flat_list), 1), flat_list, "o", label='chooose time')
    ax1.set_xlabel('Occurrences')
    ax1.set_ylabel('Time (ms)')
    ax1.set_title('Choose time')
    ax1.legend()

    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)
    print("Average time for choose key: " + str(sum(flat_list) / len(flat_list)))
    print("Max/min time for choose key: " + str(max(flat_list)) + ", " + str(min(flat_list)))
    #print("Max/min choosing times: " + str(max([len(x) for x in choose_times])))
    print("Max/min choosing times: " + str(len(choose_times[4])))

    figName = "time_reallocate"
    fig, axs = plt.subplots(1, 1, figsize=(10, 6), layout='constrained')
    ax1 = axs
    # flat_list = [
    #     x
    #     for xs in reallocate_times
    #     for x in xs
    # ]
    flat_list = reallocate_times[4]
    ax1.plot(np.arange(0, len(flat_list), 1), flat_list, "o", label='reallocate time')
    ax1.set_xlabel('Occurrences')
    ax1.set_ylabel('Time (ms)')
    ax1.set_title('Reallocate time')
    ax1.legend()

    import os
    if not os.path.exists(outputDir):
        os.makedirs(outputDir)
    plt.savefig(outputDir + figName + ".png", bbox_inches='tight')
    plt.close(fig)
    print("Average time for reallocate key: " + str(sum(flat_list) / len(flat_list)))
    print("Max/min time for reallocate key: " + str(max(flat_list)) + ", " + str(min(flat_list)))
    #print("Max/min reallocate times: " + str(max([len(x) for x in reallocate_times])))
    print("Max/min reallocate times: " + str(len(reallocate_times[4])))


rawDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
outputDir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/results/"
expName = "system-streamsluice-streamsluice-true-false-when-1split2join1-400-6000-3000-4000-1-0-2-300-1-5000-2-300-1-5000-2-300-1-5000-6-510-5000-2000-3000-100-10-true-1"
analyze_algorithm_time(rawDir, expName, outputDir + expName + "/")


