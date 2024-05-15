import os
import random
random.seed(12345)

def generate_rate(avg_rate, stage_length):
    rate_per_stage = []
    for i in range(0, 180):
        rate = int(random.randint(75, 125) / 100.0 * avg_rate)
        rate_per_stage.append(rate)
    rate_per_second = []
    for stage_rate in rate_per_stage:
        for i in range(0, stage_length):
            rate = int(random.randint(-50, 50) / 100.0 * (avg_rate / 10) + stage_rate)
            rate_per_second.append(rate)
    return rate_per_second


rate_per_second = generate_rate(1300, 120)
original_rate_flag = False

linear_road_raw_path = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/lr/LinearRoadJava/generatedRecords.txt"
linear_road_refined_path = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/lr/LinearRoadJava/3hr-our-rate.txt"
output_interval = 50 # milliseconds
fw = open(linear_road_refined_path, "w")
with (open(linear_road_raw_path) as f):
    lines = f.readlines()
    if original_rate_flag:
        counter = 0
        lastTime = -1
        lines_this_second = []
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split(',')
            counter += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            if lastTime == int(split[1]):
                lines_this_second.append(','.join(split[:1] + split[2:]))
            else:
                if(len(lines_this_second) > 0):
                    per_interval_len = int((len(lines_this_second) - 1) / (1000 / output_interval) + 1)
                    lines_per_interval = [lines_this_second[i: i + per_interval_len] for i in range(0, len(lines_this_second), per_interval_len)]
                    for output_lines in lines_per_interval:
                        for output_line in output_lines:
                            fw.write(output_line + "\n")
                        fw.write("END\n")
                lines_this_second = []
                lastTime = int(split[1])
    else:
        counter = 0
        cur_count = 0
        rate_index = 0
        lines_this_second = []
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split(',')
            counter += 1
            cur_count += 1
            if (counter % 5000 == 0):
                print("Processed to line:" + str(counter))
            lines_this_second.append(','.join(split[:1] + split[2:]))
            if cur_count >= rate_per_second[rate_index]:
                if(len(lines_this_second) > 0):
                    per_interval_len = int((len(lines_this_second) - 1) / (1000 / output_interval) + 1)
                    lines_per_interval = [lines_this_second[i: i + per_interval_len] for i in range(0, len(lines_this_second), per_interval_len)]
                    for output_lines in lines_per_interval:
                        for output_line in output_lines:
                            fw.write(output_line + "\n")
                        fw.write("END\n")
                lines_this_second = []
                rate_index += 1
                cur_count = 0
fw.close()