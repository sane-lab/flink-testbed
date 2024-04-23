import os


linear_road_raw_path = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/lr/LinearRoadJava/generatedRecords.txt"
linear_road_refined_path = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/lr/LinearRoadJava/3hr.txt"
output_interval = 50 # milliseconds
fw = open(linear_road_refined_path, "w")
with open(linear_road_raw_path) as f:
    lines = f.readlines()
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
fw.close()