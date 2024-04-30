import math
import os
import pandas as pd
import numpy as np


total_hour = 3
import random
random.seed(12345)

def generate_rate_per_second():
    average_rate_per_second = []

    # Generate curve
    # c1 = 0.43
    # c2 = 13
    # c3 = -0.32
    # c4 = 32.3
    c1 = 0.34
    c2 = 13
    c3 = -0.07
    c4 = 34

    average_base_rate = 2000

    print(c1, c2, c3, c4)
    # Generate per minute number
    for i in range(0, total_hour * 60 * 60):
        # add curve
        base_rate = c1 * math.sin(c2 * i / (total_hour * 60.0 * 60)) + c3 * math.cos(c4 * i / (total_hour * 60.0 * 60)) + 1

        # add noise
        noise = np.random.normal(0, 1) * random.randint(0, 100)/50.0

        # add spike
        spike = 0
        if random.randint(0, 1000) <= 2:
            spike = random.randint(100, 200)/100.0

        if noise <= -10:
            noise = -9.9
        average_rate = (base_rate * (1 + noise/30.0) + spike) * average_base_rate
        average_rate_per_second.append(average_rate)

    # print(average_rate_per_second)
    # import matplotlib
    # #matplotlib.use('Agg')
    # import matplotlib.pyplot as plt
    # plt.plot(np.arange(0, total_hour * 60 * 60), average_rate_per_second, "*")
    # plt.show()
    return average_rate_per_second

def read_raw_data(raw_election_path, raw_bitcoin_path):
    # read raw dataset
    exist_user_id = []
    exist_content = []

    import csv
    f1_in = open(raw_election_path, newline="")
    f2_in = open(raw_bitcoin_path, newline="")

    reader1 = csv.reader(f1_in, delimiter=';')
    reader2 = csv.reader(f2_in, delimiter=';')

    print("Reading election data...")
    N=1000000
    for row in reader1:
        coded_id = row[1].encode("ascii", errors="ignore").decode()
        exist_user_id.append(coded_id)
        if N <= 0:
            break
        N-=1
    f1_in.close()

    print("Reading bitcoin data...")
    N=10000000
    for row in reader2:
        if(N<=0):
            break
        N-=1
        if(len(row) > 8):
            coded_id = row[0].encode("ascii", errors="ignore").decode()
            exist_user_id.append(coded_id)
            coded_content = row[8].encode("ascii", errors="ignore").decode()
            if(len(coded_content) > 0):
                exist_content.append(coded_content)
    f2_in.close()

    return exist_user_id, exist_content

def generate_data(generated_data_path: str, user_ids: list[str], contents: list[str], rate_per_second: list[float]):
    f_out = open(generated_data_path, "w", newline="")
    interval = 50
    tweet_id = 1000000000000
    for i in range(0, len(rate_per_second)):
        number = rate_per_second[i]
        if(i % 300 == 0):
            print("Generating " + str(i/60) + " minute records...")
        for interval_index in range(0, int(1000/interval)):
            interval_number = int(number / (1000/interval))
            for j in range(0, interval_number):
                tweet_id += 1
                user_id = user_ids[random.randint(0, len(user_ids) - 1)]
                content = contents[random.randint(0, len(contents) - 1)]
                time_stamp = i
                follower_count = random.randint(0, len(user_ids))
                f_out.write(str(tweet_id) + "," + user_id + "," + content.replace("\r", "  ").replace("\n", "  ").replace(",", " ") + "," + str(time_stamp) + "," + str(follower_count) + "\n")
            f_out.write("END\n")
    f_out.close()

def check_data(generated_data_path: str):
    f_in = open(generated_data_path, newline="")
    for row in f_in:
        splits = row.rstrip("\n").split(",")
        if(len(splits) < 5 and splits[0] != "END"):
            print("!!!! error " + row)
        if(len(splits) == 5 and (len(splits[0]) == 0 or len(splits[1]) == 0 or len(splits[2]) == 0 or len(splits[3]) == 0 or len(splits[4]) == 0)):
            print("!!!! error " + row)



raw_election_path = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/tweet/tweet_data/uselection_tweets_1jul_11nov.csv"
raw_bitcoin_path = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/tweet/tweet_data/bitcoin.csv"
generated_data_path = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/tweet/tweet_data/3hr.txt"

user_ids, contents = read_raw_data(raw_election_path, raw_bitcoin_path)
rate_per_minute = generate_rate_per_second()
generate_data(generated_data_path, user_ids, contents, rate_per_minute)
check_data(generated_data_path)
