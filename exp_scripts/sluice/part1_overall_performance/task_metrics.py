def calculate_task_metrics(key_task_mapping, key_arrival_rate, key_backlog):
    """
    Calculate task-level arrival rate and backlog based on key-task mapping.

    Parameters:
        key_task_mapping (dict): A dictionary where keys are tasks and values are lists of keys assigned to the task.
        key_arrival_rate (dict): A dictionary where keys are keys and values are their arrival rates.
        key_backlog (dict): A dictionary where keys are keys and values are their backlogs.

    Returns:
        task_arrival_rate (dict): A dictionary where keys are tasks and values are their arrival rates.
        task_backlog (dict): A dictionary where keys are tasks and values are their backlogs.
    """
    task_arrival_rate = {}
    task_backlog = {}

    for task, keys in key_task_mapping.items():
        # Calculate arrival rate and backlog for each task
        task_arrival_rate[task] = sum(key_arrival_rate.get(key, 0) for key in keys)
        task_backlog[task] = sum(key_backlog.get(key, 0) for key in keys)

    return task_arrival_rate, task_backlog

def parse_key_metrics(metrics_string):
    """
    Parse a string of key metrics into a dictionary.

    Parameters:
        metrics_string (str): The string representation of key metrics.

    Returns:
        dict: A dictionary where keys are integers (keys) and values are floats (metrics).
    """
    metrics = {}
    # Remove surrounding braces and split by commas
    pairs = metrics_string.strip("{}").split(", ")
    for pair in pairs:
        # Split each pair by '=' to get key and value
        key, value = pair.split("=")
        metrics[int(key)] = float(value)
    return metrics

def parse_key_task_mapping(input_string):
    key_task_mapping = {}
    mappings = input_string.strip().replace(" ", "").split("],")
    for mapping in mappings:
        # Split each task mapping into the task name and its list of keys
        print(mapping)
        task, keys = mapping.split("=")
        # Strip and clean up the task name and convert keys to a list of integers
        task = task.strip()
        keys = list(map(int, keys.strip("[]").split(",")))
        key_task_mapping[task] = keys
    return key_task_mapping

# Example usage
if __name__ == "__main__":
    mapping_str = """eabd4c11f6c6fbdf011f0f1fc42097b1_27=[83, 73, 55, 98, 93, 64, 69], eabd4c11f6c6fbdf011f0f1fc42097b1_29=[117, 72, 95, 46, 114, 109], eabd4c11f6c6fbdf011f0f1fc42097b1_28=[106, 88, 127, 47, 91, 5], eabd4c11f6c6fbdf011f0f1fc42097b1_22=[31, 105, 20, 115, 44], eabd4c11f6c6fbdf011f0f1fc42097b1_25=[27, 103, 2, 14, 7, 122], eabd4c11f6c6fbdf011f0f1fc42097b1_24=[102, 25, 112, 124, 86, 30, 37], eabd4c11f6c6fbdf011f0f1fc42097b1_3=[16, 17, 18, 78, 79, 80, 23, 10, 0], eabd4c11f6c6fbdf011f0f1fc42097b1_NewTask_1=[19, 67], eabd4c11f6c6fbdf011f0f1fc42097b1_15=[121, 111, 49, 75, 15, 94, 53], eabd4c11f6c6fbdf011f0f1fc42097b1_NewTask_0=[13, 54], eabd4c11f6c6fbdf011f0f1fc42097b1_18=[22, 39, 50, 12, 68, 70], eabd4c11f6c6fbdf011f0f1fc42097b1_17=[29, 32, 119, 51, 6, 57], eabd4c11f6c6fbdf011f0f1fc42097b1_7=[11, 43, 28, 35], eabd4c11f6c6fbdf011f0f1fc42097b1_NewTask_5=[60, 65], eabd4c11f6c6fbdf011f0f1fc42097b1_19=[120, 113, 42], eabd4c11f6c6fbdf011f0f1fc42097b1_9=[96, 97, 1, 77, 24, 26, 52, 92, 62], eabd4c11f6c6fbdf011f0f1fc42097b1_NewTask_4=[9, 34], eabd4c11f6c6fbdf011f0f1fc42097b1_NewTask_3=[58, 108], eabd4c11f6c6fbdf011f0f1fc42097b1_NewTask_2=[66, 61], eabd4c11f6c6fbdf011f0f1fc42097b1_30=[82, 81, 76, 126, 59, 71], eabd4c11f6c6fbdf011f0f1fc42097b1_32=[74, 118, 41, 87, 107, 63], eabd4c11f6c6fbdf011f0f1fc42097b1_31=[101, 84, 116, 100, 40, 36, 99], eabd4c11f6c6fbdf011f0f1fc42097b1_NewTask_6=[38, 56], eabd4c11f6c6fbdf011f0f1fc42097b1_12=[110, 48], eabd4c11f6c6fbdf011f0f1fc42097b1_33=[33, 104, 89, 123, 85, 4, 125, 3], eabd4c11f6c6fbdf011f0f1fc42097b1_14=[45, 90], eabd4c11f6c6fbdf011f0f1fc42097b1_13=[8, 21]"""
    # Example key-task mapping
    key_task_mapping = parse_key_task_mapping(mapping_str)

    key_arrival_rate_str = """{0=0.014500000000000002, 1=0.009000000000000003, 2=0.006999999999999999, 3=0.013000000000000001, 4=0.0075, 5=0.013000000000000003, 6=0.012499999999999997, 7=0.0095, 8=0.011000000000000001, 9=0.009000000000000001, 10=0.008499999999999997, 11=0.008, 12=0.011999999999999999, 13=0.011000000000000001, 14=0.0085, 15=0.009500000000000001, 16=0.005999999999999999, 17=0.009, 18=0.013000000000000001, 19=0.014000000000000002, 20=0.014000000000000002, 21=0.012, 22=0.009499999999999998, 23=0.0085, 24=0.011500000000000003, 25=0.0105, 26=0.015500000000000003, 27=0.009999999999999998, 28=0.009, 29=0.011, 30=0.0075000000000000015, 31=0.01, 32=0.008, 33=0.012, 34=0.009000000000000001, 35=0.0105, 36=0.006999999999999999, 37=0.014000000000000002, 38=0.0105, 39=0.013000000000000001, 40=0.007999999999999998, 41=0.008999999999999998, 42=0.010500000000000002, 43=0.008, 44=0.009, 45=0.01, 46=0.011000000000000001, 47=0.009500000000000001, 48=0.007000000000000001, 49=0.012, 50=0.009000000000000001, 51=0.0105, 52=0.0085, 53=0.013000000000000001, 54=0.007000000000000001, 55=0.013499999999999995, 56=0.008000000000000002, 57=0.0085, 58=0.012, 59=0.006999999999999999, 60=0.011, 61=0.007999999999999998, 62=0.007999999999999998, 63=0.009, 64=0.0125, 65=0.010000000000000002, 66=0.014000000000000002, 67=0.0105, 68=0.013000000000000001, 69=0.008499999999999997, 70=0.009000000000000001, 71=0.0105, 72=0.011, 73=0.010499999999999999, 74=0.011, 75=0.013500000000000002, 76=0.0095, 77=0.006499999999999999, 78=0.011, 79=0.0075, 80=0.009000000000000001, 81=0.0095, 82=0.006499999999999999, 83=0.011500000000000003, 84=0.011500000000000003, 85=0.011, 86=0.011500000000000002, 87=0.009000000000000001, 88=0.0125, 89=0.006499999999999999, 90=0.013000000000000003, 91=0.011, 92=0.0105, 93=0.0115, 94=0.013000000000000001, 95=0.013500000000000002, 96=0.009500000000000001, 97=0.0075, 98=0.011, 99=0.009499999999999998, 100=0.0115, 101=0.009500000000000001, 102=0.010000000000000002, 103=0.0115, 104=0.0115, 105=0.010499999999999999, 106=0.008, 107=0.011, 108=0.011000000000000003, 109=0.0125, 110=0.0125, 111=0.008499999999999999, 112=0.0055, 113=0.010499999999999999, 114=0.011999999999999999, 115=0.006999999999999999, 116=0.010499999999999999, 117=0.01, 118=0.0115, 119=0.014000000000000002, 120=0.009, 121=0.0055, 122=0.009000000000000003, 123=0.012500000000000002, 124=0.008499999999999999, 125=0.009499999999999998, 126=0.0085, 127=0.0115}"""
    # Example key-level arrival rates
    key_arrival_rate = parse_key_metrics(key_arrival_rate_str)

    key_backlog_str = """{0=5.0, 1=0.0, 2=1.0, 3=9.0, 4=11.0, 5=16.0, 6=20.0, 7=22.0, 8=20.0, 9=27.0, 10=4.0, 11=14.0, 12=2.0, 13=28.0, 14=1.0, 15=1.0, 16=3.0, 17=4.0, 18=6.0, 19=32.0, 20=11.0, 21=21.0, 22=1.0, 23=2.0, 24=1.0, 25=1.0, 26=1.0, 27=2.0, 28=10.0, 29=1.0, 30=22.0, 31=11.0, 32=0.0, 33=2.0, 34=12.0, 35=8.0, 36=12.0, 37=18.0, 38=23.0, 39=1.0, 40=16.0, 41=1.0, 42=17.0, 43=12.0, 44=8.0, 45=18.0, 46=1.0, 47=1.0, 48=17.0, 49=1.0, 50=1.0, 51=2.0, 52=1.0, 53=16.0, 54=13.0, 55=2.0, 56=11.0, 57=17.0, 58=26.0, 59=19.0, 60=27.0, 61=19.0, 62=17.0, 63=16.0, 64=22.0, 65=12.0, 66=23.0, 67=10.0, 68=20.0, 69=13.0, 70=17.0, 71=16.0, 72=1.0, 73=1.0, 74=0.0, 75=2.0, 76=1.0, 77=0.0, 78=7.0, 79=3.0, 80=5.0, 81=2.0, 82=1.0, 83=2.0, 84=1.0, 85=0.0, 86=0.0, 87=0.0, 88=1.0, 89=1.0, 90=20.0, 91=22.0, 92=20.0, 93=1.0, 94=20.0, 95=2.0, 96=1.0, 97=1.0, 98=1.0, 99=8.0, 100=2.0, 101=2.0, 102=1.0, 103=2.0, 104=0.0, 105=7.0, 106=1.0, 107=18.0, 108=16.0, 109=14.0, 110=23.0, 111=1.0, 112=0.0, 113=15.0, 114=22.0, 115=6.0, 116=1.0, 117=1.0, 118=2.0, 119=2.0, 120=10.0, 121=1.0, 122=14.0, 123=2.0, 124=0.0, 125=10.0, 126=2.0, 127=1.0}"""
    # Example key-level backlogs
    key_backlog = parse_key_metrics(key_backlog_str)

    # Calculate task metrics
    task_arrival_rate, task_backlog = calculate_task_metrics(key_task_mapping, key_arrival_rate, key_backlog)

    # Display results
    print("Task Arrival Rate:", task_arrival_rate)
    print("Task Backlog:", task_backlog)
