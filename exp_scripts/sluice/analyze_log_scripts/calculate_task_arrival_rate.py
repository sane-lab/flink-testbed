import json
from collections import defaultdict
import re

# Example input data
key_arrival_rate_str = "{0=0.034865076772296624, 1=0.024281035609278002, 2=0.03361989545900031, 3=0.039223211368833694, 4=0.03797803005553739, 5=0.020545491669389078, 6=0.026771398235870613, 7=0.025526216922574306, 8=0.03673284874224108, 9=0.03299730480235215, 10=0.016187357072852, 11=0.03548766742894477, 12=0.019922901012740923, 13=0.03548766742894477, 14=0.032374714145704, 15=0.02863917020581508, 16=0.040468392682130004, 17=0.032374714145704, 18=0.025526216922574306, 19=0.026148807579222465, 20=0.03299730480235215, 21=0.032374714145704, 22=0.028016579549166924, 23=0.03112953283240769, 24=0.039845802025481845, 25=0.026771398235870613, 26=0.041090983338778156, 27=0.023035854295981695, 28=0.029884351519111382, 29=0.03299730480235215, 30=0.027393988892518775, 31=0.02863917020581508, 32=0.026148807579222465, 33=0.05229761515844493, 34=0.021790672982685385, 35=0.026771398235870613, 36=0.03797803005553739, 37=0.028016579549166924, 38=0.03299730480235215, 39=0.034865076772296624, 40=0.032374714145704, 41=0.018677719699444616, 42=0.03611025808559293, 43=0.03361989545900031, 44=0.01930031035609277, 45=0.039223211368833694, 46=0.03611025808559293, 47=0.03611025808559293, 48=0.03860062071218554, 49=0.023035854295981695, 50=0.024281035609278002, 51=0.021790672982685385, 52=0.03797803005553739, 53=0.01431958510290754, 54=0.040468392682130004, 55=0.028016579549166924, 56=0.03361989545900031, 57=0.029261760862463234, 58=0.019922901012740923, 59=0.024281035609278002, 60=0.03050694217575954, 61=0.032374714145704, 62=0.023035854295981695, 63=0.034865076772296624, 64=0.039845802025481845, 65=0.022413263639333537, 66=0.03424248611564846, 67=0.020545491669389078, 68=0.03299730480235215, 69=0.025526216922574306, 70=0.028016579549166924, 71=0.029261760862463234, 72=0.032374714145704, 73=0.04295875530872261, 74=0.023658444952629847, 75=0.026148807579222465, 76=0.04544911793531523, 77=0.03860062071218554, 78=0.022413263639333537, 79=0.031752123489055845, 80=0.03050694217575954, 81=0.03361989545900031, 82=0.023658444952629847, 83=0.028016579549166924, 84=0.026771398235870613, 85=0.039223211368833694, 86=0.02116808232603723, 87=0.03673284874224108, 88=0.028016579549166924, 89=0.02863917020581508, 90=0.02116808232603723, 91=0.01930031035609277, 92=0.032374714145704, 93=0.025526216922574306, 94=0.031752123489055845, 95=0.032374714145704, 96=0.023658444952629847, 97=0.024281035609278002, 98=0.03361989545900031, 99=0.034865076772296624, 100=0.023035854295981695, 101=0.018055129042796464, 102=0.027393988892518775, 103=0.024903626265926154, 104=0.031752123489055845, 105=0.029261760862463234, 106=0.024281035609278002, 107=0.03424248611564846, 108=0.032374714145704, 109=0.031752123489055845, 110=0.03548766742894477, 111=0.027393988892518775, 112=0.03299730480235215, 113=0.032374714145704, 114=0.028016579549166924, 115=0.03548766742894477, 116=0.032374714145704, 117=0.026148807579222465, 118=0.023658444952629847, 119=0.029884351519111382, 120=0.019922901012740923, 121=0.026771398235870613, 122=0.039223211368833694, 123=0.03050694217575954, 124=0.022413263639333537, 125=0.028016579549166924, 126=0.028016579549166924, 127=0.016187357072852}"
#key_arrival_rate_str = "{0=5.0, 1=2.0, 2=8.0, 3=4.0, 4=5.0, 5=13.0, 6=3.0, 7=4.0, 8=5.0, 9=4.0, 10=2.0, 11=5.0, 12=4.0, 13=5.0, 14=5.0, 15=4.0, 16=5.0, 17=10.0, 18=8.0, 19=14.0, 20=5.0, 21=4.0, 22=8.0, 23=19.0, 24=17.0, 25=8.0, 26=19.0, 27=7.0, 28=3.0, 29=5.0, 30=4.0, 31=4.0, 32=4.0, 33=6.0, 34=4.0, 35=3.0, 36=4.0, 37=4.0, 38=10.0, 39=4.0, 40=19.0, 41=5.0, 42=4.0, 43=17.0, 44=2.0, 45=6.0, 46=16.0, 47=4.0, 48=5.0, 49=3.0, 50=3.0, 51=3.0, 52=5.0, 53=2.0, 54=5.0, 55=3.0, 56=4.0, 57=10.0, 58=6.0, 59=3.0, 60=5.0, 61=4.0, 62=3.0, 63=4.0, 64=4.0, 65=2.0, 66=14.0, 67=2.0, 68=3.0, 69=6.0, 70=15.0, 71=17.0, 72=17.0, 73=6.0, 74=7.0, 75=7.0, 76=7.0, 77=5.0, 78=3.0, 79=3.0, 80=3.0, 81=4.0, 82=2.0, 83=4.0, 84=4.0, 85=4.0, 86=3.0, 87=11.0, 88=6.0, 89=3.0, 90=3.0, 91=2.0, 92=4.0, 93=3.0, 94=3.0, 95=4.0, 96=16.0, 97=3.0, 98=16.0, 99=4.0, 100=10.0, 101=2.0, 102=3.0, 103=7.0, 104=13.0, 105=8.0, 106=4.0, 107=3.0, 108=9.0, 109=9.0, 110=5.0, 111=3.0, 112=4.0, 113=4.0, 114=4.0, 115=4.0, 116=11.0, 117=3.0, 118=3.0, 119=4.0, 120=2.0, 121=9.0, 122=16.0, 123=4.0, 124=3.0, 125=3.0, 126=4.0, 127=2.0}"
key_task_mapping_str = "{d2336f79a0d60b5a4b16c8769ec82e47_30=[84, 85, 86, 68, 4], d2336f79a0d60b5a4b16c8769ec82e47_38=[108, 109, 74, 66, 75, 25, 12, 88], d2336f79a0d60b5a4b16c8769ec82e47_27=[76, 77, 44, 35, 50, 73, 54], d2336f79a0d60b5a4b16c8769ec82e47_13=[38, 22, 103, 18, 41, 87, 17, 27], d2336f79a0d60b5a4b16c8769ec82e47_46=[33, 48, 31, 32, 53, 120, 98, 100], d2336f79a0d60b5a4b16c8769ec82e47_25=[70, 71, 72, 5, 96, 40, 19, 23], d2336f79a0d60b5a4b16c8769ec82e47_33=[92, 93, 94, 30, 28, 67], d2336f79a0d60b5a4b16c8769ec82e47_22=[62, 63, 3, 113, 112, 26], d2336f79a0d60b5a4b16c8769ec82e47_44=[123, 124, 125, 16, 13, 1, 8], d2336f79a0d60b5a4b16c8769ec82e47_34=[95, 97, 55, 6], d2336f79a0d60b5a4b16c8769ec82e47_23=[64, 65, 36], d2336f79a0d60b5a4b16c8769ec82e47_45=[126, 127, 21, 115, 47, 14, 101, 104], d2336f79a0d60b5a4b16c8769ec82e47_20=[57, 58, 105, 2, 116, 69, 121, 46], d2336f79a0d60b5a4b16c8769ec82e47_42=[117, 118, 119, 59, 9, 99, 89, 43], d2336f79a0d60b5a4b16c8769ec82e47_32=[90, 91, 20, 37, 15, 110, 10, 122], d2336f79a0d60b5a4b16c8769ec82e47_21=[60, 61, 42, 34, 114, 102, 39], d2336f79a0d60b5a4b16c8769ec82e47_28=[78, 79, 80, 56, 107], d2336f79a0d60b5a4b16c8769ec82e47_18=[51, 52, 29, 11, 45, 7, 49], d2336f79a0d60b5a4b16c8769ec82e47_29=[81, 82, 83, 106, 0, 111, 24]}"

# Function to convert the custom formatted string to JSON-compatible format
def convert_to_json_compatible(input_str):
    # Replace = with :
    input_str = input_str.replace("=", ": ")
    # Wrap keys with quotes if they are not already
    input_str = re.sub(r"(\w+):", r'"\1":', input_str)
    # Replace single quotes with double quotes if there are any
    input_str = input_str.replace("'", '"')
    return input_str

# Convert the strings into JSON-compatible format
key_arrival_rate_json = convert_to_json_compatible(key_arrival_rate_str)
key_task_mapping_json = convert_to_json_compatible(key_task_mapping_str)

# Parse the JSON strings into dictionaries
key_arrival_rate = json.loads(key_arrival_rate_json)
key_task_mapping = json.loads(key_task_mapping_json)

# Initialize a dictionary to store the total arrival rate for each task
task_total_arrival_rate = defaultdict(float)

# Compute the total arrival rate for each task
for task, keys in key_task_mapping.items():
    total_rate = sum(key_arrival_rate[str(key)] for key in keys)
    task_total_arrival_rate[task] = total_rate

# Output the total arrival rates for each task
for task, rate in task_total_arrival_rate.items():
    print(f"{task}: {rate}")