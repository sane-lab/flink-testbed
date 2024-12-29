def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
workload-setting1-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-25-3000-5000-20-1-0-1-20-1-15000-17-1500-1-15000-17-1000-1-15000-1-20-15000--2000-1000-100-1-true-1
workload-setting1-streamsluice-streamsluice-false-true-false-when-linear-1split2join1-720-7000-25-3000-5000-20-1-0-1-20-1-15000-17-1500-1-15000-17-1000-1-15000-1-20-15000--2000-1000-100-1-true-1
workload-setting1-streamsluice-streamsluice-false-true-false-when-gradient-1split2join1-720-7000-25-3000-5000-20-1-0-1-20-1-15000-17-1500-1-15000-17-1000-1-15000-1-20-15000--2000-1000-100-1-true-1
workload-setting2-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-5500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting2-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6000-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting2-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting2-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting2-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-90-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-60-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-30-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-15-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-10-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-1op-720-6500-45-3500-5000-0-1-0-17-1500-1-10000-17-20-1-10000-17-20-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-2op-720-6500-45-3500-5000-0-1-0-17-1500-1-10000-17-1000-1-10000-17-20-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-4op-720-6500-45-3500-5000-0-1-0-1-20-1-10000-1-20-1-10000-17-1500-1-10000-17-1000-10000--2000-1000-100-1-true-1
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-24-3333-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-24-2000-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-24-1333-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-24-800-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-24-666-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-24-500-1-10000-1-20-10000--2000-1000-100-1-true-1
workload-setting6-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-0-17-1500-1-0-17-1000-1-0-1-20-0--2000-1000-100-1-true-1
workload-setting6-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-17-1500-1-5000-17-1000-1-5000-1-20-5000--2000-1000-100-1-true-1
workload-setting6-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000--2000-1000-100-1-true-1
workload-setting6-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-40000-17-1500-1-40000-17-1000-1-40000-1-20-40000--2000-1000-100-1-true-1
workload-setting6-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-80000-17-1500-1-80000-17-1000-1-80000-1-20-80000--2000-1000-100-1-true-1
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000-0.1-2000-1000-100-1-true-1
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000-0.2-2000-1000-100-1-true-1
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000-0.4-2000-1000-100-1-true-1
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-17-1500-1-10000-17-1000-1-10000-1-20-10000-0.8-2000-1000-100-1-true-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)