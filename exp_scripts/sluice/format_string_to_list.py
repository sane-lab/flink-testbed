def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-3000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-4000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-5000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-10000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-20000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-12000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-3000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-12000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-4000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-12000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-5000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-12000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-10000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-12000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-20000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-3000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-4000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-5000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-10000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-10000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-20000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-3000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-4000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-5000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-10000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-11000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-20000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-12000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-3000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-12000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-4000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-12000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-5000-3000-100-1-true-1
autotune-setting15-true-streamsluice-streamsluice-60-100-300-1-2.0-3-0.25-changing_amplitude_and_period-sine-1split2join1-1260-14000-60-12000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-10000-3000-100-1-true-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)