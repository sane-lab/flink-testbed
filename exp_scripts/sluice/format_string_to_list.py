def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1
autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-2-1.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1
autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-1-2.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1
autotune-setting1-true-streamsluice-streamsluice-60-100-300-2-0.2-2-1.0-1-when-sine-1split2join1-660-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)