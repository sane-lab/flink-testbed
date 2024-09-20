def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-250-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-250-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-250-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-250-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-2000-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-2000-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-2000-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-2000-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-4000-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-1-0.5-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-4000-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-1-2.0-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-4000-3000-100-1-true-1
autotune-setting3-true-streamsluice-streamsluice-30-100-400-2-0.2-2-0.8-2-when-gradient-4op_line-660-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-4000-3000-100-1-true-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)