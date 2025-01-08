def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
part4-microbench-5-1.2-1-8-systemsensitivity-sine-1split2join1-1260-2500-7500-60-30-1-0-1-20-1-10000-14-2050-1-10000-1-20-1-10000-1-10-10000-0.05-true-0.5-3000-500-100-1-true-3
part4-microbench-5-1.2-1-8-systemsensitivity-sine-1split2join1-1260-2500-7500-60-30-1-0-1-20-1-10000-14-2050-1-10000-1-20-1-10000-1-10-10000-0.05-true-0.5-3000-500-100-1-true-4
part4-microbench-5-1.2-1-8-systemsensitivity-sine-1split2join1-1260-2500-7500-60-30-1-0-1-20-1-10000-14-2050-1-10000-1-20-1-10000-1-10-10000-0.05-true-0.5-3000-500-100-1-true-5
part4-microbench-5-1.2-1-8-systemsensitivity-sine-1split2join1-1260-2500-7500-60-30-1-0-1-20-1-10000-14-2050-1-10000-1-20-1-10000-1-10-10000-0.05-true-0.75-3000-500-100-1-true-1
part4-microbench-5-1.2-1-8-systemsensitivity-sine-1split2join1-1260-2500-7500-60-30-1-0-1-20-1-10000-14-2050-1-10000-1-20-1-10000-1-10-10000-0.05-true-0.75-3000-500-100-1-true-2
part4-microbench-5-1.2-1-8-systemsensitivity-sine-1split2join1-1260-2500-7500-60-30-1-0-1-20-1-10000-14-2050-1-10000-1-20-1-10000-1-10-10000-0.05-true-0.75-3000-500-100-1-true-3
part4-microbench-5-1.2-1-8-systemsensitivity-sine-1split2join1-1260-2500-7500-60-30-1-0-1-20-1-10000-14-2050-1-10000-1-20-1-10000-1-10-10000-0.05-true-0.75-3000-500-100-1-true-4
part4-microbench-5-1.2-1-8-systemsensitivity-sine-1split2join1-1260-2500-7500-60-30-1-0-1-20-1-10000-14-2050-1-10000-1-20-1-10000-1-10-10000-0.05-true-0.75-3000-500-100-1-true-5
"""


formatted_script = format_to_script(input_string)
print(formatted_script)