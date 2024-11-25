def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
Setting 1
setting1--streamsluice-ds2-false-true-false-when-sine-1split2join1-690-12500-90-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-false-1
setting1--streamsluice-ds2-false-true-false-when-sine-1split2join1-690-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-false-1
setting1--streamsluice-ds2-false-true-false-when-sine-1split2join1-690-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-false-1
setting1--streamsluice-ds2-false-true-false-when-sine-1split2join1-690-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-false-1
setting1--streamsluice-ds2-false-true-false-when-sine-1split2join1-690-12500-20-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-false-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)