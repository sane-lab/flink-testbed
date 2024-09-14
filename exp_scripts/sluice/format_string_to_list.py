def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
system-true-streamsluice-ds2-false-true-true-false-when-linear-1split2join1-390-15000-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-false-1
system-true-streamsluice-ds2-false-true-true-false-when-linear-1split2join1-390-15000-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-false-1
system-true-streamsluice-ds2-false-true-true-false-when-linear-1split2join1-390-15000-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-false-1
system-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-12500-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1000-3000-100-1-false-1
system-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1000-3000-100-1-false-1
system-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-17500-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1000-3000-100-1-false-1
system-true-streamsluice-ds2-false-true-true-false-when-gradient-1split2join1-390-12500-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-false-1
system-true-streamsluice-ds2-false-true-true-false-when-gradient-1split2join1-390-15000-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-false-1
"""

formatted_script = format_to_script(input_string)
print(formatted_script)