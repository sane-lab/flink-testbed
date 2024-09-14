def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
system-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-210-3000-100-1-true-1
system-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-220-3000-100-1-true-1
system-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-230-3000-100-1-true-1
system-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-240-3000-100-1-true-1
system-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-250-3000-100-1-true-1
"""

formatted_script = format_to_script(input_string)
print(formatted_script)