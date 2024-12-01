def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-750-3000-25-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-1000-3000-25-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-1250-3000-25-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-750-3000-50-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-1000-3000-50-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-1250-3000-50-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-750-3000-100-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-1000-3000-100-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-1250-3000-100-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-750-3000-200-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-1000-3000-200-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-1250-3000-200-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-750-3000-500-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-1000-3000-500-60-0.1-1-true-1
system_d1--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-10000-1250-3000-500-60-0.1-1-true-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)