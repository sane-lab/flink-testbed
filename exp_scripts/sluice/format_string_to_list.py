def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
setting4--streamsluice-streamsluice-false-true-false-when-sine-1op-720-6500-45-3500-5000-0-1-0-17-2000-1-5000-1-20-1-5000-1-20-1-5000-17-20-5000--750-3000-100-1-true-1
setting4--streamsluice-streamsluice-false-true-false-when-sine-1op-720-6500-45-3500-5000-0-1-0-17-2000-1-5000-1-20-1-5000-1-20-1-5000-17-20-5000--1250-3000-100-1-true-1
setting4--streamsluice-streamsluice-false-true-false-when-sine-2op-720-6500-45-3500-5000-0-1-0-1-20-1-5000-17-2000-1-5000-1-20-1-5000-17-20-5000--750-3000-100-1-true-1
setting4--streamsluice-streamsluice-false-true-false-when-sine-2op-720-6500-45-3500-5000-0-1-0-1-20-1-5000-17-2000-1-5000-1-20-1-5000-17-20-5000--1250-3000-100-1-true-1
setting4--streamsluice-streamsluice-false-true-false-when-sine-3op-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-17-2000-1-5000-17-20-5000--750-3000-100-1-true-1
setting4--streamsluice-streamsluice-false-true-false-when-sine-3op-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-17-2000-1-5000-17-20-5000--1250-3000-100-1-true-1
setting4--streamsluice-streamsluice-false-true-false-when-sine-4op-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-2000-5000--750-3000-100-1-true-1
setting4--streamsluice-streamsluice-false-true-false-when-sine-4op-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-2000-5000--1250-3000-100-1-true-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)