def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
setting2--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-5500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000--1000-3000-100-1-true-1
setting2--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6000-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000--1000-3000-100-1-true-1
setting2--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000--1000-3000-100-1-true-1
setting2--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7000-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000--1000-3000-100-1-true-1
setting2--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-7500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000--1000-3000-100-1-true-1
setting3--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-75-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000--1000-3000-100-1-true-1
setting3--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-60-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000--1000-3000-100-1-true-1
setting3--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000--1000-3000-100-1-true-1
setting3--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-30-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000--1000-3000-100-1-true-1
setting3--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-15-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000--1000-3000-100-1-true-1
setting4--streamsluice-streamsluice-false-true-false-when-sine-1op-720-6500-45-3500-5000-0-1-0-17-2000-1-5000-1-20-1-5000-1-20-1-5000-17-20-5000--1000-3000-100-1-true-1
setting4--streamsluice-streamsluice-false-true-false-when-sine-2op-720-6500-45-3500-5000-0-1-0-1-20-1-5000-17-2000-1-5000-1-20-1-5000-17-20-5000--1000-3000-100-1-true-1
setting4--streamsluice-streamsluice-false-true-false-when-sine-3op-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-17-2000-1-5000-17-20-5000--1000-3000-100-1-true-1
setting4--streamsluice-streamsluice-false-true-false-when-sine-4op-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-2000-5000--1000-3000-100-1-true-1
setting5--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-30-2000-5000--1000-3000-100-1-true-1
setting5--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-30-1333-5000--1000-3000-100-1-true-1
setting5--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-30-800-5000--1000-3000-100-1-true-1
setting5--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-30-666-5000--1000-3000-100-1-true-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-1250-1-20-1-1250-1-20-1-1250-17-1000-1250--1000-3000-100-1-true-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-2500-1-20-1-2500-1-20-1-2500-17-1000-2500--1000-3000-100-1-true-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-1-20-1-10000-1-20-1-10000-17-1000-10000--1000-3000-100-1-true-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-1000-20000--1000-3000-100-1-true-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.1-1000-3000-100-1-true-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.2-1000-3000-100-1-true-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.3-1000-3000-100-1-true-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.4-1000-3000-100-1-true-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)