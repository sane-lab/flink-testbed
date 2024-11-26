def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
setting7--streamsluice-ds2-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.1-1250-3000-100-1-false-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.1-750-3000-100-1--true-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.1-1250-3000-100-1-true-1
setting7--streamsluice-ds2-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.2-1250-3000-100-1-false-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.2-750-3000-100-1-true-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.2-1000-3000-100-1-true-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.2-1250-3000-100-1-true-1
setting7--streamsluice-ds2-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.3-1250-3000-100-1-false-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.3-750-3000-100-1-true-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.3-1000-3000-100-1-true-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.3-1250-3000-100-1-true-1
setting7--streamsluice-ds2-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.4-1250-3000-100-1-false-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.4-750-3000-100-1-true-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.4-1000-3000-100-1-true-1
setting7--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-0.4-1250-3000-100-1-true-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)