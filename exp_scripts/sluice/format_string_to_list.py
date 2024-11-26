def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
setting6--streamsluice-ds2-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-1250-1-20-1-1250-1-20-1-1250-17-1000-1250-1250-3000-100-1-false-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-1250-1-20-1-1250-1-20-1-1250-17-1000-1250-750-3000-100-1-true-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-1250-1-20-1-1250-1-20-1-1250-17-1000-1250-1000-3000-100-1-true-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-1250-1-20-1-1250-1-20-1-1250-17-1000-1250-1250-3000-100-1-true-1
setting6--streamsluice-ds2-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-2500-1-20-1-2500-1-20-1-2500-17-1000-2500-1250-3000-100-1-false-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-2500-1-20-1-2500-1-20-1-2500-17-1000-2500-750-3000-100-1-true-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-2500-1-20-1-2500-1-20-1-2500-17-1000-2500-1000-3000-100-1-true-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-2500-1-20-1-2500-1-20-1-2500-17-1000-2500-1250-3000-100-1-true-1
setting6--streamsluice-ds2-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-1-20-1-10000-1-20-1-10000-17-1000-10000-1250-3000-100-1-false-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-1-20-1-10000-1-20-1-10000-17-1000-10000-750-3000-100-1-true-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-1-20-1-10000-1-20-1-10000-17-1000-10000-1000-3000-100-1-true-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-10000-1-20-1-10000-1-20-1-10000-17-1000-10000-1250-3000-100-1-true-1
setting6--streamsluice-ds2-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-1000-20000-1250-3000-100-1-false-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-1000-20000-750-3000-100-1-true-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-1000-20000-1000-3000-100-1-true-1
setting6--streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-0-1-20-1-20000-1-20-1-20000-1-20-1-20000-17-1000-20000-1250-3000-100-1-true-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)