def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-310-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-320-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-330-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-340-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-290-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-310-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-320-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-330-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-340-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-350-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-500-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-750-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-1250-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-1500-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-290-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-310-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-320-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-330-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-340-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-350-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-500-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-750-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-1250-3000-100-1-true-1
setting6-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-1500-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-310-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-320-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-330-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-340-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-290-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-310-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-320-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-330-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-340-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-350-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-500-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-750-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-1250-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-1500-3000-100-1-true-1
setting7-true-streamsluice-streamsluice-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-3-222-1-5000-5-333-1-5000-15-500-5000-1000-3000-100-1-true-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)