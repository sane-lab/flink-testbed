def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-60-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-60-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-325-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-60-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-60-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-60-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-60-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-60-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-60-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-325-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-325-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1
setting3-true-streamsluice-streamsluice-false-true-true-false-when-linear-1split2join1-390-15000-30-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1
"""

formatted_script = format_to_script(input_string)
print(formatted_script)