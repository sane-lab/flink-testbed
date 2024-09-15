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
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-90-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-125-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-250-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-190-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-225-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-290-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-325-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1
setting1-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1
Setting 2
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-290-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-325-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-290-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-325-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-290-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-325-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1
setting2-true-streamsluice-streamsluice-false-true-true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1
Setting 3
Setting 4
Setting 5
Setting 6
setting6-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-false-1
setting6-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-10000-1000-3000-100-1-false-1
setting6-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-15000-1000-3000-100-1-false-1
Setting 7
setting7-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-false-1
setting7-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-1-20-1-5000-5-333-1-5000-15-500-5000-1000-3000-100-1-false-1
setting7-true-streamsluice-ds2-false-true-true-false-when-sine-1split2join1-390-15000-45-5000-10000-0-1-0-1-20-1-5000-3-222-1-5000-5-333-1-5000-15-500-5000-1000-3000-100-1-false-1
"""

formatted_script = format_to_script(input_string)
print(formatted_script)