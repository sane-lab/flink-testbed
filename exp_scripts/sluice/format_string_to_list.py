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
setting1-2-true-streamsluice-ds2-false--true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1000-3000-100-1-false-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-90-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-125-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-250-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-1op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1
setting1-2-true-streamsluice-ds2-false--true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-false-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-190-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-225-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-2op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1
setting1-2-true-streamsluice-ds2-false--true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-false-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-290-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-325-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1
setting1-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-10000-600-10000-10000-90-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1
Setting 2
setting2-2-true-streamsluice-ds2-false--true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-false-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-290-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-325-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-10000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1
setting2-2-true-streamsluice-ds2-false--true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-false-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-290-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-325-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-15000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1
setting2-2-true-streamsluice-ds2-false--true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-false-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-290-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-325-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-500-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-750-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1250-3000-100-1-true-1
setting2-2-true-streamsluice-streamsluice-false--true-false-when-linear-3op_line-390-20000-600-10000-5000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-1000-5000-1500-3000-100-1-true-1
Setting 3
setting3-2-true-streamsluice-ds2-false--true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-false-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-325-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-400-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-450-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-60-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1
setting3-2-true-streamsluice-ds2-false--true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-false-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-325-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-400-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-450-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-45-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1
setting3-2-true-streamsluice-ds2-false--true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-false-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-290-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-325-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-400-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-450-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1250-3000-100-1-true-1
setting3-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-12500-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1500-3000-100-1-true-1
Setting 4
setting4-2-true-streamsluice-ds2-false--true-false-when-sine-1split2join1-390-13750-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1500-3000-100-1-false-1
setting4-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-13750-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-350-3000-100-1-true-1
setting4-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-13750-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-500-3000-100-1-true-1
setting4-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-13750-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-750-3000-100-1-true-1
setting4-2-true-streamsluice-streamsluice-false--true-false-when-sine-1split2join1-390-13750-30-7500-10000-0-1-0-1-20-1-5000-1-20-1-5000-17-1000-1-5000-1-20-5000-1000-3000-100-1-true-1
Setting 5
setting5-2-true-streamsluice-ds2-false--true-false-when-gradient-1split2join1-390-13750-45-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-false-1
setting5-2-true-streamsluice-streamsluice-false--true-false-when-gradient-1split2join1-390-13750-45-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-350-3000-100-1-true-1
setting5-2-true-streamsluice-streamsluice-false--true-false-when-gradient-1split2join1-390-13750-45-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-500-3000-100-1-true-1
setting5-2-true-streamsluice-streamsluice-false--true-false-when-gradient-1split2join1-390-13750-45-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-750-3000-100-1-true-1
setting5-2-true-streamsluice-streamsluice-false--true-false-when-gradient-1split2join1-390-13750-45-6250-10000-0-1-0-1-20-1-5000-1-20-1-5000-1-20-1-5000-17-500-5000-1000-3000-100-1-true-1
Setting 6
Setting 7
"""


formatted_script = format_to_script(input_string)
print(formatted_script)