def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
part4-microbench-streamsluice-ds2-systemsensitivity-sine-1split2join1-960-1000-3000-50-50-1-0-1-20-1-15000-12-1000-1-15000-12-666-1-15000-1-67-15000--0.05-false-0.5-3000-1000-100-1-false-1
part4-microbench-streamsluice-streamsluice-systemsensitivity-sine-1split2join1-960-1000-3000-50-50-1-0-1-20-1-15000-12-1000-1-15000-12-666-1-15000-1-67-15000--0.05-true-0.5-3000-1000-100-1-true-1
part4-microbench-streamsluice-streamsluice-systemsensitivity-sine-1split2join1-960-1000-3000-50-50-1-0-1-20-1-15000-12-1000-1-15000-12-666-1-15000-1-67-15000--0.05-true-0.5-3000-1000-100-1-true-2
part4-microbench-streamsluice-streamsluice-systemsensitivity-sine-1split2join1-960-1000-3000-50-50-1-0-1-20-1-15000-12-1000-1-15000-12-666-1-15000-1-67-15000--0.05-true-0.5-3000-1000-100-1-true-3
part4-microbench-streamsluice-streamsluice-systemsensitivity-sine-1split2join1-960-1000-3000-50-50-1-0-1-20-1-15000-12-1000-1-15000-12-666-1-15000-1-67-15000--0.05-true-0.5-3000-1000-100-1-true-4
part4-microbench-streamsluice-streamsluice-systemsensitivity-sine-1split2join1-960-1000-3000-50-50-1-0-1-20-1-15000-12-1000-1-15000-12-666-1-15000-1-67-15000--0.05-true-0.5-3000-1000-100-1-true-5
part4-microbench-streamsluice-streamsluice-systemsensitivity-sine-1split2join1-960-1000-3000-50-50-1-0-1-20-1-15000-12-1000-1-15000-12-666-1-15000-1-67-15000--0.05-false-0.5-1000-1000-100-1-true-1
part4-microbench-streamsluice-streamsluice-systemsensitivity-sine-1split2join1-960-1000-3000-50-50-1-0-1-20-1-15000-12-1000-1-15000-12-666-1-15000-1-67-15000--0.05-false-0.5-1000-1000-100-1-true-2
part4-microbench-streamsluice-streamsluice-systemsensitivity-sine-1split2join1-960-1000-3000-50-50-1-0-1-20-1-15000-12-1000-1-15000-12-666-1-15000-1-67-15000--0.05-false-0.5-1000-1000-100-1-true-3
part4-microbench-streamsluice-streamsluice-systemsensitivity-sine-1split2join1-960-1000-3000-50-50-1-0-1-20-1-15000-12-1000-1-15000-12-666-1-15000-1-67-15000--0.05-false-0.5-1000-1000-100-1-true-4
part4-microbench-streamsluice-streamsluice-systemsensitivity-sine-1split2join1-960-1000-3000-50-50-1-0-1-20-1-15000-12-1000-1-15000-12-666-1-15000-1-67-15000--0.05-false-0.5-1000-1000-100-1-true-5
"""


formatted_script = format_to_script(input_string)
print(formatted_script)