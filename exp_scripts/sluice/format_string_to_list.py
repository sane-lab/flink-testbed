def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-15-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-15-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-15-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-15-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-15-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-30-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-30-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-30-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-30-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-30-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-60-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-60-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-60-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-60-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-60-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-75-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-75-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-75-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-75-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting3-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-75-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-1op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-20-1-20000-17-20-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-1op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-20-1-20000-17-20-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-1op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-20-1-20000-17-20-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-1op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-20-1-20000-17-20-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-1op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-20-1-20000-17-20-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-2op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-17-20-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-2op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-17-20-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-2op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-17-20-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-2op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-17-20-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-2op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-17-20-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-3op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-1-20-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-3op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-1-20-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-3op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-1-20-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-3op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-1-20-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-3op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-1-20-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-4op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-1-20-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-4op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-1-20-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-4op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-1-20-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-4op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-1-20-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting4-streamsluice-streamsluice-false-true-false-when-sine-4op-720-6500-45-3500-5000-0-1-17-1500-1-20000-17-1000-1-20000-1-20-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-2000-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-2000-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-2000-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-2000-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-2000-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-1333-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-1333-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-1333-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-1333-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-1333-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-1000-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-666-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-666-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-666-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-666-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-666-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-500-1-20000-1-20-20000-0-2000-1000-100-1-true-1
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-500-1-20000-1-20-20000-0-2000-1000-100-1-true-2
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-500-1-20000-1-20-20000-0-2000-1000-100-1-true-3
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-500-1-20000-1-20-20000-0-2000-1000-100-1-true-4
workload-setting5-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-24-500-1-20000-1-20-20000-0-2000-1000-100-1-true-5
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0.1-2000-1000-100-1-true-1
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0.1-2000-1000-100-1-true-2
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0.1-2000-1000-100-1-true-3
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0.2-2000-1000-100-1-true-1
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0.2-2000-1000-100-1-true-2
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0.2-2000-1000-100-1-true-3
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0.4-2000-1000-100-1-true-1
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0.4-2000-1000-100-1-true-2
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0.4-2000-1000-100-1-true-3
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0.8-2000-1000-100-1-true-1
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0.8-2000-1000-100-1-true-2
workload-setting7-streamsluice-streamsluice-false-true-false-when-sine-1split2join1-720-6500-45-3500-5000-0-1-1-20-1-20000-17-1500-1-20000-17-1000-1-20000-1-20-20000-0.8-2000-1000-100-1-true-3
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-50-60-0.5-1-true-1
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-50-60-0.5-1-true-2
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-50-60-0.5-1-true-3
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-50-60-0.5-1-true-4
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-50-60-0.5-1-true-5
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-100-60-0.5-1-true-1
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-100-60-0.5-1-true-2
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-100-60-0.5-1-true-3
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-100-60-0.5-1-true-4
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-100-60-0.5-1-true-5
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-200-60-0.5-1-true-1
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-200-60-0.5-1-true-2
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-200-60-0.5-1-true-3
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-200-60-0.5-1-true-4
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-200-60-0.5-1-true-5
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-1000-60-0.5-1-true-1
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-1000-60-0.5-1-true-2
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-1000-60-0.5-1-true-3
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-1000-60-0.5-1-true-4
system_d2--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-3-500-1-20000-1-20-1-20000-17-1000-20000-2000-500-1000-60-0.5-1-true-5
system_d4--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-12-1000-1-20000-12-666-1-20000-1-20-20000-3000-500-100-15-0.5-1-true-4
system_d4--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-12-1000-1-20000-12-666-1-20000-1-20-20000-3000-500-100-15-0.5-1-true-5
system_d4--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-12-1000-1-20000-12-666-1-20000-1-20-20000-3000-500-100-30-0.5-1-true-4
system_d4--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-12-1000-1-20000-12-666-1-20000-1-20-20000-3000-500-100-30-0.5-1-true-5
system_d4--streamsluice-streamsluice-false-true-false-systemsensitivity-sine-1split2join1-900-6000-30-3000-5000-0-1-0-1-20-1-20000-12-1000-1-20000-12-666-1-20000-1-20-20000-3000-500-100-60-0.5-1-true-4
"""


formatted_script = format_to_script(input_string)
print(formatted_script)