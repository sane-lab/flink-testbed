def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
stock-ds2-ds2-1-1950-90-1000-20-1-200-4-2500-1-200-1-500-1-7-3333-1000-100-0.4-false-false-1
stock-ds2-ds2-1-1950-90-1000-20-1-200-11-2500-1-200-2-500-1-15-3333-1000-100-0.4-false-false-1
stock-ds2-ds2-1-1950-90-1000-20-1-200-11-2500-1-200-2-500-1-15-3333-1000-100-0.4-true-false-1
stock-streamswitch-streamswitch-1-1950-90-1000-20-1-200-11-2500-1-200-2-500-1-15-3333-1000-100-0.4-true-false-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-2000-0.1-100-1-0-0.0-true-500-0.8-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-2500-0.1-100-1-0-0.0-true-500-0.8-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-3000-0.1-100-1-0-0.0-true-500-0.8-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-1000-0.2-100-1-0-0.0-true-500-0.8-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-1500-0.2-100-1-0-0.0-true-500-0.8-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-2000-0.2-100-1-0-0.0-true-500-0.8-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-2500-0.2-100-1-0-0.0-true-500-0.8-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-3000-0.2-100-1-0-0.0-true-500-0.8-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-1000-0.4-100-1-0-0.0-true-500-0.8-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-1500-0.4-100-1-0-0.0-true-500-0.8-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-2000-0.4-100-1-0-0.0-true-500-0.8-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-2500-0.4-100-1-0-0.0-true-500-0.8-1
lr-streamsluice-streamsluice-1-1080-150-1300-10-1-50-3-1666-1-50-27-4000-3000-0.4-100-1-0-0.0-true-500-0.8-1
lr-ds2-ds2-1-1080-150-1300-10-1-50-10-1666-2-50-1-4000-2000-0.4-100-1-0-0.0-false-2500-0.8-1
lr-ds2-ds2-1-1080-150-1300-10-1-50-15-1666-4-50-1-4000-2000-0.4-100-1-0-0.0-false-2500-0.8-1
lr-ds2-ds2-1-1080-150-1300-10-1-50-15-1666-4-50-1-4000-2000-0.4-100-1-0-0.0-true-2500-0.8-1
lr-streamswitch-streamswitch-1-1080-150-1300-10-1-50-15-1666-4-50-1-4000-2000-0.4-100-1-0-0.0-true-1000-0.8-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)