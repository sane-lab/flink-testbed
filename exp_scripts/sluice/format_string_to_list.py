def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-1000-0.1-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-1500-0.1-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-2000-0.1-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-2500-0.1-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-3000-0.1-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-1000-0.2-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-1500-0.2-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-2000-0.2-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-2500-0.2-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-3000-0.2-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-1000-0.4-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-1500-0.4-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-2000-0.4-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-2500-0.4-100-1-0-0.0-true-500-0.8-2
lr-streamsluice-streamsluice-1-1980-150-1300-10-1-50-3-1000-1-50-27-2500-3000-0.4-100-1-0-0.0-true-500-0.8-2
"""


formatted_script = format_to_script(input_string)
print(formatted_script)