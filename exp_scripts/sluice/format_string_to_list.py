def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
lr-streamsluice-streamsluice--780-150-1300-10-1-50-1-50-1-50-36-2000-1000-0.1-100-1-0-0.0-false-3000-1
lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-1000-0.1-100-1-0-0.0-true-3000-1
lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-2000-0.1-100-1-0-0.0-true-3000-1
lr-streamsluice-streamsluice-1-780-150-1300-10-1-50-1-50-1-50-36-2000-4000-0.1-100-1-0-0.0-true-3000-1
lr-streamsluice-streamsluice-2-780-150-1300-10-1-50-1-50-1-50-36-2000-1000-0.1-100-1-0-0.0-true-3000-1
lr-streamsluice-streamsluice-2-780-150-1300-10-1-50-1-50-1-50-36-2000-2000-0.1-100-1-0-0.0-true-3000-1
lr-streamsluice-streamsluice-2-780-150-1300-10-1-50-1-50-1-50-36-2000-4000-0.1-100-1-0-0.0-true-3000-1
lr-streamsluice-streamsluice-0-780-150-1300-10-1-50-1-50-1-50-36-2000-1000-0.1-100-1-0-0.0-true-3000-1
lr-streamsluice-streamsluice-0-780-150-1300-10-1-50-1-50-1-50-36-2000-2000-0.1-100-1-0-0.0-true-3000-1
lr-streamsluice-streamsluice-0-780-150-1300-10-1-50-1-50-1-50-36-2000-4000-0.1-100-1-0-0.0-true-3000-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)