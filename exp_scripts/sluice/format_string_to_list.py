def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
stock-streamsluice-streamsluice--1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1000-100-0.1-false-false-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-750-100-0.1-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1000-100-0.1-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1500-100-0.1-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-2000-100-0.1-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-2500-100-0.1-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-750-100-0.2-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1000-100-0.2-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1500-100-0.2-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-2000-100-0.2-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-2500-100-0.2-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-750-100-0.4-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1000-100-0.4-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-1500-100-0.4-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-2000-100-0.4-true-true-1
stock-streamsluice-streamsluice-1-1950-90-1000-20-1-200-15-2500-1-200-2-500-1-21-3333-2500-100-0.4-true-true-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)