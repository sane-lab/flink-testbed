def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-1000-100-true-0.1-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-1500-100-true-0.1-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-2000-100-true-0.1-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-2500-100-true-0.1-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-3000-100-true-0.1-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-3500-100-true-0.1-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-1000-100-true-0.2-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-1500-100-true-0.2-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-2000-100-true-0.2-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-2500-100-true-0.2-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-3000-100-true-0.2-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-3500-100-true-0.2-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-1000-100-true-0.4-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-1500-100-true-0.4-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-2000-100-true-0.4-2
tweet-streamsluice-streamsluice-5-8-1950-90-1500-1-19-6666-9-1000-1-50-1-50-2500-100-true-0.4-2
"""


formatted_script = format_to_script(input_string)
print(formatted_script)