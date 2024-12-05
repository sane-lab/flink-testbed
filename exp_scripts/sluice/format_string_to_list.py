def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
part5-microbench-streamsluice-streamsluice-part5-sine-1split2join1-1890-4000-6000-600-linear-500-2000-1800-stair_4-120-60-1800-stair_4-1-0-1-20-1-5000-2-50-1-5000-1-20-1-5000-17-800-5000--0.05-0.1-4000-3000-100-1-true-1
part5-microbench-streamsluice-streamsluice-part5-sine-1split2join1-1890-4000-6000-600-linear-500-2000-1800-stair_4-120-60-1800-stair_4-1-0-1-20-1-5000-2-50-1-5000-1-20-1-5000-17-800-5000--0.05-0.2-4000-3000-100-1-true-1
part5-microbench-streamsluice-streamsluice-part5-sine-1split2join1-1890-4000-6000-600-linear-500-2000-1800-stair_4-120-60-1800-stair_4-1-0-1-20-1-5000-2-50-1-5000-1-20-1-5000-17-800-5000--0.05-0.4-4000-3000-100-1-true-1
part5-microbench-streamsluice-streamsluice-part5-sine-1split2join1-1890-4000-6000-600-linear-500-2000-1800-stair_4-120-60-1800-stair_4-1-0-1-20-1-5000-2-50-1-5000-1-20-1-5000-17-800-5000--0.05-0.1-5000-3000-100-1-true-1
part5-microbench-streamsluice-streamsluice-part5-sine-1split2join1-1890-4000-6000-600-linear-500-2000-1800-stair_4-120-60-1800-stair_4-1-0-1-20-1-5000-2-50-1-5000-1-20-1-5000-17-800-5000--0.05-0.2-5000-3000-100-1-true-1
part5-microbench-streamsluice-streamsluice-part5-sine-1split2join1-1890-4000-6000-600-linear-500-2000-1800-stair_4-120-60-1800-stair_4-1-0-1-20-1-5000-2-50-1-5000-1-20-1-5000-17-800-5000--0.05-0.4-5000-3000-100-1-true-1
"""


formatted_script = format_to_script(input_string)
print(formatted_script)