def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
part6and7-microbench-streamsluice-ds2-800-part6-linear-1split2join1-120-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-false-1
part6and7-microbench-streamsluice_later-ds2-800-part6-linear-1split2join1-100-4000-4000-960-linear-1000-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-false-3
part6and7-microbench-streamsluice-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1000-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1
part6and7-microbench-streamsluice-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1000-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2
part6and7-microbench-streamsluice-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1000-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-3
part6and7-microbench-streamsluice_earlier-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1000-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1
part6and7-microbench-streamsluice_earlier-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1000-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2
part6and7-microbench-streamsluice_earlier-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1000-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-3
part6and7-microbench-streamsluice_earlier-ds2-800-part6-linear-1split2join1-100-4000-4000-960-linear-1250-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-false-3
part6and7-microbench-streamsluice-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1250-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1
part6and7-microbench-streamsluice-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1250-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2
part6and7-microbench-streamsluice-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1250-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-3
part6and7-microbench-streamsluice_earlier-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1250-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1
part6and7-microbench-streamsluice_earlier-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1250-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2
part6and7-microbench-streamsluice_earlier-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1250-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-3
part6and7-microbench-streamsluice_earlier-ds2-800-part6-linear-1split2join1-100-4000-4000-960-linear-1500-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-false-3
part6and7-microbench-streamsluice-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1500-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1
part6and7-microbench-streamsluice-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1500-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2
part6and7-microbench-streamsluice-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1500-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-3
part6and7-microbench-streamsluice_earlier-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1500-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1
part6and7-microbench-streamsluice_earlier-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1500-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2
part6and7-microbench-streamsluice_earlier-streamsluice-800-part6-linear-1split2join1-100-4000-4000-960-linear-1500-1-1440-stair_3-40-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-3
part6and7-microbench-streamsluice-streamsluice_more-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1
part6and7-microbench-streamsluice-streamsluice_more-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2
part6and7-microbench-streamsluice-streamsluice_more-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-3
part6and7-microbench-streamsluice-streamsluice_less-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1
part6and7-microbench-streamsluice-streamsluice_less-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2
part6and7-microbench-streamsluice-streamsluice_less-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-3
part6and7-microbench-streamsluice-streamsluice_minus_one-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1
part6and7-microbench-streamsluice-streamsluice_minus_one-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2
part6and7-microbench-streamsluice-streamsluice_minus_one-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-3
part6and7-microbench-streamsluice-streamsluice_no_balance-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1
part6and7-microbench-streamsluice-streamsluice_no_balance-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2
part6and7-microbench-streamsluice-streamsluice_no_balance-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-3
part6and7-microbench-streamsluice-streamsluice_not_bottleneck-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1
part6and7-microbench-streamsluice-streamsluice_not_bottleneck-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2
part6and7-microbench-streamsluice-streamsluice_not_bottleneck-800-part7-linear-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-3
part6and7-microbench-streamsluice-streamsluice_more-800-part7-sine-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-1
part6and7-microbench-streamsluice-streamsluice_more-800-part7-sine-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-2
part6and7-microbench-streamsluice-streamsluice_more-800-part7-sine-1split2join1-100-4000-4000-960-linear-2000-1-1440-stair_3-80-1-1440-stair_3-1-0-3-444-1-5000-3-444-1-5000-3-444-1-5000-5-500-5000-0.00-0.1-2000-3000-100-10-true-3
"""


formatted_script = format_to_script(input_string)
print(formatted_script)