def format_to_script(input_string):
    # Split the input string into lines
    lines = input_string.strip().split('\n')

    # Wrap each line in quotes and join them with commas
    formatted_lines = ',\n'.join([f'"{line.strip()}"' for line in lines])

    # Return the final script-like string
    return formatted_lines


# Example usage
input_string = """
part6and7-microbench-streamsluice-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-ds2-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-ds2-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-ds2-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-dhalion-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-dhalion-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-dhalion-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamswitch-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamswitch-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamswitch-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-ds2-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-ds2-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-ds2-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-dhalion-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-dhalion-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-dhalion-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamswitch-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamswitch-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamswitch-streamsluice-part6-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-ds2-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-ds2-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-ds2-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-dhalion-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-dhalion-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-dhalion-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamswitch-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamswitch-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamswitch-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-ds2-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-ds2-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-ds2-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-dhalion-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-dhalion-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-dhalion-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamswitch-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamswitch-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamswitch-streamsluice-part6-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-ds2-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-ds2-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-ds2-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-dhalion-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-dhalion-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-dhalion-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamswitch-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamswitch-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamswitch-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-ds2-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-ds2-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-ds2-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-dhalion-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-dhalion-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-dhalion-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamswitch-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamswitch-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamswitch-streamsluice-part6-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-ds2-part7-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-ds2-part7-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-ds2-part7-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-drs-part7-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-drs-part7-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-drs-part7-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-streamswitch-part7-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-streamswitch-part7-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-streamswitch-part7-linear-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-ds2-part7-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-ds2-part7-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-ds2-part7-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-drs-part7-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-drs-part7-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-drs-part7-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-streamswitch-part7-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-streamswitch-part7-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-streamswitch-part7-linear-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-ds2-part7-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-ds2-part7-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-ds2-part7-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-drs-part7-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-drs-part7-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-drs-part7-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-streamswitch-part7-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-streamswitch-part7-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-streamswitch-part7-sine-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-ds2-part7-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-ds2-part7-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-ds2-part7-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-drs-part7-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-drs-part7-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-drs-part7-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-streamswitch-part7-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-streamswitch-part7-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-streamswitch-part7-sine-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-ds2-part7-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-ds2-part7-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-ds2-part7-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-drs-part7-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-drs-part7-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-drs-part7-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-streamswitch-part7-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-streamswitch-part7-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-streamswitch-part7-gradient-1split2join1-150-5000-5000-960-linear-2000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-ds2-part7-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-ds2-part7-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-ds2-part7-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-drs-part7-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-drs-part7-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-drs-part7-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
part6and7-microbench-streamsluice-streamswitch-part7-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-1
part6and7-microbench-streamsluice-streamswitch-part7-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-2
part6and7-microbench-streamsluice-streamswitch-part7-gradient-1split2join1-150-5000-5000-960-linear-1000-1-1440-stair_3-120-1-1440-stair_3-1-0-1-20-1-1-2-50-1-1-1-20-1-1-17-800-1--0.05-0.1-1000-3000-100-1-true-3
"""


formatted_script = format_to_script(input_string)
print(formatted_script)