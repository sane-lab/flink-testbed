import sys
from pathlib import Path

path_root = Path(__file__).parents[3]
sys.path.append(str(path_root))

from analysis.workloads.breakdown import breakdown_batching_key_size, breakdown_batching_state_size, \
    breakdown_batching_input_rate, breakdown_update_state_size, breakdown_update_state_access_ratio, \
    breakdown_ordering_input_rate, breakdown_ordering_zipf_skew, latency_ordering_input_rate, \
    latency_ordering_zipf_skew, latency_batching_input_rate, latency_batching_key_size, \
    latency_batching_state_size, latency_update_state_size, latency_update_state_access_ratio

if __name__ == '__main__':
    # breakdown_batching_key_size.draw()
    # breakdown_batching_state_size.draw()
    # breakdown_batching_input_rate.draw()
    # breakdown_update_state_size.draw()
    # breakdown_update_state_access_ratio.draw()
    # breakdown_ordering_input_rate.draw()
    breakdown_ordering_zipf_skew.draw()

    
    # latency_batching_input_rate.draw()
    # latency_batching_key_size.draw()
    # latency_batching_state_size.draw()
    # latency_update_state_size.draw()
    # latency_update_state_access_ratio.draw()
    # latency_ordering_input_rate.draw()
    latency_ordering_zipf_skew.draw()