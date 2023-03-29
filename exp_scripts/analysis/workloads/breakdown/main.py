import sys
from pathlib import Path

path_root = Path(__file__).parents[3]
sys.path.append(str(path_root))

from analysis.workloads.breakdown import breakdown_batching_key_size, breakdown_batching_state_size, \
    breakdown_batching_input_rate, breakdown_update_state_size, breakdown_update_state_access_ratio

if __name__ == '__main__':
    # breakdown_batching_key_size.draw()
    # breakdown_batching_state_size.draw()
    # breakdown_batching_input_rate.draw()
    # breakdown_update_state_size.draw()
    breakdown_update_state_access_ratio.draw()
