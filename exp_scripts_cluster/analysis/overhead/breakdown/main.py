import sys
from pathlib import Path

path_root = Path(__file__).parents[3]
sys.path.append(str(path_root))

from analysis.overhead.breakdown import breakdown_order_keys, breakdown_sync_keys, breakdown_replicate_keys, \
    breakdown_state_size

if __name__ == '__main__':
    # breakdown_state_size.draw()
    # breakdown_sync_keys.draw()
    # breakdown_replicate_keys.draw()
    breakdown_order_keys.draw()
