import sys
from pathlib import Path

path_root = Path(__file__).parents[3]
sys.path.append(str(path_root))

from analysis.workloads.breakdown import breakdown_batching_key_size

if __name__ == '__main__':
    breakdown_batching_key_size.draw()
