import sys
from pathlib import Path

path_root = Path(__file__).parents[3]
sys.path.append(str(path_root))

from analysis.overview.breakdown import breakdown_latency_overview, breakdown_completion_time_overview
if __name__ == '__main__':
    # breakdown_latency_overview.draw()
    breakdown_completion_time_overview.draw()
