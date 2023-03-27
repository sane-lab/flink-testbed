import utilities
from analysis.workloads.breakdown import breakdown_batching_key_size

if __name__ == '__main__':
    val = utilities.init()

    breakdown_batching_key_size.draw(val)
