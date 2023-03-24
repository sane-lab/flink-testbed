import utilities
from analysis.overhead.breakdown import breakdown_order_keys
from analysis.workloads.breakdown import breakdown_parallelism

if __name__ == '__main__':
    val = utilities.init()

    breakdown_parallelism.draw(val)
