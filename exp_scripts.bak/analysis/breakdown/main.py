import getopt
import os
import sys

cur_dir = os.path.abspath(os.path.dirname(__file__))
pro_dir = os.path.split(cur_dir)[0]
sys.path.append(pro_dir)

import utilities
import breakdown_state_size
# from analysis.breakdown import breakdown_sync_keys, breakdown_replicate_keys, breakdown_order_keys
import breakdown_sync_keys, breakdown_replicate_keys, breakdown_order_keys


if __name__ == '__main__':
    val = utilities.init()

    # try:
    #     opts, args = getopt.getopt(sys.argv[1:], '-t::h', ['reconfig type', 'help'])
    # except getopt.GetoptError:
    #     print('breakdown_parallelism.py -t type')
    #     sys.exit(2)
    # for opt, opt_value in opts:
    #     if opt in ('-h', '--help'):
    #         print("[*] Help info")
    #         exit()
    #     elif opt == '-t':
    #         print('Reconfig Type:', opt_value)
    #         val[6] = str(opt_value)
    #
    # val_list = list(val)
    # val_list[-3] = "remap"
    # val_list[1] = 5000
    # val_list[3] = 10000
    # val = tuple(val_list)
    # breakdown_parallelism.draw(val)
    # breakdown_state_size.draw(val)
    breakdown_sync_keys.draw(val)
    # breakdown_replicate_keys.draw(val)
    # breakdown_order_keys.draw(val)
    # breakdown_arrival_rate.draw(val)
    # breakdown_affected_tasks.draw(val)
