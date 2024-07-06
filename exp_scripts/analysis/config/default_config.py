# Need to substitute the default_config.py with this one.

OPT_FONT_NAME = 'Helvetica'
TICK_FONT_SIZE = 30
LABEL_FONT_SIZE = 34
LEGEND_FONT_SIZE = 26

MARKERS = (['o', 's', 'v', "^", "h", "v", ">", "x", "d", "<", "|", "", "|", "_"])
# you may want to change the color map for different figures
COLOR_MAP = ('#B03A2E', '#2874A6', '#239B56', '#7D3C98', '#F1C40F', '#F5CBA7', '#82E0AA', '#AEB6BF', '#AA4499')
# you may want to change the patterns for different figures
PATTERNS = (["\\", "///", "o", "||", "\\\\", "\\\\", "//////", "//////", ".", "\\\\\\", "\\\\\\"])
LABEL_WEIGHT = 'bold'
LINE_COLORS = COLOR_MAP
LINE_WIDTH = 3.0
MARKER_SIZE = 10.0
MARKER_FREQUENCY = 1000

timers_plot = ["++++++syncTimer", "++++++replicationTimer", "++++++updateTimer"]
breakdown_legend_labels = ['sync', 'replicate', 'update']

FIGURE_FOLDER = '/data/myc/spector-proj/results'
FILE_FOLER = '/data/myc/spector-proj/raw'

per_key_state_size = 32768
replicate_keys_filter = 0
sync_keys = 0
per_task_rate = 5000
state_access_ratio = 2
parallelism = 8
max_parallelism = 1024
order_function = "default"
zipf_skew = 1

repeat_num = 1