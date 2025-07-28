#!/bin/bash

# Enhanced monitoring with improved accuracy for CPU cycle measurements
# - 80% sampling coverage (0.8s perf sampling every 1s) vs previous 20% coverage
# - Per-second cycle rates for better comparison across experiments  
# - Cache miss rate for memory efficiency analysis
# - Configurable timing parameters for fine-tuning accuracy vs overhead

source config-server-lr-local.sh

# Define the process names to monitor
PROCESS_NAMES=("StandaloneSessionClusterEntrypoint" "TaskManagerRunner")
MONITOR_LOG_DIR="${FLINK_DIR}/log"
MONITOR_LOG_FILE="${MONITOR_LOG_DIR}/monitor_$(date +%Y%m%d_%H%M%S).out"

# Monitoring configuration for higher accuracy
MONITOR_INTERVAL=1        # Main monitoring loop interval (seconds)
PERF_SAMPLE_TIME=0.8      # Perf sampling duration (seconds) 
PERF_SLEEP_TIME=0.2       # Sleep between perf samples

# Create monitor log directory
mkdir -p $MONITOR_LOG_DIR

# Function to get PIDs of Flink processes using jps
get_flink_pids() {
    local pids=()
    for process_name in "${PROCESS_NAMES[@]}"; do
        pids+=($(jps | grep "$process_name" | awk '{print $1}'))
    done
    echo "${pids[@]}"
}

# Simple monitoring function for CPU cycles and GC time
start_simple_monitoring() {
    echo "INFO: Starting simple monitoring for CPU cycles and GC time..."
    
    # Initialize accumulator file for total cycles calculation
    TOTAL_CYCLES_FILE="${MONITOR_LOG_DIR}/total_cycles_${EXP_NAME}.txt"
    echo "# Total CPU cycles accumulator for overhead calculation" > $TOTAL_CYCLES_FILE
    echo "# Format: timestamp,total_cycles_so_far,interval_cycles,total_instructions,gc_time_total" >> $TOTAL_CYCLES_FILE
    
    {
        # Header for monitoring log (streamlined for CPU cycles and GC time only)
        echo "Timestamp, PID, Process Name, Heap Used (MB), GC Time (ms), Interval Cycles, Total Cycles, Instructions, IPC, Cache Misses"
        
        # Initialize running totals
        TOTAL_CYCLES_ACCUMULATED=0
        TOTAL_INSTRUCTIONS_ACCUMULATED=0
        TOTAL_GC_TIME=0
        
        while true; do
            TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
            PIDS=$(get_flink_pids)
            
            INTERVAL_CYCLES_SUM=0
            INTERVAL_INSTRUCTIONS_SUM=0
            INTERVAL_GC_SUM=0

            for PID in $PIDS; do
                # Get JVM GC time using jstat (faster, get this first)
                if command -v jstat &> /dev/null; then
                    JVM_STATS=$(jstat -gc $PID 2>/dev/null | tail -1 | awk '{print ($3+$4)/1024, ($9+$10)}' || echo "0 0")
                    HEAP_USED=$(echo $JVM_STATS | awk '{print $1}') # Heap Used in MB
                    GC_TIME=$(echo $JVM_STATS | awk '{print $2}')   # GC Time in ms
                    INTERVAL_GC_SUM=$(echo "$INTERVAL_GC_SUM + $GC_TIME" | bc -l 2>/dev/null || echo "$GC_TIME")
                else
                    HEAP_USED="N/A"
                    GC_TIME="0"
                fi

                # Get CPU cycles using perf stat (this is the expensive operation)
                if command -v perf &> /dev/null; then
                    PERF_OUTPUT=$(perf stat -p $PID -e cycles,instructions,cache-misses sleep 2 2>&1)
                    PERF_EXIT_CODE=$?
                    
                    # DEBUG: Log raw perf output to help diagnose parsing issues
                    echo "[$TIMESTAMP] PID $PID perf output (exit $PERF_EXIT_CODE):" >> "${MONITOR_LOG_DIR}/perf_debug.log"
                    echo "$PERF_OUTPUT" >> "${MONITOR_LOG_DIR}/perf_debug.log"
                    echo "---" >> "${MONITOR_LOG_DIR}/perf_debug.log"
                    
                    # Extract metrics with multiple parsing approaches (robust fallback)
                    INTERVAL_CYCLES=$(echo "$PERF_OUTPUT" | awk '/cycles/ {gsub(/,/, ""); print $1}' | head -1)
                    INSTRUCTIONS=$(echo "$PERF_OUTPUT" | awk '/instructions/ {gsub(/,/, ""); print $1}' | head -1)
                    CACHE_MISSES=$(echo "$PERF_OUTPUT" | awk '/cache-misses/ {gsub(/,/, ""); print $1}' | head -1)
                    
                    # DEBUG: Log parsed values
                    echo "[$TIMESTAMP] PID $PID parsed: cycles=$INTERVAL_CYCLES, instructions=$INSTRUCTIONS, cache_misses=$CACHE_MISSES" >> "${MONITOR_LOG_DIR}/perf_debug.log"
                    
                    # Accumulate cycles for total overhead calculation
                    if [[ "$INTERVAL_CYCLES" != "" && "$INTERVAL_CYCLES" != "0" ]]; then
                        INTERVAL_CYCLES_SUM=$(echo "$INTERVAL_CYCLES_SUM + $INTERVAL_CYCLES" | bc -l 2>/dev/null || echo "$INTERVAL_CYCLES")
                        INTERVAL_INSTRUCTIONS_SUM=$(echo "$INTERVAL_INSTRUCTIONS_SUM + $INSTRUCTIONS" | bc -l 2>/dev/null || echo "$INSTRUCTIONS")
                    fi
                    
                    # Calculate IPC for this interval
                    if [[ "$INTERVAL_CYCLES" != "" && "$INSTRUCTIONS" != "" && "$INTERVAL_CYCLES" != "0" ]]; then
                        IPC=$(echo "scale=4; $INSTRUCTIONS / $INTERVAL_CYCLES" | bc -l 2>/dev/null || echo "0")
                    else
                        IPC="0"
                    fi
                    
                    INTERVAL_CYCLES=${INTERVAL_CYCLES:-"0"}
                    INSTRUCTIONS=${INSTRUCTIONS:-"0"}
                    CACHE_MISSES=${CACHE_MISSES:-"0"}
                else
                    INTERVAL_CYCLES="N/A"
                    INSTRUCTIONS="N/A"
                    IPC="N/A"
                    CACHE_MISSES="N/A"
                fi

                # Get process name (only system call needed)
                PROCESS_NAME=$(jps | grep "$PID" | awk '{print $2}')

                # Update running totals
                TOTAL_CYCLES_ACCUMULATED=$(echo "$TOTAL_CYCLES_ACCUMULATED + $INTERVAL_CYCLES_SUM" | bc -l 2>/dev/null || echo "$TOTAL_CYCLES_ACCUMULATED")
                TOTAL_INSTRUCTIONS_ACCUMULATED=$(echo "$TOTAL_INSTRUCTIONS_ACCUMULATED + $INTERVAL_INSTRUCTIONS_SUM" | bc -l 2>/dev/null || echo "$TOTAL_INSTRUCTIONS_ACCUMULATED")
                TOTAL_GC_TIME=$INTERVAL_GC_SUM

                # Log the streamlined data (CPU cycles + GC time focus)
                echo "$TIMESTAMP, $PID, $PROCESS_NAME, $HEAP_USED, $GC_TIME, $INTERVAL_CYCLES, $TOTAL_CYCLES_ACCUMULATED, $INSTRUCTIONS, $IPC, $CACHE_MISSES"
            done
            
            # Write total cycles summary for overhead calculation
            echo "$TIMESTAMP,$TOTAL_CYCLES_ACCUMULATED,$INTERVAL_CYCLES_SUM,$TOTAL_INSTRUCTIONS_ACCUMULATED,$TOTAL_GC_TIME" >> $TOTAL_CYCLES_FILE
            
            sleep 0.5  # Minimal sleep - total cycle now ~2.5s (2s perf + 0.5s other + 0.5s sleep)
        done
    } >> $MONITOR_LOG_FILE &
    MONITOR_PID=$!
    echo "INFO: Simple monitoring started with PID: $MONITOR_PID"
    echo "INFO: Total cycles accumulator: $TOTAL_CYCLES_FILE"
}

# Function to start standard monitoring (80% coverage fallback)
start_standard_monitoring() {
    echo "INFO: Starting standard monitoring..."
    {
        # Start monitoring
        echo "Timestamp, PID, Process Name, CPU%, TOTAL_CPU_TIME, %MEM, RSS (KB), VSZ (KB), Heap Used (MB), GC Time (ms), CPU Cycles/sec, Instructions, IPC, Cache Misses, Cache Miss Rate"
        while true; do
            TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
            PIDS=$(get_flink_pids)

            for PID in $PIDS; do
                # Get CPU usage using pidstat
                TOP_OUTPUT=$(top -b -n 1 -p $PID | tail -1)
                CPU_USAGE=$(echo $TOP_OUTPUT | awk '{print $9}')
                TOTAL_CPU_TIME=$(echo $TOP_OUTPUT | awk '{print $11}')

                # Get memory usage using ps
                MEM_STATS=$(ps -p $PID -o %mem,rss,vsz --no-headers)
                MEM_PERCENT=$(echo $MEM_STATS | awk '{print $1}')
                RSS=$(echo $MEM_STATS | awk '{print $2}')
                VSZ=$(echo $MEM_STATS | awk '{print $3}')

                # Get JVM memory usage using jstat
                if command -v jstat &> /dev/null; then
                    JVM_STATS=$(jstat -gc $PID 1 1 | tail -1 | awk '{print ($3+$4), $9+$10}')
                    HEAP_USED=$(echo $JVM_STATS | awk '{print $1}') # Heap Used in KB
                    GC_TIME=$(echo $JVM_STATS | awk '{print $2}')   # GC Time in ms
                else
                    HEAP_USED="N/A"
                    GC_TIME="N/A"
                fi

                # Get CPU cycles and performance counters using perf (higher accuracy)
                if command -v perf &> /dev/null; then
                    # Run perf with configurable sampling time for better accuracy
                    PERF_OUTPUT=$(timeout ${PERF_SAMPLE_TIME}s perf stat -p $PID -e cycles,instructions,cache-misses,cache-references 2>&1 | grep -E "cycles|instructions|cache-misses|cache-references")
                    
                    CPU_CYCLES=$(echo "$PERF_OUTPUT" | grep -w "cycles" | awk '{gsub(/,/, ""); print $1}' | head -1)
                    INSTRUCTIONS=$(echo "$PERF_OUTPUT" | grep -w "instructions" | awk '{gsub(/,/, ""); print $1}' | head -1)
                    CACHE_MISSES=$(echo "$PERF_OUTPUT" | grep "cache-misses" | awk '{gsub(/,/, ""); print $1}' | head -1)
                    CACHE_REFS=$(echo "$PERF_OUTPUT" | grep "cache-references" | awk '{gsub(/,/, ""); print $1}' | head -1)
                    
                    # Calculate per-second rates for better comparison
                    if [[ "$CPU_CYCLES" != "" && "$CPU_CYCLES" != "0" ]]; then
                        CYCLES_PER_SEC=$(echo "scale=0; $CPU_CYCLES / $PERF_SAMPLE_TIME" | bc -l 2>/dev/null || echo "0")
                    else
                        CYCLES_PER_SEC="0"
                    fi
                    
                    # Calculate Instructions Per Cycle (IPC) - important metric for efficiency
                    if [[ "$CPU_CYCLES" != "" && "$INSTRUCTIONS" != "" && "$CPU_CYCLES" != "0" ]]; then
                        IPC=$(echo "scale=4; $INSTRUCTIONS / $CPU_CYCLES" | bc -l 2>/dev/null || echo "0")
                    else
                        IPC="0"
                    fi
                    
                    # Calculate cache miss rate
                    if [[ "$CACHE_REFS" != "" && "$CACHE_MISSES" != "" && "$CACHE_REFS" != "0" ]]; then
                        CACHE_MISS_RATE=$(echo "scale=4; $CACHE_MISSES / $CACHE_REFS" | bc -l 2>/dev/null || echo "0")
                    else
                        CACHE_MISS_RATE="0"
                    fi
                    
                    # Set defaults if perf fails
                    CPU_CYCLES=${CYCLES_PER_SEC:-"0"}
                    INSTRUCTIONS=${INSTRUCTIONS:-"0"}
                    CACHE_MISSES=${CACHE_MISSES:-"0"}
                else
                    CPU_CYCLES="N/A"
                    INSTRUCTIONS="N/A"
                    IPC="N/A"
                    CACHE_MISSES="N/A"
                fi

                # Get process name
                PROCESS_NAME=$(jps | grep "$PID" | awk '{print $2}')

                # Log the data
                echo "$TIMESTAMP, $PID, $PROCESS_NAME, $CPU_USAGE, $TOTAL_CPU_TIME, $MEM_PERCENT, $RSS, $VSZ, $HEAP_USED, $GC_TIME, $CPU_CYCLES, $INSTRUCTIONS, $IPC, $CACHE_MISSES, $CACHE_MISS_RATE"
            done
            sleep $MONITOR_INTERVAL  # Configurable monitoring frequency for higher accuracy
        done
    } >> $MONITOR_LOG_FILE &
    MONITOR_PID=$!
}

# Function to stop monitoring
stop_monitoring() {
    echo "INFO: Stopping simple monitoring..."
    
    # Stop simple monitoring if running
    if [[ ! -z "$MONITOR_PID" ]]; then
        kill $MONITOR_PID 2>/dev/null
        wait $MONITOR_PID 2>/dev/null
        echo "INFO: Simple monitoring stopped. Data saved to $MONITOR_LOG_FILE."
    fi
}

# dump data
function analyze() {
    mkdir -p ${EXP_DIR}/raw/
    mkdir -p ${EXP_DIR}/results/

    echo "INFO: dump to ${EXP_DIR}/raw/${EXP_NAME}"
    if [[ -d ${EXP_DIR}/raw/${EXP_NAME} ]]; then
        rm -rf ${EXP_DIR}/raw/${EXP_NAME}
    fi
    mv ${FLINK_DIR}/log/* ${EXP_DIR}/streamsluice/
    
    # Simple monitoring data is already in MONITOR_LOG_FILE, no additional collection needed
    echo "INFO: Monitoring data saved to $MONITOR_LOG_FILE"
    
    mv ${EXP_DIR}/streamsluice/ ${EXP_DIR}/raw/${EXP_NAME}
    mkdir ${EXP_DIR}/streamsluice/
}

run_one_exp() {
  EXP_NAME=part8-lr-${controller_type}-${autotuner_initial_value_option}-${autotuner_increase_bar_option}-${autotune_interval}-${runtime}-${warmup_time}-${warmup_rate}-${skip_interval}-${P2}-${DELAY2}-${P3}-${DELAY3}-${P4}-${DELAY4}-${P5}-${DELAY5}-${L}-${autotuner_increase_bar_alpha}-${epoch}-${input_rate_factor}-${PAYLOAD}-${SKEWNESS}-${is_treat}-${migration_interval}-${conservative_factor}-${repeat}

  echo "INFO: run exp ${EXP_NAME}"
  configFlink
  runFlink

  python -c 'import time; time.sleep(5)'

  # Start application and monitoring
  runApp
  start_simple_monitoring

  SCRIPTS_RUNTIME=$((runtime + 10))
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'

  # Stop monitoring and analyze logs
  stop_monitoring
  analyze
  stopFlink

  python -c 'import time; time.sleep(5)'
}

# initialization of the parameters
init() {
  # exp scenario
  controller_type=StreamSluice
  whether_type="streamsluice"
  how_type="streamsluice"
  scalein_type="streamsluice"
  is_scalein=true
  L=2000
  runtime=380 #1380 #1980 #780 #2190
  skip_interval=10 #120 #300 # skip seconds
  warmup=10000
  warmup_time=150 #300
  warmup_rate=1300
  repeat=1
  spike_estimation="linear_regression"
  spike_slope=0.75
  spike_intercept=1000
  errorcase_number=3
  #calibrate_selectivity=false
  calibrate_selectivity=true
  vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47" #,36fcfcb61a35d065e60ee34fccb0541a,c395b989724fa728d0a2640c6ccdb8a1,8e0d1d377d577c52511ad507bf0ce330,feccfb8648621345be01b71938abfb72" # ,
  is_treat=true
  migration_interval=500
  epoch=100
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.linearroad.LinearRoad"
  # set in Flink app
  stock_path="/home/samza/LR_data/"
  stock_file_name="3hr-our-rate.txt" #"3hr-50ms.txt"
  MP1=1
  MP2=128
  MP3=128
  MP4=128
  MP5=128

#  LP2=1
#  LP3=1
#  LP4=1
#  LP5=36
  LP2=1
  LP3=5 #36
  LP4=1 #7
  LP5=32

#  P1=1
#  P2=1
#  P3=1
#  P4=1
#  P5=30
  P1=1
  P2=1
  P3=3 #27
  P4=1 #4
  P5=27


  DELAY2=50
  DELAY3=1000 #8000 # 50
  DELAY4=50 #2000 # 50
  DELAY5=3333 #2500 #3333 #4000
#  DELAY6=10
#  DELAY7=500
#  DELAY8=10
#  DELAY9=100
  input_rate_factor=1
  PAYLOAD=25 #100 #0 # about (100 + 2 * PAYLOAD) MB in every operator (1000000 keys, every key contains about 100 bytes)
  SKEWNESS=0.0 # ZIPF factor
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} \
    -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} \
    -p6 ${P6} -mp6 ${MP6} -op6Delay ${DELAY6} \
    -p7 ${P7} -mp7 ${MP7} -op7Delay ${DELAY7} \
    -p8 ${P8} -mp8 ${MP8} -op8Delay ${DELAY8} \
    -p9 ${P9} -mp9 ${MP9} -op9Delay ${DELAY9} \
    -input_rate_factor ${input_rate_factor} \
    -payload ${PAYLOAD} -skew_factor ${SKEWNESS} \
    -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
        -p1 ${P1} -mp1 ${MP1} \
        -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} \
        -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} \
        -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} \
        -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} \
        -p6 ${P6} -mp6 ${MP6} -op6Delay ${DELAY6} \
        -p7 ${P7} -mp7 ${MP7} -op7Delay ${DELAY7} \
        -p8 ${P8} -mp8 ${MP8} -op8Delay ${DELAY8} \
        -p9 ${P9} -mp9 ${MP9} -op9Delay ${DELAY9} \
        -input_rate_factor ${input_rate_factor} \
        -payload ${PAYLOAD} -skew_factor ${SKEWNESS} \
        -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} &
}

run_stock_test(){
    echo "Run linear road experiments..."
    init
    printf "Part_8\n" > part8_result.txt
    how_more_optimization_flag=false
    how_optimization_flag=false
    how_intrinsic_bound_flag=true
    how_conservative_flag=false # true
    coordination_latency_flag=true
    conservative_service_rate_flag=true # false
    conservative_factor=0.8
    smooth_backlog_flag=false
    new_metrics_retriever_flag=true

    autotune=true
    autotune_interval=60
    autotuner="UserLimitTuner"
    autotuner_latency_window=100
    autotuner_bar_lowerbound=350
    autotuner_adjustment_option=1
    autotuner_increase_bar_option=1 # 2
    autotuner_initial_value_alpha=1.2
    autotuner_adjustment_beta=2.0
    epoch=100
    decision_interval=1 #10
    snapshot_size=20
    L=1000 #2000 #2500
    migration_interval=1000 #500
    spike_slope=0.7
    autotuner_initial_value_option=5
    autotuner_increase_bar_option=8 # 3 5
    autotuner_increase_bar_alpha=0.1 #0.25
    scaling_decision_option=1
    repeat=1

    is_treat=false
    autotune=true
    metrics_report=true
    repeat=2
    L=3000
    controller_type="StreamSluice"
    whether_type="streamsluice"
    how_type="streamsluice"
    scalein_type="streamsluice"
    run_one_exp
    printf "${EXP_NAME}\n" >> part8_result.txt

    controller_type="NoControll"
    is_treat=false
    autotune=false
    metrics_report=false
    repeat=2
    L=3000
    whether_type="streamsluice"
    how_type="streamsluice"
    scalein_type="streamsluice"
    run_one_exp
    printf "${EXP_NAME}\n" >> part8_result.txt
}
run_stock_test