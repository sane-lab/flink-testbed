#!/bin/bash

# Enhanced monitoring with improved accuracy for CPU cycle measurements
# - 80% sampling coverage (0.8s perf sampling every 1s) vs previous 20% coverage
# - Per-second cycle rates for better comparison across experiments  
# - Cache miss rate for memory efficiency analysis
# - Configurable timing parameters for fine-tuning accuracy vs overhead

source config-server-twitter-local.sh

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

# Function to start continuous monitoring (100% accurate)
start_continuous_monitoring() {
    echo "INFO: Starting continuous monitoring for experiment ${EXP_NAME}..."
    
    # Create monitoring directories
    CONTINUOUS_MONITOR_DIR="${EXP_DIR}/continuous_monitoring"
    mkdir -p $CONTINUOUS_MONITOR_DIR
    
    # Start continuous perf monitoring
    CONTINUOUS_SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"  # Get absolute path
    CONTINUOUS_SCRIPT_PATH="${CONTINUOUS_SCRIPT_DIR}/continuous_perf_monitor.sh"
    
    echo "DEBUG: Looking for continuous monitoring script at: ${CONTINUOUS_SCRIPT_PATH}"
    
    if [[ -f "${CONTINUOUS_SCRIPT_PATH}" ]]; then
        echo "INFO: Using continuous perf monitoring script..."
        echo "INFO: Waiting for Flink processes to be available..."
        
        # Wait for Flink processes to be available (max 30 seconds)
        for i in {1..30}; do
            FLINK_PIDS=$(jps | grep -E "(StandaloneSessionClusterEntrypoint|TaskManagerRunner)" | awk '{print $1}')
            if [[ ! -z "$FLINK_PIDS" ]]; then
                echo "INFO: Flink processes found: $FLINK_PIDS"
                break
            fi
            echo "INFO: Waiting for Flink processes... (attempt $i/30)"
            sleep 1
        done
        
        if [[ -z "$FLINK_PIDS" ]]; then
            echo "WARNING: No Flink processes found after 30 seconds, monitoring may not work properly"
        fi
        
        cd $CONTINUOUS_MONITOR_DIR
        nohup "${CONTINUOUS_SCRIPT_PATH}" ${EXP_NAME} 200 > continuous_monitor.log 2>&1 &
        CONTINUOUS_MONITOR_PID=$!
        echo $CONTINUOUS_MONITOR_PID > "${CONTINUOUS_MONITOR_DIR}/${EXP_NAME}_continuous.pid"
        cd - > /dev/null
        echo "INFO: Continuous monitoring started with PID: $CONTINUOUS_MONITOR_PID"
    else
        echo "WARNING: continuous_perf_monitor.sh not found at ${CONTINUOUS_SCRIPT_PATH}, falling back to standard monitoring"
        ls -la "${CONTINUOUS_SCRIPT_DIR}/" | grep continuous || echo "No continuous scripts found in directory"
        start_standard_monitoring
    fi
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
    echo "INFO: Stopping monitoring..."
    
    # Stop continuous monitoring if running
    if [[ -f "${CONTINUOUS_MONITOR_DIR}/${EXP_NAME}_continuous.pid" ]]; then
        CONTINUOUS_MONITOR_PID=$(cat "${CONTINUOUS_MONITOR_DIR}/${EXP_NAME}_continuous.pid")
        if kill -0 $CONTINUOUS_MONITOR_PID 2>/dev/null; then
            echo "INFO: Stopping continuous monitoring (PID: $CONTINUOUS_MONITOR_PID)..."
            kill -INT $CONTINUOUS_MONITOR_PID 2>/dev/null
            sleep 3
            if kill -0 $CONTINUOUS_MONITOR_PID 2>/dev/null; then
                kill -TERM $CONTINUOUS_MONITOR_PID 2>/dev/null
                sleep 2
                if kill -0 $CONTINUOUS_MONITOR_PID 2>/dev/null; then
                    kill -KILL $CONTINUOUS_MONITOR_PID 2>/dev/null
                fi
            fi
        fi
        rm -f "${CONTINUOUS_MONITOR_DIR}/${EXP_NAME}_continuous.pid"
        echo "INFO: Continuous monitoring stopped."
    fi
    
    # Stop standard monitoring if running
    if [[ ! -z "$MONITOR_PID" ]]; then
        kill $MONITOR_PID 2>/dev/null
        wait $MONITOR_PID 2>/dev/null
        echo "INFO: Standard monitoring stopped. Logs saved to $MONITOR_LOG_FILE."
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
    
    # Collect continuous monitoring data if available
    if [[ -d "${CONTINUOUS_MONITOR_DIR}" ]]; then
        echo "INFO: Collecting continuous monitoring data..."
        mkdir -p ${EXP_DIR}/streamsluice/continuous_monitoring/
        
        # Copy continuous monitoring files
        if [[ -d "${CONTINUOUS_MONITOR_DIR}/perf_logs" ]]; then
            cp -r ${CONTINUOUS_MONITOR_DIR}/perf_logs/* ${EXP_DIR}/streamsluice/continuous_monitoring/ 2>/dev/null || true
        fi
        
        # Copy log files
        cp ${CONTINUOUS_MONITOR_DIR}/*.log ${EXP_DIR}/streamsluice/continuous_monitoring/ 2>/dev/null || true
        cp ${CONTINUOUS_MONITOR_DIR}/*.csv ${EXP_DIR}/streamsluice/continuous_monitoring/ 2>/dev/null || true
        
        echo "INFO: Continuous monitoring data collected."
    fi
    
    mv ${EXP_DIR}/streamsluice/ ${EXP_DIR}/raw/${EXP_NAME}
    mkdir ${EXP_DIR}/streamsluice/
}

run_one_exp() {
  EXP_NAME=part8-tweet-${controller_type}-${autotuner_initial_value_option}-${autotune_interval}-${runtime}-${warmup_time}-${warmup_rate}-${skip_interval}-${P2}-${DELAY2}-${P3}-${DELAY3}-${P4}-${DELAY4}-${P5}-${DELAY5}-${PAYLOAD}-${L}-${epoch}-${is_treat}-${autotuner_increase_bar_alpha}-${repeat}

  echo "INFO: run exp ${EXP_NAME}"
  configFlink
  runFlink

  python -c 'import time; time.sleep(5)'

  # Start application and continuous monitoring
  runApp
  start_continuous_monitoring

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
  L=2000 #4000
  runtime=1350 #1950 #750
  skip_interval=1 # skip seconds
  warmup=10000
  warmup_time=90
  warmup_rate=1700 #3400 #1500
  repeat=1
  spike_estimation="linear_regression"
  spike_slope=0.75
  spike_intercept=1000 #2500
  errorcase_number=3
  #calibrate_selectivity=false
  calibrate_selectivity=true
  vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47" #feccfb8648621345be01b71938abfb72,36fcfcb61a35d065e60ee34fccb0541a" #,c395b989724fa728d0a2640c6ccdb8a1"
  is_treat=true
  migration_interval=500
  epoch=100
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.tweetalert.TweetAlertTrigger"
  # set in Flink app
  stock_path="/home/samza/Tweet_data/"
  stock_file_name="2hr-smooth.txt" #"3hr-50ms.txt"
  MP1=1
  MP2=128
  MP3=128
  MP4=128
  MP5=128
  MP6=128
  MP7=128

  LP2=27
  LP3=10
  LP4=1
  LP5=1

  P1=1
  P2=19
  P3=9
  P4=1
  P5=1

  DELAY2=3333 #3333 #3333 # 6666 #5000
  DELAY3=500 # 1000 #1000
  DELAY4=50
  DELAY5=50
  #DELAY6=100

  PAYLOAD=1250
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} \
    -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} \
    -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} \
    -payload ${PAYLOAD} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
        -p1 ${P1} -mp1 ${MP1} \
        -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} \
        -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} \
        -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} \
        -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} \
        -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} \
        -payload ${PAYLOAD} &
}

run_stock_test(){
    how_more_optimization_flag=false
    how_optimization_flag=false
    how_intrinsic_bound_flag=true
    how_conservative_flag=false # true
    coordination_latency_flag=true
    conservative_service_rate_flag=true # false
    smooth_backlog_flag=false
    new_metrics_retriever_flag=true

    autotune=true
    autotune_interval=60
    autotuner="UserLimitTuner"
    autotuner_latency_window=100
    autotuner_bar_lowerbound=350 #350
    # Old setting, no limitation on maximum bound value, binary incrase.
#    autotuner_initial_value_option=4
#    autotuner_increase_bar_option=7
    # New setting, limitation on maximum bound value, constant decrease (0.05 * limit)
    autotuner_initial_value_option=5
    autotuner_increase_bar_option=8

    autotuner_adjustment_option=1
    autotuner_initial_value_alpha=1.2
    autotuner_adjustment_beta=2.0

    echo "Run twitter alert experiments..."
    init
    printf "Part_8\n" > part8_result.txt

    epoch=100
    decision_interval=1 #10
    snapshot_size=20
    L=2000
    migration_interval=1000 #500
    spike_slope=0.7
    autotuner_increase_bar_alpha=0.1 #0.25

    is_treat=false
    autotune=true
    metrics_report=true
    controller_type="StreamSluice"
    whether_type="streamsluice"
    how_type="streamsluice"
    scalein_type="streamsluice"
    run_one_exp
    printf "${EXP_NAME}\n" >> part8_result.txt

    controller_type="NoControll"
    autotune=false
    metrics_report=false
    whether_type="streamsluice"
    how_type="streamsluice"
    scalein_type="streamsluice"
    run_one_exp
    printf "${EXP_NAME}\n" >> part8_result.txt
}
run_stock_test