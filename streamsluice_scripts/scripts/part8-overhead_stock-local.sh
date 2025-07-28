#!/bin/bash

# CPU cycle monitoring using perf for overhead analysis
# Generates monitor_*.out files with perf output for CPU cycle measurement

source config-server-local.sh

# Function to configure perf security (run once with sudo)
configure_perf_security() {
    echo "INFO: Configuring perf security settings..."
    
    # Check if we can run perf without sudo
    if perf stat -e cycles sleep 1 >/dev/null 2>&1; then
        echo "INFO: Perf already configured for non-root access"
        return 0
    fi
    
    # Try to configure perf security
    if command -v sudo >/dev/null 2>&1; then
        echo "INFO: Attempting to configure perf security (requires sudo once)..."
        if sudo sh -c 'echo -1 > /proc/sys/kernel/perf_event_paranoid'; then
            echo "INFO: Perf security configured successfully"
            return 0
        else
            echo "WARNING: Failed to configure perf security automatically"
            echo "WARNING: You may need to run: sudo sh -c 'echo -1 > /proc/sys/kernel/perf_event_paranoid'"
            return 1
        fi
    else
        echo "WARNING: sudo not available, perf may require root access"
        return 1
    fi
}

# Configure perf security at script start
configure_perf_security

# Define the process names to monitor
PROCESS_NAMES=("StandaloneSessionClusterEntrypoint" "TaskManagerRunner")
MONITOR_LOG_DIR="${FLINK_DIR}/log"
MONITOR_LOG_FILE="${MONITOR_LOG_DIR}/monitor_$(date +%Y%m%d_%H%M%S).out"

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

# Function to run perf with appropriate privileges
run_perf() {
    local pid=$1
    local duration=$2
    
    # Try without sudo first
    if perf stat -p $pid -e cycles,instructions,cache-misses sleep $duration 2>&1 > /tmp/perf_${pid}.tmp 2>/dev/null; then
        echo "perf_${pid}.tmp"
    else
        # Fall back to sudo if needed
        sudo perf stat -p $pid -e cycles,instructions,cache-misses sleep $duration 2>&1 > /tmp/perf_${pid}.tmp
        echo "perf_${pid}.tmp"
    fi
}

# CPU cycle monitoring function using perf
start_cpu_monitoring() {
    echo "INFO: Starting CPU cycle monitoring..."
    
    # Timeline alignment parameters
    WARMUP_DELAY=20        # Start monitoring after 20s warmup
    MONITOR_DURATION=240   # Monitor for 4 minutes (240s)
    # Start CPU cycle monitoring
    {
        # Header for monitoring log
        echo "Timestamp, PID, Process Name, Total Cycles, Total Instructions, Total Cache Misses, Duration (s)"
    
        # Wait for warmup period
        echo "INFO: Waiting ${WARMUP_DELAY}s for warmup before starting monitoring..."
        sleep $WARMUP_DELAY
        # Record start time for alignment
        MONITOR_START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
        echo "INFO: Monitoring started at: $MONITOR_START_TIME"
        echo "INFO: Will monitor for ${MONITOR_DURATION}s"
        
        # Get PIDs
        PIDS=$(get_flink_pids)
        
        # Run perf in parallel for all PIDs for the entire duration
        PERF_OUTPUTS=()
        PERF_PIDS=()
        for PID in $PIDS; do
            # Start perf in background for this PID for entire duration
            run_perf $PID $MONITOR_DURATION &
            PERF_PIDS+=($!)
        done
        
        # Wait for all perf commands to complete
        for i in "${!PERF_PIDS[@]}"; do
            wait ${PERF_PIDS[$i]}
            PERF_OUTPUTS[$i]=$(cat /tmp/perf_${PIDS[$i]}.tmp)
            rm -f /tmp/perf_${PIDS[$i]}.tmp
        done
        
        # Process results
        for i in "${!PIDS[@]}"; do
            PID=${PIDS[$i]}
            PERF_OUTPUT=${PERF_OUTPUTS[$i]}
            
            # Get process name
            PROCESS_NAME=$(jps | grep "$PID" | awk '{print $2}')
            
            # Extract cycles, instructions, and cache misses
            INTERVAL_CYCLES=$(echo "$PERF_OUTPUT" | awk '/cycles/ {gsub(/,/, ""); print $1}' | head -1)
            INTERVAL_CYCLES=${INTERVAL_CYCLES:-"0"}
            
            INTERVAL_INSTRUCTIONS=$(echo "$PERF_OUTPUT" | awk '/instructions/ {gsub(/,/, ""); print $1}' | head -1)
            INTERVAL_INSTRUCTIONS=${INTERVAL_INSTRUCTIONS:-"0"}
            
            INTERVAL_CACHE_MISSES=$(echo "$PERF_OUTPUT" | awk '/cache-misses/ {gsub(/,/, ""); print $1}' | head -1)
            INTERVAL_CACHE_MISSES=${INTERVAL_CACHE_MISSES:-"0"}
            
            # Log data
            if [[ "$PROCESS_NAME" == "TaskManagerRunner" ]]; then
                echo "$MONITOR_START_TIME, $PID, $PROCESS_NAME [PRIMARY], $INTERVAL_CYCLES, $INTERVAL_INSTRUCTIONS, $INTERVAL_CACHE_MISSES, $MONITOR_DURATION"
            else
                echo "$MONITOR_START_TIME, $PID, $PROCESS_NAME [secondary], $INTERVAL_CYCLES, $INTERVAL_INSTRUCTIONS, $INTERVAL_CACHE_MISSES, $MONITOR_DURATION"
            fi
        done
        
        # Record end time
        MONITOR_END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
        echo "INFO: Monitoring ended at: $MONITOR_END_TIME"
        echo "INFO: Total monitoring time: ${MONITOR_DURATION}s"
        
    } >> $MONITOR_LOG_FILE &
    MONITOR_PID=$!
    
    echo "INFO: CPU cycle monitoring started with PID: $MONITOR_PID"
    echo "INFO: Monitor log: $MONITOR_LOG_FILE"
    echo "INFO: Single perf run for ${MONITOR_DURATION}s (parallel for all processes)"
    echo "INFO: Timeline: ${WARMUP_DELAY}s warmup + ${MONITOR_DURATION}s monitoring"
}


# Function to stop monitoring
stop_monitoring() {
    echo "INFO: Stopping CPU cycle monitoring..."
    
    # Stop CPU cycle monitoring
    if [[ ! -z "$MONITOR_PID" ]]; then
        kill $MONITOR_PID 2>/dev/null
        wait $MONITOR_PID 2>/dev/null
    fi
    
    echo "INFO: Monitoring stopped. Data saved to: $MONITOR_LOG_FILE"
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
    
    echo "INFO: Monitoring data saved to $MONITOR_LOG_FILE"
    
    mv ${EXP_DIR}/streamsluice/ ${EXP_DIR}/raw/${EXP_NAME}
    mkdir ${EXP_DIR}/streamsluice/
}

run_one_exp() {
    EXP_NAME=part8-stock-${controller_type}-${metrics_report_interval}-${runtime}-${warmup_time}-${warmup_rate}-${skip_interval}-${P2}-${DELAY2}-${P3}-${DELAY3}-${P4}-${DELAY4}-${P5}-${DELAY5}-${P6}-${P7}-${DELAY7}-${L}-${epoch}-${autotuner_increase_bar_alpha}-${is_treat}-${autotune}-${repeat}

    echo "INFO: run exp ${EXP_NAME}"
    configFlink
    runFlink

    python -c 'import time; time.sleep(5)'

    # Start application and monitoring
    runApp
    start_cpu_monitoring

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
  scalein_type="streamsuice"
  is_scalein=true
  L=2000
  runtime=360
  skip_interval=20
  warmup=10000
  warmup_time=90
  warmup_rate=1000
  repeat=1
  spike_estimation="linear_regression"
  spike_slope=0.75
  spike_intercept=1000
  errorcase_number=3
  calibrate_selectivity=true
  vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47,36fcfcb61a35d065e60ee34fccb0541a,c395b989724fa728d0a2640c6ccdb8a1"
  is_treat=true
  migration_interval=500
  epoch=100
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.StreamSluiceTestSet.StockAnalysisApplication"
  # set in Flink app
  stock_path="/home/samza/SSE_data/"
  stock_file_name="sb-4hr-50ms.txt"
  MP1=1
  MP2=128
  MP3=128
  MP4=128
  MP5=128
  MP6=128
  MP7=128

  LP2=1
  LP3=14
  LP4=1
  LP5=2
  LP6=1
  LP7=20

  P1=1
  P2=1
  P3=4
  P4=1
  P5=1
  P6=1
  P7=5

  DELAY2=200
  DELAY3=1111
  DELAY4=200
  DELAY5=166
  DELAY7=1666

  PAYLOAD=5000
  metrics_report_interval=100000000
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -p1 ${P1} -mp1 ${MP1} \
    -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} \
    -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} \
    -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} \
    -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} \
    -p6 ${P6} -mp6 ${MP6} \
    -p7 ${P7} -mp7 ${MP7} -op7Delay ${DELAY7} -payload ${PAYLOAD}\
    -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} &"
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
        -p1 ${P1} -mp1 ${MP1} \
        -p2 ${P2} -mp2 ${MP2} -op2Delay ${DELAY2} \
        -p3 ${P3} -mp3 ${MP3} -op3Delay ${DELAY3} \
        -p4 ${P4} -mp4 ${MP4} -op4Delay ${DELAY4} \
        -p5 ${P5} -mp5 ${MP5} -op5Delay ${DELAY5} \
        -p6 ${P6} -mp6 ${MP6} \
        -p7 ${P7} -mp7 ${MP7} -op7Delay ${DELAY7} -payload ${PAYLOAD}\
        -file_name ${stock_path}${stock_file_name} -warmup_rate ${warmup_rate} -warmup_time ${warmup_time} -skip_interval ${skip_interval} &
}

run_stock_test(){
    echo "Run stock experiments..."
    init
    printf "Part_8\n" > part8_result.txt
    how_more_optimization_flag=false
    how_optimization_flag=false
    how_intrinsic_bound_flag=true
    how_conservative_flag=false
    coordination_latency_flag=true
    conservative_service_rate_flag=true
    conservative_factor=0.8
    smooth_backlog_flag=false
    new_metrics_retriever_flag=true

    autotune=true
    autotune_interval=60
    autotuner="UserLimitTuner"
    autotuner_latency_window=100
    autotuner_bar_lowerbound=550
    autotuner_adjustment_option=1
    autotuner_increase_bar_option=1
    autotuner_initial_value_alpha=1.2
    autotuner_adjustment_beta=2.0
    epoch=100
    decision_interval=1
    snapshot_size=40
    L=1000
    migration_interval=1000
    spike_slope=0.7
    autotuner_initial_value_option=5
    autotuner_increase_bar_option=8
    autotuner_increase_bar_alpha=0.1

    is_treat=false
    autotune=true
    metrics_report=true
    repeat=1
    scaling_decision_option=1
    autotuner_increase_bar_alpha=0.1
    L=3000
    controller_type="StreamSluice"
    whether_type="streamsluice"
    how_type="streamsluice"
    scalein_type="streamsluice"
    for metrics_report_interval in 5000000 10000000 25000000 50000000; do
      for repeat in 1; do
        run_one_exp
        printf "${EXP_NAME}\n" >> part8_result.txt
      done
    done

    controller_type="NoControll"
    metrics_report_interval=100000000
    is_treat=false
    autotune=false
    metrics_report=false
    L=3000
    whether_type="streamsluice"
    how_type="streamsluice"
    scalein_type="streamsluice"
    for repeat in 1; do
      run_one_exp
      printf "${EXP_NAME}\n" >> part8_result.txt
    done
}
run_stock_test