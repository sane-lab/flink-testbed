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

# Simple monitoring function for CPU cycles only
start_simple_monitoring() {
    echo "INFO: Starting CPU cycle monitoring..."
    
    # Timeline alignment parameters
    WARMUP_DELAY=20        # Start monitoring after 20s warmup
    MONITOR_DURATION=240 #1200  # Monitor for 20 minutes (1200s)
    
    # Initialize accumulator files for TaskManagerRunner (primary) and total (secondary)
    TASKMANAGER_CYCLES_FILE="${MONITOR_LOG_DIR}/taskmanager_cycles_${EXP_NAME}.txt"
    TOTAL_CYCLES_FILE="${MONITOR_LOG_DIR}/total_cycles_${EXP_NAME}.txt"
    
    echo "# TaskManagerRunner CPU cycles accumulator (PRIMARY for overhead calculation)" > $TASKMANAGER_CYCLES_FILE
    echo "# Format: timestamp,tm_total_cycles,tm_interval_cycles" >> $TASKMANAGER_CYCLES_FILE
    
    echo "# Total CPU cycles accumulator (ALL processes - secondary reference)" > $TOTAL_CYCLES_FILE
    echo "# Format: timestamp,all_total_cycles,all_interval_cycles" >> $TOTAL_CYCLES_FILE
    
    # Start CPU cycle monitoring
    {
        # Header for monitoring log
        echo "Timestamp, PID, Process Name, Total Cycles, Duration (s)"
        
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
            perf stat -p $PID -e cycles sleep $MONITOR_DURATION 2>&1 > /tmp/perf_${PID}.tmp &
            PERF_PIDS+=($!)
        done
        
        # Wait for all perf commands to complete
        for i in "${!PERF_PIDS[@]}"; do
            wait ${PERF_PIDS[$i]}
            PERF_OUTPUTS[$i]=$(cat /tmp/perf_${PIDS[$i]}.tmp)
            rm -f /tmp/perf_${PIDS[$i]}.tmp
        done
        
        # Process results
        TOTAL_CYCLES_SUM=0
        TM_CYCLES_SUM=0
        
        for i in "${!PIDS[@]}"; do
            PID=${PIDS[$i]}
            PERF_OUTPUT=${PERF_OUTPUTS[$i]}
            
            # Get process name
            PROCESS_NAME=$(jps | grep "$PID" | awk '{print $2}')
            
            # Extract cycles
            INTERVAL_CYCLES=$(echo "$PERF_OUTPUT" | awk '/cycles/ {gsub(/,/, ""); print $1}' | head -1)
            INTERVAL_CYCLES=${INTERVAL_CYCLES:-"0"}
            
            echo "DEBUG: PID $PID ($PROCESS_NAME) perf output:" >> "${MONITOR_LOG_DIR}/perf_debug.log"
            echo "$PERF_OUTPUT" >> "${MONITOR_LOG_DIR}/perf_debug.log"
            echo "DEBUG: Parsed cycles: $INTERVAL_CYCLES" >> "${MONITOR_LOG_DIR}/perf_debug.log"
            echo "---" >> "${MONITOR_LOG_DIR}/perf_debug.log"
            
            # Accumulate cycles
            if [[ "$INTERVAL_CYCLES" != "" && "$INTERVAL_CYCLES" != "0" ]]; then
                TOTAL_CYCLES_SUM=$(echo "$TOTAL_CYCLES_SUM + $INTERVAL_CYCLES" | bc -l 2>/dev/null || echo "$INTERVAL_CYCLES")
                if [[ "$PROCESS_NAME" == "TaskManagerRunner" ]]; then
                    TM_CYCLES_SUM=$INTERVAL_CYCLES
                fi
            fi
            
            # Log data
            if [[ "$PROCESS_NAME" == "TaskManagerRunner" ]]; then
                echo "$MONITOR_START_TIME, $PID, $PROCESS_NAME [PRIMARY], $INTERVAL_CYCLES, $MONITOR_DURATION"
            else
                echo "$MONITOR_START_TIME, $PID, $PROCESS_NAME [secondary], $INTERVAL_CYCLES, $MONITOR_DURATION"
            fi
        done
        
        # Write final results to files
        TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
        echo "$TIMESTAMP,$TM_CYCLES_SUM,$TM_CYCLES_SUM" >> $TASKMANAGER_CYCLES_FILE
        echo "$TIMESTAMP,$TOTAL_CYCLES_SUM,$TOTAL_CYCLES_SUM" >> $TOTAL_CYCLES_FILE
        
        # Record end time
        MONITOR_END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
        echo "INFO: Monitoring ended at: $MONITOR_END_TIME"
        echo "INFO: Total monitoring time: ${MONITOR_DURATION}s"
        echo "INFO: TaskManager cycles: $TM_CYCLES_SUM"
        echo "INFO: Total cycles: $TOTAL_CYCLES_SUM"
        
    } >> $MONITOR_LOG_FILE &
    MONITOR_PID=$!
    
    echo "INFO: CPU cycle monitoring started with PID: $MONITOR_PID"
    echo "INFO: TaskManagerRunner cycles (PRIMARY): $TASKMANAGER_CYCLES_FILE"
    echo "INFO: Total cycles accumulator (secondary): $TOTAL_CYCLES_FILE"
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
    
    echo "INFO: Monitoring stopped. Data saved to:"
    echo "- Main log: $MONITOR_LOG_FILE"
    echo "- TaskManager cycles: $TASKMANAGER_CYCLES_FILE"
    echo "- Total cycles: $TOTAL_CYCLES_FILE"
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
  EXP_NAME=part8-lr-${controller_type}-${metrics_report_interval}-${runtime}-${warmup_time}-${warmup_rate}-${skip_interval}-${P2}-${DELAY2}-${P3}-${DELAY3}-${P4}-${DELAY4}-${P5}-${DELAY5}-${L}-${autotuner_increase_bar_alpha}-${epoch}-${input_rate_factor}-${PAYLOAD}-${SKEWNESS}-${is_treat}-${migration_interval}-${conservative_factor}-${repeat}

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
  runtime=360 #1380 #1980 #780 #2190
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
  P3=1 #3 #27
  P4=1 #4
  P5=9 #27


  DELAY2=50
  DELAY3=333 #1000
  DELAY4=50 #2000 # 50
  DELAY5=1111 #3333
#  DELAY6=10
#  DELAY7=500
#  DELAY8=10
#  DELAY9=100
  input_rate_factor=1
  PAYLOAD=25 #100 #0 # about (100 + 2 * PAYLOAD) MB in every operator (1000000 keys, every key contains about 100 bytes)
  SKEWNESS=0.0 # ZIPF factor
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
    repeat=2
    L=3000
    whether_type="streamsluice"
    how_type="streamsluice"
    scalein_type="streamsluice"
    for repeat in 1 2 3; do
#      run_one_exp
#      printf "${EXP_NAME}\n" >> part8_result.txt
    done
}
run_stock_test