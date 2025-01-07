#!/bin/bash

source config-server-local.sh

# Define the process names to monitor
PROCESS_NAMES=("StandaloneSessionClusterEntrypoint" "TaskManagerRunner")
MONITOR_LOG_DIR="${FLINK_DIR}/log/"
MONITOR_LOG_FILE="${MONITOR_LOG_DIR}/monitor_$(date +%Y%m%d_%H%M%S).out"

# Create monitor log directory
mkdir -p $MONITOR_LOG_DIR

# Function to get PIDs of Flink processes
get_flink_pids() {
    local pids=()
    for process_name in "${PROCESS_NAMES[@]}"; do
        pids+=($(pgrep -f "$process_name"))
    done
    echo "${pids[@]}"
}

# Function to start monitoring
start_monitoring() {
    echo "INFO: Starting monitoring..."
    {
        # Start monitoring
        echo "Timestamp, PID, Process Name, CPU%, %MEM, RSS (KB), VSZ (KB), Heap Used (MB), GC Time (ms)"
        while true; do
            TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
            PIDS=$(get_flink_pids)

            for PID in $PIDS; do
                # Get CPU usage using pidstat
                CPU_USAGE=$(pidstat -u -p $PID 1 1 | awk '/^[0-9]/ {print $7}' | tail -1)

                # Get memory usage using ps
                MEM_STATS=$(ps -p $PID -o %mem,rss,vsz --no-headers)
                MEM_PERCENT=$(echo $MEM_STATS | awk '{print $1}')
                RSS=$(echo $MEM_STATS | awk '{print $2}')
                VSZ=$(echo $MEM_STATS | awk '{print $3}')

                # Get JVM memory usage using jstat
                if command -v jstat &> /dev/null; then
                    JVM_STATS=$(jstat -gc $PID 1 1 | tail -1 | awk '{print ($3+$4)/1024, $9+$10}')
                    HEAP_USED=$(echo $JVM_STATS | awk '{print $1}') # Heap Used in MB
                    GC_TIME=$(echo $JVM_STATS | awk '{print $2}')   # GC Time in ms
                else
                    HEAP_USED="N/A"
                    GC_TIME="N/A"
                fi

                # Get process name
                PROCESS_NAME=$(ps -p $PID -o comm=)

                # Log the data
                echo "$TIMESTAMP, $PID, $PROCESS_NAME, $CPU_USAGE, $MEM_PERCENT, $RSS, $VSZ, $HEAP_USED, $GC_TIME"
            done
            sleep 5  # Adjust monitoring frequency as needed
        done
    } >> $MONITOR_LOG_FILE &
    MONITOR_PID=$!
}

# Function to stop monitoring
stop_monitoring() {
    echo "INFO: Stopping monitoring..."
    kill $MONITOR_PID
    wait $MONITOR_PID 2>/dev/null
    echo "Monitoring stopped. Logs saved to $MONITOR_LOG_FILE."
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
    mv ${EXP_DIR}/streamsluice/ ${EXP_DIR}/raw/${EXP_NAME}
    mkdir ${EXP_DIR}/streamsluice/
}

run_one_exp() {
    EXP_NAME=part8-stock-${controller_type}-${autotuner_initial_value_option}-${autotuner_increase_bar_option}-${autotune_interval}-${runtime}-${warmup_time}-${warmup_rate}-${skip_interval}-${P2}-${DELAY2}-${P3}-${DELAY3}-${P4}-${DELAY4}-${P5}-${DELAY5}-${P6}-${P7}-${DELAY7}-${L}-${epoch}-${autotuner_increase_bar_alpha}-${is_treat}-${autotune}-${repeat}

    echo "INFO: run exp ${EXP_NAME}"
    configFlink
    runFlink

    python -c 'import time; time.sleep(5)'

    # Start application and monitoring
    runApp
    start_monitoring

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
  L=2000
  runtime=1350 #1950 #750 #2190 #3990 #
  skip_interval=20 # skip seconds
  warmup=10000
  warmup_time=90 #30
  warmup_rate=1000
  repeat=1
  spike_estimation="linear_regression"
  spike_slope=0.75
  spike_intercept=1000
  errorcase_number=3
  #calibrate_selectivity=false
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
  P3=11
  P4=1
  P5=2
  P6=1
  P7=15

  # Original setting
  DELAY2=200
  DELAY3=3333 #2500
  DELAY4=200
  DELAY5=500
  DELAY7=5000 #3333

  PAYLOAD=5000
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
    echo "Run overall test..."
    init
    printf "Part_8\n" > part8_result.txt

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
    autotuner_bar_lowerbound=550 #300
    autotuner_initial_value_option=4 # 1
    autotuner_adjustment_option=1
    autotuner_increase_bar_option=1 # 2
    autotuner_initial_value_alpha=1.2
    autotuner_adjustment_beta=2.0
    epoch=100
    decision_interval=1 #10
    snapshot_size=40 #20
    L=1000 #2000 #2500
    migration_interval=1000 #500
    spike_slope=0.7
    autotuner_initial_value_option=5
    autotuner_increase_bar_option=8
    autotuner_increase_bar_alpha=0.1 #0.25

    is_treat=true
    autotune=true
    repeat=1
    scaling_decision_option=1
    autotuner_increase_bar_alpha=0.1
    L=3000
    controller_type="StreamSluice"
    whether_type="streamsluice"
    how_type="streamsluice"
    scalein_type="streamsluice"
    run_one_exp
    printf "${EXP_NAME}\n" >> part8_result.txt

    is_treat=false
    autotune=false
    L=3000
    controller_type="NoControll"
    whether_type="streamsluice"
    how_type="streamsluice"
    scalein_type="streamsluice"
    run_one_exp
    printf "${EXP_NAME}\n" >> part8_result.txt
}
run_stock_test