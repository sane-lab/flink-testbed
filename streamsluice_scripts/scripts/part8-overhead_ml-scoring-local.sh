#!/bin/bash

# Enhanced monitoring with improved accuracy for CPU cycle measurements
# - 80% sampling coverage (0.8s perf sampling every 1s) vs previous 20% coverage
# - Per-second cycle rates for better comparison across experiments  
# - Cache miss rate for memory efficiency analysis
# - Configurable timing parameters for fine-tuning accuracy vs overhead

source $(dirname "$0")/config-mlscore-local.sh

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
    
    # Initialize accumulator files for TaskManagerRunner (primary) and total (secondary)
    TASKMANAGER_CYCLES_FILE="${MONITOR_LOG_DIR}/taskmanager_cycles_${EXP_NAME}.txt"
    TOTAL_CYCLES_FILE="${MONITOR_LOG_DIR}/total_cycles_${EXP_NAME}.txt"
    
    echo "# TaskManagerRunner CPU cycles accumulator (PRIMARY for overhead calculation)" > $TASKMANAGER_CYCLES_FILE
    echo "# Format: timestamp,tm_total_cycles,tm_interval_cycles,tm_instructions,tm_gc_time" >> $TASKMANAGER_CYCLES_FILE
    
    echo "# Total CPU cycles accumulator (ALL processes - secondary reference)" > $TOTAL_CYCLES_FILE
    echo "# Format: timestamp,all_total_cycles,all_interval_cycles,all_instructions,all_gc_time" >> $TOTAL_CYCLES_FILE
    
    {
        # Header for monitoring log (streamlined for CPU cycles and GC time only)
        echo "Timestamp, PID, Process Name, Heap Used (MB), GC Time (ms), Interval Cycles, Total Cycles, Instructions, IPC, Cache Misses"
        
        # Initialize running totals (separate TaskManagerRunner from total)
        TOTAL_CYCLES_ACCUMULATED=0
        TOTAL_INSTRUCTIONS_ACCUMULATED=0
        TOTAL_GC_TIME=0
        
        # TaskManagerRunner-specific accumulators (PRIMARY for overhead calculation)
        TM_CYCLES_ACCUMULATED=0
        TM_INSTRUCTIONS_ACCUMULATED=0
        TM_GC_TIME=0
        
        while true; do
            TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
            PIDS=$(get_flink_pids)
            
            # All processes interval sums
            INTERVAL_CYCLES_SUM=0
            INTERVAL_INSTRUCTIONS_SUM=0
            INTERVAL_GC_SUM=0
            
            # TaskManagerRunner-only interval sums
            TM_INTERVAL_CYCLES_SUM=0
            TM_INTERVAL_INSTRUCTIONS_SUM=0
            TM_INTERVAL_GC_SUM=0

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
                    
                    # Extract metrics with multiple parsing approaches (robust fallback)
                    INTERVAL_CYCLES=$(echo "$PERF_OUTPUT" | awk '/cycles/ {gsub(/,/, ""); print $1}' | head -1)
                    INSTRUCTIONS=$(echo "$PERF_OUTPUT" | awk '/instructions/ {gsub(/,/, ""); print $1}' | head -1)
                    CACHE_MISSES=$(echo "$PERF_OUTPUT" | awk '/cache-misses/ {gsub(/,/, ""); print $1}' | head -1)
                    
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
                    CACHE_MISS_RATE="N/A"
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
        echo "INFO: Collecting continuous monitoring data (optimized)..."
        mkdir -p ${EXP_DIR}/streamsluice/continuous_monitoring/
        
        # Fast move operation instead of copy (much faster for large files)
        if [[ -d "${CONTINUOUS_MONITOR_DIR}/perf_logs" ]]; then
            echo "INFO: Moving perf data files..."
            mv "${CONTINUOUS_MONITOR_DIR}/perf_logs" "${EXP_DIR}/streamsluice/continuous_monitoring/" 2>/dev/null || true
        fi
        
        # Move other monitoring files quickly
        mv ${CONTINUOUS_MONITOR_DIR}/*.log ${EXP_DIR}/streamsluice/continuous_monitoring/ 2>/dev/null || true
        mv ${CONTINUOUS_MONITOR_DIR}/*.csv ${EXP_DIR}/streamsluice/continuous_monitoring/ 2>/dev/null || true
        mv ${CONTINUOUS_MONITOR_DIR}/*.data ${EXP_DIR}/streamsluice/continuous_monitoring/ 2>/dev/null || true
        mv ${CONTINUOUS_MONITOR_DIR}/*.txt ${EXP_DIR}/streamsluice/continuous_monitoring/ 2>/dev/null || true
        
        # Clean up temporary monitoring directory
        rm -rf ${CONTINUOUS_MONITOR_DIR} 2>/dev/null || true
        
        echo "INFO: Continuous monitoring data collected."
    fi
    
    mv ${EXP_DIR}/streamsluice/ ${EXP_DIR}/raw/${EXP_NAME}
    mkdir ${EXP_DIR}/streamsluice/
}

run_one_exp() {
  EXP_NAME=part8-mlscore-${controller_type}-${autotuner_initial_value_option}-${autotuner_increase_bar_option}-${autotune_interval}-${runtime}-${warmup_time}-${warmup_rate}-${P2}-${parse_delay}-${P3}-${feature_delay}-${P4}-${scorer_base_delay}-${P5}-${scorer_complexity_factor}-${L}-${autotuner_increase_bar_alpha}-${epoch}-${input_rate_factor}-${is_treat}-${migration_interval}-${conservative_factor}-${repeat}

  echo "INFO: run exp ${EXP_NAME}"
  configFlink
  runFlink

  python -c 'import time; time.sleep(5)'

  # Start application and continuous monitoring
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
  controller_type="StreamSluice"
  whether_type="streamsluice"
  how_type="streamsluice"
  scalein_type="streamsluice"
  is_scalein=true
  L=2000
  runtime=1380    # experiment runtime in seconds
  warmup_time=120 # warmup duration in seconds
  warmup_rate=800 # warmup rate txn/s
  repeat=1
  spike_estimation="linear_regression"
  spike_slope=0.7
  spike_intercept=1000
  errorcase_number=3
  calibrate_selectivity=true
  vertex_id="a84740bacf923e828852cc4966f2247c,eabd4c11f6c6fbdf011f0f1fc42097b1,d01047f852abd5702a0dabeedac99ff5,d2336f79a0d60b5a4b16c8769ec82e47"
  is_treat=true
  migration_interval=1000
  epoch=100
  decision_interval=1
  snapshot_size=20
  
  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.MLmodelscoring.MLScoringJob"
  
  # ML scoring job specific parameters
  sine_baseline=1000.0      # baseline rate for sine curve f(t) = amplitude * sin(t) + baseline
  sine_amplitude=300.0      # amplitude of sine wave (absolute value, not percentage)
  sine_period=60.0          # period in seconds
  spike_probability=0.05
  spike_multiplier=3.0
  fluctuation_std=0.1
  parse_delay=1             # microseconds
  feature_delay=1           # microseconds
  input_rate_factor=1.0
  
  # Feature-based scorer configuration parameters
  scorer_base_delay=2000      # Base processing delay in microseconds
  scorer_complexity_factor=1.0 # Complexity multiplier for feature-based processing
  
  # parallelism settings
  P1=1
  MP1=1
  P2=1
  MP2=128
  P3=1
  MP3=128
  P4=2
  MP4=128
  P5=1
  MP5=128
  
  # ML-specific max parallelism limits for vertex-based scaling
  LP_PARSE=4     # max parallelism for parse_txn operator
  LP_FEATURE=4   # max parallelism for feature_builder operator
  LP_SCORER=30    # max parallelism for scorer operator
  LP_SINK=1       # max parallelism for sink operator
  
  # ML-specific configuration parameters passed to Flink config
  ml_sine_baseline=${sine_baseline}
  ml_sine_amplitude=${sine_amplitude}
  ml_sine_period=${sine_period}
  ml_spike_probability=${spike_probability}
  ml_spike_multiplier=${spike_multiplier}
  ml_fluctuation_std=${fluctuation_std}
  ml_parse_delay=${parse_delay}
  ml_feature_delay=${feature_delay}
  
  # system settings
  metrics_output=true
  autotune=true
  autotune_interval=60
  autotuner="UserLimitTuner"
  autotuner_latency_window=100
  autotuner_bar_lowerbound=350
  autotuner_adjustment_option=1
  autotuner_increase_bar_option=8
  autotuner_initial_value_alpha=1.2
  autotuner_adjustment_beta=2.0
  autotuner_initial_value_option=5
  autotuner_increase_bar_alpha=0.1
  
  # flags
  how_more_optimization_flag=false
  how_optimization_flag=false
  how_intrinsic_bound_flag=true
  how_conservative_flag=false
  coordination_latency_flag=true
  conservative_service_rate_flag=true
  conservative_factor=0.8
  transmission_delay=100
  smooth_backlog_flag=false
  new_metrics_retriever_flag=true
  scaling_decision_option=1
  
  # migration overhead (ms)
  migration_overhead=100
  
  # Additional variables needed by configFlink
  warmup=10000
  metrics_report=true
}

# run applications
function runApp() {
    echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -run.seconds ${runtime} \
    -sine.baseline ${sine_baseline} \
    -sine.amplitude ${sine_amplitude} \
    -sine.period ${sine_period} \
    -spike.probability ${spike_probability} \
    -spike.multiplier ${spike_multiplier} \
    -fluctuation.std ${fluctuation_std} \
    -parse.delay ${parse_delay} \
    -feature.delay ${feature_delay} \
    -scorer.base.delay ${scorer_base_delay} \
    -scorer.complexity.factor ${scorer_complexity_factor} \
    -warmup_time ${warmup_time} \
    -warmup_rate ${warmup_rate} \
    -input_rate_factor ${input_rate_factor} \
    -p1 ${P1} \
    -p2 ${P2} -mp2 ${MP2} \
    -p3 ${P3} -mp3 ${MP3} \
    -p4 ${P4} -mp4 ${MP4} \
    -p5 ${P5} -mp5 ${MP5} &"
    
    ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -run.seconds ${runtime} \
    -sine.baseline ${sine_baseline} \
    -sine.amplitude ${sine_amplitude} \
    -sine.period ${sine_period} \
    -spike.probability ${spike_probability} \
    -spike.multiplier ${spike_multiplier} \
    -fluctuation.std ${fluctuation_std} \
    -parse.delay ${parse_delay} \
    -feature.delay ${feature_delay} \
    -scorer.base.delay ${scorer_base_delay} \
    -scorer.complexity.factor ${scorer_complexity_factor} \
    -warmup_time ${warmup_time} \
    -warmup_rate ${warmup_rate} \
    -input_rate_factor ${input_rate_factor} \
    -p1 ${P1} \
    -p2 ${P2} -mp2 ${MP2} \
    -p3 ${P3} -mp3 ${MP3} \
    -p4 ${P4} -mp4 ${MP4} \
    -p5 ${P5} -mp5 ${MP5} &
}

function setting1(){
  # Setting 1: Light processing baseline
  printf "ML Scoring Setting 1 - Light Processing\n" >> part8_result.txt
  runtime=600
  setting="light"
  sine_baseline=600.0         # baseline rate for sine curve
  sine_amplitude=100.0        # amplitude of sine wave (range: 500-700 txn/s)
  sine_period=60.0            # period in seconds
  warmup_rate=400             # warmup rate (should be <= sine_baseline - sine_amplitude)
  spike_probability=0.02
  spike_multiplier=2.0
  fluctuation_std=0.05
  parse_delay=100 # microseconds
  feature_delay=100  # microseconds
  
  # Light processing settings
  scorer_base_delay=1000      # 1ms in microseconds
  scorer_complexity_factor=0.5  # Reduced complexity
  
  P2=1
  P3=1
  P4=2
  P5=1

  # Update ML config parameters
  ml_sine_baseline=${sine_baseline}
  ml_sine_amplitude=${sine_amplitude}
  ml_sine_period=${sine_period}
  ml_spike_probability=${spike_probability}
  ml_spike_multiplier=${spike_multiplier}
  ml_fluctuation_std=${fluctuation_std}
  ml_parse_delay=${parse_delay}
  ml_feature_delay=${feature_delay}
  
  for repeat in 1; do
    run_one_exp
    printf "${EXP_NAME}\n" >> part8_result.txt
  done
}

function setting2(){
  # Setting 2: Medium processing with realistic complexity
  printf "ML Scoring Setting 2 - Medium Processing\n" >> part8_result.txt
  runtime=1380
  setting="medium"
  L=2000
  transmission_delay=50
  
  sine_baseline=1500.0        # baseline rate for sine curve
  sine_amplitude=350.0        # amplitude of sine wave (range: 1100-1900 txn/s)
  sine_period=240             # period in seconds
  warmup_rate=1200            # warmup rate (should be <= sine_baseline - sine_amplitude)
  spike_probability=0.05
  spike_multiplier=3.0
  fluctuation_std=0.1
  parse_delay=100
  feature_delay=100
  
  # Medium processing settings - realistic GBDT complexity
  scorer_base_delay=3000      # 3ms in microseconds
  scorer_complexity_factor=1.5  # Moderate complexity
  
  P2=1
  P3=1
  P4=15
  
  # Update ML config parameters
  ml_sine_baseline=${sine_baseline}
  ml_sine_amplitude=${sine_amplitude}
  ml_sine_period=${sine_period}
  ml_spike_probability=${spike_probability}
  ml_spike_multiplier=${spike_multiplier}
  ml_fluctuation_std=${fluctuation_std}
  ml_parse_delay=${parse_delay}
  ml_feature_delay=${feature_delay}

  printf "Comparison\n" >> part8_result.txt
  for repeat in 1; do
    L=2000
    autotune=false
    is_treat=false
    P2=1
    P3=1
    P4=15
    controller_type="StreamSluice"
    whether_type="streamsluice"
    how_type="streamsluice"
    scalein_type="streamsluice"
    migration_interval=2500
    run_one_exp
    printf "${EXP_NAME}\n" >> part8_result.txt

    controller_type="NoControll"
    is_treat=false
    autotune=false
    whether_type="streamsluice"
    how_type="streamsluice"
    scalein_type="streamsluice"
    migration_interval=2500
    run_one_exp
    printf "${EXP_NAME}\n" >> part8_result.txt
  done
}

run_ml_scoring_test(){
    echo "Run ML Scoring experiments..."
    init
    printf "Part_8_ML_Scoring\n" > part8_result.txt
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
    autotuner_bar_lowerbound=350
    autotuner_adjustment_option=1
    autotuner_increase_bar_option=8
    autotuner_initial_value_alpha=1.2
    autotuner_adjustment_beta=2.0
    epoch=100
    decision_interval=1
    snapshot_size=20
    L=2000
    migration_interval=1000
    spike_slope=0.7
    autotuner_initial_value_option=5
    autotuner_increase_bar_alpha=0.1
    scaling_decision_option=1
    repeat=1

    is_treat=false
    autotune=true
    metrics_report=true
    repeat=1
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
    repeat=1
    L=3000
    whether_type="streamsluice"
    how_type="streamsluice"
    scalein_type="streamsluice"
    run_one_exp
    printf "${EXP_NAME}\n" >> part8_result.txt
}

# Run the ML scoring test
run_ml_scoring_test 