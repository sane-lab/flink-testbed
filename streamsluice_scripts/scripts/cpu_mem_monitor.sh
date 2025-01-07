#!/bin/bash

# Define the process names to monitor
PROCESS_NAMES=("StandaloneSessionClusterEntrypoint" "TaskManagerRunner")

# Function to get PIDs of Flink processes
function get_flink_pids() {
    local pids=()
    for process_name in "${PROCESS_NAMES[@]}"; do
        pids+=($(pgrep -f "$process_name"))
    done
    echo "${pids[@]}"
}

function start_monitor_cpu_mem_log() {

    LOG_DIR="${EXP_DIR}/raw/${EXP_NAME}"
    LOG_FILE="${LOG_DIR}/flink_monitor_$(date +%Y%m%d_%H%M%S).out"

    # Create log directory if not exists
    mkdir -p $LOG_DIR

    # Header for the log file
    echo "Timestamp, PID, Process Name, CPU%, MEM%, Heap Used, GC Time" > $LOG_FILE

    # Start monitoring
    echo "Starting monitoring of Flink processes. Logs will be saved to $LOG_FILE"
    while true; do
        TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
        PIDS=$(get_flink_pids)

        "" > $LOG_FILE

        for PID in $PIDS; do
            # Get process name
            PROCESS_NAME=$(ps -p $PID -o comm=)

            # Get CPU and memory usage using pidstat
            CPU_MEM=$(pidstat -u -p $PID 1 1 | awk '/^[0-9]/ {print $7, $8}' | tail -1)

            # Get JVM heap and GC time using jstat
            if command -v jstat &> /dev/null; then
                JVM_STATS=$(jstat -gc $PID 1 1 | tail -1 | awk '{print $3+$4, $9+$10}')
                HEAP_USED=$(echo $JVM_STATS | awk '{print $1}')
                GC_TIME=$(echo $JVM_STATS | awk '{print $2}')
            else
                HEAP_USED="N/A"
                GC_TIME="N/A"
            fi

            # Append the data to the log file
            echo "$TIMESTAMP, $PID, $PROCESS_NAME, $CPU_MEM, $HEAP_USED, $GC_TIME" >> $LOG_FILE
        done

        # Sleep interval (adjust as needed)
        sleep 5
    done
}