#!/bin/bash

# Continuous CPU cycle monitoring with high accuracy
# Usage: ./continuous_perf_monitor.sh [experiment_name] [sampling_frequency_ms]

EXPERIMENT_NAME=${1:-"perf_experiment_$(date +%Y%m%d_%H%M%S)"}
SAMPLING_FREQ=${2:-200}  # Default 200ms sampling (5Hz)
OUTPUT_DIR="perf_logs"
PERF_DATA_FILE="${OUTPUT_DIR}/${EXPERIMENT_NAME}_perf.data"
PERF_LOG_FILE="${OUTPUT_DIR}/${EXPERIMENT_NAME}_cycles.csv"

# Create output directory
mkdir -p $OUTPUT_DIR

echo "Starting continuous perf monitoring..."
echo "Experiment: $EXPERIMENT_NAME"
echo "Sampling frequency: ${SAMPLING_FREQ}ms"
echo "Output directory: $OUTPUT_DIR"

# Function to get Flink PIDs
get_flink_pids() {
    jps | grep -E "(StandaloneSessionClusterEntrypoint|TaskManagerRunner)" | awk '{print $1}' | tr '\n' ',' | sed 's/,$//'
}

# Function to start continuous perf recording
start_continuous_perf() {
    echo "Waiting for Flink processes to be available..."
    
    # Wait for Flink processes with retry (max 60 seconds)
    for attempt in {1..60}; do
        local pids=$(get_flink_pids)
        if [ ! -z "$pids" ]; then
            echo "Found Flink processes after ${attempt} seconds: $pids"
            break
        fi
        echo "Attempt $attempt/60: Waiting for Flink processes..."
        sleep 1
    done
    
    local pids=$(get_flink_pids)
    if [ -z "$pids" ]; then
        echo "ERROR: No Flink processes found after 60 seconds. Please check if Flink is running."
        exit 1
    fi
    
    echo "Monitoring PIDs: $pids"
    
    # Start perf record for continuous data collection with optimized settings
    echo "Starting perf record..."
    perf record -p $pids \
        -e cycles,instructions,cache-misses,cache-references,branch-misses,page-faults \
        -o $PERF_DATA_FILE \
        --mmap-pages=512 \
        --freq=1000 \
        sleep 3600 &  # Run for 1 hour max
    
    PERF_RECORD_PID=$!
    
    # Start periodic perf stat for real-time metrics
    echo "Starting real-time metrics collection..."
    {
        echo "timestamp,pid,process_name,cycles_per_sec,instructions_per_sec,ipc,cache_miss_rate,page_faults_per_sec"
        
        while kill -0 $PERF_RECORD_PID 2>/dev/null; do
            timestamp=$(date '+%Y-%m-%d %H:%M:%S.%3N')
            
            # Get current PIDs (in case of restarts)
            current_pids=$(get_flink_pids)
            
            for pid in $(echo $current_pids | tr ',' ' '); do
                if [ ! -z "$pid" ]; then
                    process_name=$(jps | grep "$pid" | awk '{print $2}')
                    
                    # Optimized perf stat with reduced overhead
                    perf_result=$(timeout 0.${SAMPLING_FREQ}s perf stat -p $pid \
                        -e cycles,instructions,cache-misses,cache-references,page-faults \
                        --interval-print 50 \
                        sleep 0.${SAMPLING_FREQ} 2>&1)
                    
                    # Parse results
                    cycles=$(echo "$perf_result" | grep -w "cycles" | awk '{gsub(/,/, ""); print $1}')
                    instructions=$(echo "$perf_result" | grep -w "instructions" | awk '{gsub(/,/, ""); print $1}')
                    cache_misses=$(echo "$perf_result" | grep "cache-misses" | awk '{gsub(/,/, ""); print $1}')
                    cache_refs=$(echo "$perf_result" | grep "cache-references" | awk '{gsub(/,/, ""); print $1}')
                    page_faults=$(echo "$perf_result" | grep "page-faults" | awk '{gsub(/,/, ""); print $1}')
                    
                    # Calculate rates per second
                    cycles_per_sec=$(echo "scale=0; $cycles / 0.${SAMPLING_FREQ}" | bc -l 2>/dev/null || echo "0")
                    instructions_per_sec=$(echo "scale=0; $instructions / 0.${SAMPLING_FREQ}" | bc -l 2>/dev/null || echo "0")
                    page_faults_per_sec=$(echo "scale=2; $page_faults / 0.${SAMPLING_FREQ}" | bc -l 2>/dev/null || echo "0")
                    
                    # Calculate IPC
                    if [[ "$cycles" != "" && "$instructions" != "" && "$cycles" -gt 0 ]]; then
                        ipc=$(echo "scale=4; $instructions / $cycles" | bc -l 2>/dev/null || echo "0")
                    else
                        ipc="0"
                    fi
                    
                    # Calculate cache miss rate
                    if [[ "$cache_refs" != "" && "$cache_misses" != "" && "$cache_refs" -gt 0 ]]; then
                        cache_miss_rate=$(echo "scale=4; $cache_misses / $cache_refs" | bc -l 2>/dev/null || echo "0")
                    else
                        cache_miss_rate="0"
                    fi
                    
                    echo "$timestamp,$pid,$process_name,$cycles_per_sec,$instructions_per_sec,$ipc,$cache_miss_rate,$page_faults_per_sec"
                fi
            done
            
            sleep 0.$(echo "1000 - $SAMPLING_FREQ" | bc)  # Adjust for processing time
        done
    } > $PERF_LOG_FILE &
    
    PERF_STAT_PID=$!
    
    echo "Continuous monitoring started:"
    echo "  - perf record PID: $PERF_RECORD_PID"
    echo "  - perf stat PID: $PERF_STAT_PID"
    echo "  - Data file: $PERF_DATA_FILE"
    echo "  - Log file: $PERF_LOG_FILE"
    
    # Save PIDs for cleanup
    echo $PERF_RECORD_PID > "${OUTPUT_DIR}/${EXPERIMENT_NAME}_record.pid"
    echo $PERF_STAT_PID > "${OUTPUT_DIR}/${EXPERIMENT_NAME}_stat.pid"
}

# Function to stop monitoring
stop_monitoring() {
    echo "Stopping perf monitoring..."
    
    if [ -f "${OUTPUT_DIR}/${EXPERIMENT_NAME}_record.pid" ]; then
        local record_pid=$(cat "${OUTPUT_DIR}/${EXPERIMENT_NAME}_record.pid")
        kill $record_pid 2>/dev/null
        rm "${OUTPUT_DIR}/${EXPERIMENT_NAME}_record.pid"
    fi
    
    if [ -f "${OUTPUT_DIR}/${EXPERIMENT_NAME}_stat.pid" ]; then
        local stat_pid=$(cat "${OUTPUT_DIR}/${EXPERIMENT_NAME}_stat.pid")
        kill $stat_pid 2>/dev/null
        rm "${OUTPUT_DIR}/${EXPERIMENT_NAME}_stat.pid"
    fi
    
    echo "Generating final report..."
    if [ -f "$PERF_DATA_FILE" ]; then
        perf report -i $PERF_DATA_FILE --stdio > "${OUTPUT_DIR}/${EXPERIMENT_NAME}_report.txt"
        echo "Perf report saved to: ${OUTPUT_DIR}/${EXPERIMENT_NAME}_report.txt"
    fi
    
    echo "Monitoring stopped. Files saved:"
    echo "  - Raw data: $PERF_DATA_FILE"
    echo "  - CSV metrics: $PERF_LOG_FILE"
    echo "  - Analysis report: ${OUTPUT_DIR}/${EXPERIMENT_NAME}_report.txt"
}

# Handle script termination
trap 'stop_monitoring; exit 0' INT TERM

# Main execution
case "${1}" in
    "stop")
        stop_monitoring
        ;;
    *)
        # Default: start monitoring with experiment name as first argument
        start_continuous_perf
        echo "Press Ctrl+C to stop monitoring"
        wait  # Wait for background processes
        ;;
esac 