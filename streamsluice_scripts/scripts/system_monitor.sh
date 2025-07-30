#!/bin/bash

# System monitoring script for recording detailed process metrics
# Monitors: TaskManagerRunner, StandaloneSessionClusterEntrypoint, Kafka, QuorumPeerMain

MONITOR_LOG_FILE=""
MONITOR_PID=""
MONITOR_INTERVAL=5  # seconds between measurements
MONITOR_DURATION=1200 #240  # total monitoring duration in seconds

# Kafka monitoring variables
KAFKA_MONITOR_LOG_FILE=""
KAFKA_SAMPLING_INTERVAL=30  # seconds between each kafka sample
KAFKA_SAMPLE_DURATION=30   # duration for each kafka sample
KAFKA_SAMPLE_COUNT=5       # number of samples to take

# JVM monitoring variables
JVM_MONITOR_LOG_FILE=""
JVM_MONITOR_PID=""
JVM_MONITOR_INTERVAL=1  # seconds between measurements (changed from 5 to 1 for better accuracy)
JVM_MONITOR_DURATION=1200  # total monitoring duration in seconds

# Function to get PIDs of target processes
get_target_pids() {
    local pids=()
    local process_names=("TaskManagerRunner" "StandaloneSessionClusterEntrypoint" "Kafka" "QuorumPeerMain")
    
    for process_name in "${process_names[@]}"; do
        local found_pids=$(jps | grep "$process_name" | awk '{print $1}')
        if [[ -n "$found_pids" ]]; then
            pids+=($found_pids)
        fi
    done
    
    echo "${pids[@]}"
}

# Function to get process name from PID
get_process_name() {
    local pid=$1
    jps | grep "$pid" | awk '{print $2}' | head -1
}

# Function to read memory stats from /proc/$pid/statm
read_memory_stats() {
    local pid=$1
    if [[ -f "/proc/$pid/statm" ]]; then
        # statm format: size resident shared text lib data dt
        # We want resident (RSS) which is field 2, in pages
        local statm_data=$(cat /proc/$pid/statm)
        local rss_pages=$(echo $statm_data | awk '{print $2}')
        local rss_kb=$((rss_pages * 4))  # Assuming 4KB pages
        echo "$rss_kb"
    else
        echo "0"
    fi
}

# Function to read I/O and page fault stats from /proc/$pid/status and /proc/$pid/io
read_io_and_fault_stats() {
    local pid=$1
    local io_data="0 0 0 0"
    local fault_data="0 0"
    
    # Read I/O stats from /proc/$pid/io
    if [[ -f "/proc/$pid/io" ]]; then
        local read_bytes=$(grep "read_bytes" /proc/$pid/io | awk '{print $2}')
        local write_bytes=$(grep "write_bytes" /proc/$pid/io | awk '{print $2}')
        local rchar=$(grep "rchar" /proc/$pid/io | awk '{print $2}')
        local wchar=$(grep "wchar" /proc/$pid/io | awk '{print $2}')
        io_data="$read_bytes $write_bytes $rchar $wchar"
    fi
    
    # Read page fault stats from /proc/$pid/status
    if [[ -f "/proc/$pid/status" ]]; then
        local minor_faults=$(grep "VmHWM" /proc/$pid/status | awk '{print $2}' || echo "0")
        local major_faults=$(grep "VmPeak" /proc/$pid/status | awk '{print $2}' || echo "0")
        # Actually get page faults from stat file
        if [[ -f "/proc/$pid/stat" ]]; then
            local stat_data=$(cat /proc/$pid/stat)
            minor_faults=$(echo $stat_data | awk '{print $10}')  # minflt
            major_faults=$(echo $stat_data | awk '{print $12}')  # majflt
        fi
        fault_data="$minor_faults $major_faults"
    fi
    
    echo "$io_data $fault_data"
}

# Function to read LLC (Last Level Cache) misses using perf
read_llc_misses() {
    local pid=$1
    local duration=1  # 1 second sample
    
    # Try to get LLC misses using perf
    local llc_misses="0"
    if command -v perf >/dev/null 2>&1; then
        # Try without sudo first
        if perf stat -p $pid -e LLC-load-misses,LLC-store-misses sleep $duration >/tmp/llc_${pid}.tmp 2>&1; then
            local load_misses=$(grep "LLC-load-misses" /tmp/llc_${pid}.tmp | awk '{gsub(/,/, ""); print $1}' | head -1)
            local store_misses=$(grep "LLC-store-misses" /tmp/llc_${pid}.tmp | awk '{gsub(/,/, ""); print $1}' | head -1)
            load_misses=${load_misses:-0}
            store_misses=${store_misses:-0}
            llc_misses=$((load_misses + store_misses))
        else
            # Try with sudo
            if sudo perf stat -p $pid -e LLC-load-misses,LLC-store-misses sleep $duration >/tmp/llc_${pid}.tmp 2>&1; then
                local load_misses=$(grep "LLC-load-misses" /tmp/llc_${pid}.tmp | awk '{gsub(/,/, ""); print $1}' | head -1)
                local store_misses=$(grep "LLC-store-misses" /tmp/llc_${pid}.tmp | awk '{gsub(/,/, ""); print $1}' | head -1)
                load_misses=${load_misses:-0}
                store_misses=${store_misses:-0}
                llc_misses=$((load_misses + store_misses))
            fi
        fi
        rm -f /tmp/llc_${pid}.tmp
    fi
    
    echo "$llc_misses"
}

# Function to sample Kafka metrics topic
sample_kafka_metrics() {
    local sample_num=$1
    local kafka_log_file=$2
    local sample_duration=${3:-$KAFKA_SAMPLE_DURATION}
    
    echo "INFO: Starting Kafka metrics sampling #$sample_num (${sample_duration}s)..."
    
    # Define Kafka consumer command
    local kafka_consumer_cmd="~/samza-hello-samza/deploy/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic flink_metrics"
    
    # Create temporary file for this sample
    local temp_file="/tmp/kafka_sample_${sample_num}_$(date +%s).tmp"
    
    # Run Kafka consumer with timeout
    timeout ${sample_duration}s bash -c "$kafka_consumer_cmd" > "$temp_file" 2>&1
    
    # Count the number of messages (lines) received
    local message_count=$(wc -l < "$temp_file" 2>/dev/null || echo "0")
    
    # Get sample timestamp
    local sample_time=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Log the results
    echo "$sample_time,Sample_$sample_num,$sample_duration,$message_count" >> "$kafka_log_file"
    
    # Also log a sample of the messages for verification
    if [[ $message_count -gt 0 ]]; then
        echo "# Sample messages from Sample_$sample_num:" >> "$kafka_log_file"
        head -3 "$temp_file" | sed 's/^/# /' >> "$kafka_log_file" 2>/dev/null
        if [[ $message_count -gt 3 ]]; then
            echo "# ... ($message_count total messages)" >> "$kafka_log_file"
        fi
        echo "" >> "$kafka_log_file"
    fi
    
    # Save full sample to a separate file for analysis
    local full_sample_file="/tmp/kafka_full_sample_${sample_num}_$(date +%s).json"
    cp "$temp_file" "$full_sample_file" 2>/dev/null
    
    # Cleanup
    rm -f "$temp_file"
    
    echo "INFO: Kafka sample #$sample_num completed: $message_count messages in ${sample_duration}s"
    echo "INFO: Full sample saved to: $full_sample_file"
    
    return 0
}

# Function to start Kafka metrics monitoring
start_kafka_monitoring() {
    local log_file=$1
    local sample_count=${2:-$KAFKA_SAMPLE_COUNT}
    local sample_interval=${3:-$KAFKA_SAMPLING_INTERVAL}
    local sample_duration=${4:-$KAFKA_SAMPLE_DURATION}
    
    KAFKA_MONITOR_LOG_FILE="$log_file"
    
    {
        # Header
        echo "# Kafka Metrics Topic Sampling Report"
        echo "# Purpose: Verify metrics_report_interval (policy.windowSize) setting is working"
        echo "# Timestamp,SampleID,Duration(s),MessageCount"
        echo ""
        
        echo "INFO: Kafka metrics monitoring started at $(date '+%Y-%m-%d %H:%M:%S')"
        echo "INFO: Will take $sample_count samples, ${sample_duration}s each, every ${sample_interval}s"
        echo ""
        
        # Take multiple samples over time
        for ((i=1; i<=sample_count; i++)); do
            sample_kafka_metrics $i "$log_file" $sample_duration
            
            # Wait before next sample (except for the last one)
            if [[ $i -lt $sample_count ]]; then
                echo "INFO: Waiting ${sample_interval}s before next sample..."
                sleep $sample_interval
            fi
        done
        
        echo ""
        echo "INFO: Kafka metrics monitoring completed at $(date '+%Y-%m-%d %H:%M:%S')"
        
        # Calculate and log summary statistics
        echo ""
        echo "# Summary Statistics:"
        local total_messages=$(grep -v "^#" "$log_file" | grep -v "^INFO:" | grep "Sample_" | awk -F',' '{sum+=$4} END {print sum+0}')
        local avg_messages_per_sample=$((total_messages / sample_count))
        local expected_rate_per_second=$(echo "scale=2; $avg_messages_per_sample / $sample_duration" | bc -l 2>/dev/null || echo "N/A")
        
        echo "# Total messages across all samples: $total_messages"
        echo "# Average messages per sample: $avg_messages_per_sample"
        echo "# Estimated rate: $expected_rate_per_second messages/second"
        echo "# Sample count: $sample_count"
        echo "# Sample duration: ${sample_duration}s each"
        
    } >> "$log_file" &
    
    echo "INFO: Kafka metrics monitoring started"
    echo "INFO: Kafka monitoring log: $log_file"
}

# Function to get PIDs of Flink JVM processes
get_flink_jvm_pids() {
    local pids=()
    local jvm_processes=("TaskManagerRunner" "StandaloneSessionClusterEntrypoint")
    
    for process_name in "${jvm_processes[@]}"; do
        local found_pids=$(jps | grep "$process_name" | awk '{print $1}')
        if [[ -n "$found_pids" ]]; then
            pids+=($found_pids)
        fi
    done
    
    echo "${pids[@]}"
}

# Function to get process name from PID for JVM monitoring
get_jvm_process_name() {
    local pid=$1
    jps | grep "$pid" | awk '{print $2}' | head -1
}

# Function to collect JVM metrics for a specific PID
collect_jvm_metrics() {
    local pid=$1
    local timestamp=$2
    local process_name=$3
    
    # Initialize default values
    local heap_used=0 heap_committed=0 heap_max=0
    local nonheap_used=0 nonheap_committed=0 nonheap_max=0
    local eden_used=0 eden_committed=0 eden_max=0
    local survivor_used=0 survivor_committed=0 survivor_max=0
    local old_used=0 old_committed=0 old_max=0
    local metaspace_used=0 metaspace_committed=0 metaspace_max=0
    local gc_young_count=0 gc_young_time=0 gc_old_count=0 gc_old_time=0
    local thread_count=0 thread_peak=0
    
    # Check if process still exists
    if ! kill -0 $pid 2>/dev/null; then
        echo "$timestamp,$pid,$process_name,PROCESS_DEAD,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        return
    fi
    
    # Get GC statistics using jstat
    if command -v jstat >/dev/null 2>&1; then
        # Get GC capacity and utilization
        local gc_data=$(jstat -gc $pid 2>/dev/null | tail -1)
        if [[ -n "$gc_data" && "$gc_data" != *"Error"* ]]; then
            # Parse jstat -gc output: S0C S1C S0U S1U EC EU OC OU MC MU CCSC CCSU YGC YGCT FGC FGCT GCT
            read -r s0c s1c s0u s1u ec eu oc ou mc mu ccsc ccsu ygc ygct fgc fgct gct <<< "$gc_data"
            
            # Convert KB to bytes and calculate values (handle floating-point by converting to int)
            eden_committed=$(($(echo "${ec:-0}" | cut -d. -f1) * 1024))
            eden_used=$(($(echo "${eu:-0}" | cut -d. -f1) * 1024))
            survivor_committed=$((($(echo "${s0c:-0}" | cut -d. -f1) + $(echo "${s1c:-0}" | cut -d. -f1)) * 1024))
            survivor_used=$((($(echo "${s0u:-0}" | cut -d. -f1) + $(echo "${s1u:-0}" | cut -d. -f1)) * 1024))
            old_committed=$(($(echo "${oc:-0}" | cut -d. -f1) * 1024))
            old_used=$(($(echo "${ou:-0}" | cut -d. -f1) * 1024))
            metaspace_committed=$(($(echo "${mc:-0}" | cut -d. -f1) * 1024))
            metaspace_used=$(($(echo "${mu:-0}" | cut -d. -f1) * 1024))
            
            # Calculate heap totals
            heap_committed=$((eden_committed + survivor_committed + old_committed))
            heap_used=$((eden_used + survivor_used + old_used))
            
            # Non-heap is primarily metaspace
            nonheap_committed=$metaspace_committed
            nonheap_used=$metaspace_used
            
            # GC statistics
            gc_young_count=${ygc:-0}
            gc_young_time=$(echo "${ygct:-0} * 1000" | bc -l 2>/dev/null | cut -d. -f1) # Convert to ms
            gc_old_count=${fgc:-0}
            gc_old_time=$(echo "${fgct:-0} * 1000" | bc -l 2>/dev/null | cut -d. -f1) # Convert to ms
        fi
        
        # Get heap capacity information
        local heap_capacity=$(jstat -gccapacity $pid 2>/dev/null | tail -1)
        if [[ -n "$heap_capacity" && "$heap_capacity" != *"Error"* ]]; then
            # Parse jstat -gccapacity output for maximum values
            read -r ngcmn ngcmx ngc s0cmx s0c s1cmx s1c ecmx ec ogcmn ogcmx ogc oc mcmn mcmx mc ccsmn ccsmx ccsc ygc fgc <<< "$heap_capacity"
            
            # Calculate maximum heap size (KB to bytes) - handle floating-point
            local young_max=$((($(echo "${s0cmx:-0}" | cut -d. -f1) + $(echo "${s1cmx:-0}" | cut -d. -f1) + $(echo "${ecmx:-0}" | cut -d. -f1)) * 1024))
            local old_max_capacity=$(($(echo "${ogcmx:-0}" | cut -d. -f1) * 1024))
            heap_max=$((young_max + old_max_capacity))
            eden_max=$(($(echo "${ecmx:-0}" | cut -d. -f1) * 1024))
            old_max=$old_max_capacity
            metaspace_max=$(($(echo "${mcmx:-0}" | cut -d. -f1) * 1024))
            nonheap_max=$metaspace_max
        fi
    fi
    
    # Get thread information using jstack (count only, to avoid overhead)
    if command -v jstack >/dev/null 2>&1; then
        local thread_info=$(jstack $pid 2>/dev/null | grep "java.lang.Thread.State" | wc -l)
        thread_count=${thread_info:-0}
        thread_peak=$thread_count  # Simplified, real peak would need tracking
    fi
    
    # Output CSV line with comprehensive JVM metrics
    echo "$timestamp,$pid,$process_name,ALIVE,$heap_used,$heap_committed,$heap_max,$nonheap_used,$nonheap_committed,$nonheap_max,$eden_used,$eden_committed,$eden_max,$survivor_used,$survivor_committed,$survivor_max,$old_used,$old_committed,$old_max,$metaspace_used,$metaspace_committed,$metaspace_max,$gc_young_count,$gc_young_time,$gc_old_count,$gc_old_time,$thread_count,$thread_peak"
}

# Function to start JVM monitoring
start_jvm_monitoring() {
    local log_file=$1
    local duration=${2:-$JVM_MONITOR_DURATION}
    local interval=${3:-$JVM_MONITOR_INTERVAL}
    
    JVM_MONITOR_LOG_FILE="$log_file"
    
    {
        # CSV Header
        echo "Timestamp,PID,ProcessName,Status,HeapUsed_Bytes,HeapCommitted_Bytes,HeapMax_Bytes,NonHeapUsed_Bytes,NonHeapCommitted_Bytes,NonHeapMax_Bytes,EdenUsed_Bytes,EdenCommitted_Bytes,EdenMax_Bytes,SurvivorUsed_Bytes,SurvivorCommitted_Bytes,SurvivorMax_Bytes,OldGenUsed_Bytes,OldGenCommitted_Bytes,OldGenMax_Bytes,MetaspaceUsed_Bytes,MetaspaceCommitted_Bytes,MetaspaceMax_Bytes,YoungGC_Count,YoungGC_Time_ms,OldGC_Count,OldGC_Time_ms,ThreadCount,ThreadPeak"
        
        local start_time=$(date +%s)
        local end_time=$((start_time + duration))
        local consecutive_empty_cycles=0
        local max_empty_cycles=10  # Stop if no processes found for 10 consecutive cycles
        
        echo "INFO: JVM monitoring started at $(date '+%Y-%m-%d %H:%M:%S')"
        echo "INFO: Will monitor JVM metrics for ${duration}s with ${interval}s intervals"
        
        # Wait a bit for Flink processes to start up
        echo "INFO: Waiting 10 seconds for Flink processes to start up..."
        sleep 10
        
        while [[ $(date +%s) -lt $end_time ]]; do
            local current_time=$(date '+%Y-%m-%d %H:%M:%S')
            local pids=$(get_flink_jvm_pids)
            
            if [[ -z "$pids" ]]; then
                consecutive_empty_cycles=$((consecutive_empty_cycles + 1))
                echo "WARNING: No Flink JVM processes found at $current_time (cycle $consecutive_empty_cycles/$max_empty_cycles)"
                
                if [[ $consecutive_empty_cycles -ge $max_empty_cycles ]]; then
                    echo "WARNING: Stopping JVM monitoring - no Flink processes found for $max_empty_cycles consecutive cycles"
                    break
                fi
                
                sleep $interval
                continue
            else
                consecutive_empty_cycles=0  # Reset counter when processes are found
                echo "INFO: Found Flink JVM processes: $pids at $current_time"
            fi
            
            for pid in $pids; do
                local process_name=$(get_jvm_process_name $pid)
                if [[ -n "$process_name" ]]; then
                    collect_jvm_metrics $pid "$current_time" "$process_name"
                else
                    echo "WARNING: Could not get process name for PID $pid"
                fi
            done
            
            sleep $interval
        done
        
        echo "INFO: JVM monitoring completed at $(date '+%Y-%m-%d %H:%M:%S')"
        
    } >> "$JVM_MONITOR_LOG_FILE" &
    
    JVM_MONITOR_PID=$!
    echo "INFO: JVM monitoring started with PID: $JVM_MONITOR_PID"
    echo "INFO: JVM monitoring log: $JVM_MONITOR_LOG_FILE"
}

# Function to stop JVM monitoring
stop_jvm_monitoring() {
    if [[ ! -z "$JVM_MONITOR_PID" ]]; then
        echo "INFO: Stopping JVM monitoring (PID: $JVM_MONITOR_PID)..."
        kill $JVM_MONITOR_PID 2>/dev/null
        wait $JVM_MONITOR_PID 2>/dev/null
        echo "INFO: JVM monitoring stopped. Data saved to: $JVM_MONITOR_LOG_FILE"
    fi
}

# Main monitoring function
start_system_monitoring() {
    local log_file=$1
    local duration=${2:-$MONITOR_DURATION}
    local interval=${3:-$MONITOR_INTERVAL}
    
    MONITOR_LOG_FILE="$log_file"
    
    {
        # Header
        echo "Timestamp,PID,ProcessName,RSS_KB,ReadBytes,WriteBytes,RChar,WChar,MinorFaults,MajorFaults,LLC_Misses"
        
        local start_time=$(date +%s)
        local end_time=$((start_time + duration))
        
        echo "INFO: System monitoring started at $(date '+%Y-%m-%d %H:%M:%S')"
        echo "INFO: Will monitor for ${duration}s with ${interval}s intervals"
        
        while [[ $(date +%s) -lt $end_time ]]; do
            local current_time=$(date '+%Y-%m-%d %H:%M:%S')
            local pids=$(get_target_pids)
            
            if [[ -z "$pids" ]]; then
                echo "WARNING: No target processes found at $current_time"
                sleep $interval
                continue
            fi
            
            for pid in $pids; do
                # Check if process still exists
                if ! kill -0 $pid 2>/dev/null; then
                    echo "WARNING: Process $pid no longer exists"
                    continue
                fi
                
                local process_name=$(get_process_name $pid)
                local rss_kb=$(read_memory_stats $pid)
                local io_fault_data=$(read_io_and_fault_stats $pid)
                local llc_misses=$(read_llc_misses $pid)
                
                # Parse io_fault_data
                local read_bytes=$(echo $io_fault_data | awk '{print $1}')
                local write_bytes=$(echo $io_fault_data | awk '{print $2}')
                local rchar=$(echo $io_fault_data | awk '{print $3}')
                local wchar=$(echo $io_fault_data | awk '{print $4}')
                local minor_faults=$(echo $io_fault_data | awk '{print $5}')
                local major_faults=$(echo $io_fault_data | awk '{print $6}')
                
                # Log the data
                echo "$current_time,$pid,$process_name,$rss_kb,$read_bytes,$write_bytes,$rchar,$wchar,$minor_faults,$major_faults,$llc_misses"
            done
            
            sleep $interval
        done
        
        echo "INFO: System monitoring completed at $(date '+%Y-%m-%d %H:%M:%S')"
        
    } >> "$MONITOR_LOG_FILE" &
    
    MONITOR_PID=$!
    echo "INFO: System monitoring started with PID: $MONITOR_PID"
    echo "INFO: Monitoring log: $MONITOR_LOG_FILE"
}

# Function to stop monitoring
stop_system_monitoring() {
    if [[ ! -z "$MONITOR_PID" ]]; then
        echo "INFO: Stopping system monitoring (PID: $MONITOR_PID)..."
        kill $MONITOR_PID 2>/dev/null
        wait $MONITOR_PID 2>/dev/null
        echo "INFO: System monitoring stopped. Data saved to: $MONITOR_LOG_FILE"
    fi
}

# If script is run directly (not sourced)
# Commented out to prevent duplicate monitoring when sourced by experiment scripts
# if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
#     # Parse command line arguments
#     LOG_FILE="${1:-/tmp/system_monitor_$(date +%Y%m%d_%H%M%S).csv}"
#     DURATION="${2:-240}"
#     INTERVAL="${3:-5}"
#     
#     echo "Starting system monitoring..."
#     echo "Log file: $LOG_FILE"
#     echo "Duration: ${DURATION}s"
#     echo "Interval: ${INTERVAL}s"
#     
#     start_system_monitoring "$LOG_FILE" "$DURATION" "$INTERVAL"
#     
#     # Wait for monitoring to complete
#     if [[ ! -z "$MONITOR_PID" ]]; then
#         wait $MONITOR_PID
#     fi
#     
#     echo "System monitoring completed!"
# fi 