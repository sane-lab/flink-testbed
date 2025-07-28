#!/bin/bash

echo "=== Testing Perf Parsing Logic ==="
echo

# Sample perf output from user's manual test
SAMPLE_PERF_OUTPUT='Performance counter stats for process id '"'"'6374'"'"':

    72,680,201,070      cycles

       2.001305493 seconds time elapsed'

echo "Sample perf output:"
echo "$SAMPLE_PERF_OUTPUT"
echo

# Test our parsing logic
echo "Testing parsing logic:"

# Original parsing (what was failing)
CYCLES_OLD=$(echo "$SAMPLE_PERF_OUTPUT" | grep -w "cycles" | awk '{gsub(/,/, ""); print $1}' | head -1)
echo "Old parsing result: '$CYCLES_OLD'"

# New parsing (current approach)
CYCLES_NEW=$(echo "$SAMPLE_PERF_OUTPUT" | grep -E "^\s*[0-9,]+\s+cycles" | sed 's/,//g' | awk '{print $1}')
echo "New parsing result: '$CYCLES_NEW'"

# Alternative parsing approaches to try
CYCLES_ALT1=$(echo "$SAMPLE_PERF_OUTPUT" | grep "cycles" | sed 's/,//g' | awk '{print $1}')
echo "Alternative 1: '$CYCLES_ALT1'"

CYCLES_ALT2=$(echo "$SAMPLE_PERF_OUTPUT" | grep -o "[0-9,]*[0-9]\s*cycles" | sed 's/,//g' | awk '{print $1}')
echo "Alternative 2: '$CYCLES_ALT2'"

CYCLES_ALT3=$(echo "$SAMPLE_PERF_OUTPUT" | awk '/cycles/ {gsub(/,/, ""); print $1}')
echo "Alternative 3: '$CYCLES_ALT3'"

echo
echo "Expected result: 72680201070"

# Test with your actual cluster - run this part on your system
echo
echo "=== Live Test (run this on your cluster) ==="
echo "# Get current Flink PIDs"
echo "jps | grep -E 'StandaloneSessionClusterEntrypoint|TaskManagerRunner'"
echo
echo "# Test perf manually on one PID"
echo "TESTPID=\$(jps | grep TaskManagerRunner | awk '{print \$1}')"
echo "echo \"Testing PID: \$TESTPID\""
echo "perf stat -p \$TESTPID -e cycles,instructions,cache-misses 2>&1" 