#!/bin/bash

# Test script to verify perf is working correctly
echo "Testing perf functionality..."

# Test 1: Check if perf is available
if ! command -v perf >/dev/null 2>&1; then
    echo "ERROR: perf command not found!"
    exit 1
fi

echo "✅ perf command found"

# Test 2: Check if we can run perf without sudo
if perf stat -e cycles sleep 1 >/dev/null 2>&1; then
    echo "✅ perf works without sudo"
    PERF_NEEDS_SUDO=false
else
    echo "⚠️  perf requires sudo"
    PERF_NEEDS_SUDO=true
fi

# Test 3: Test perf with a simple process
echo "Testing perf with sleep process..."
SLEEP_PID=$(sleep 10 & echo $!)

if [[ "$PERF_NEEDS_SUDO" == "true" ]]; then
    sudo perf stat -p $SLEEP_PID -e cycles,instructions,cache-misses sleep 2 > /tmp/test_perf_output.txt 2>&1
else
    perf stat -p $SLEEP_PID -e cycles,instructions,cache-misses sleep 2 > /tmp/test_perf_output.txt 2>&1
fi

echo "Perf output:"
cat /tmp/test_perf_output.txt

# Test 4: Extract values
CYCLES=$(cat /tmp/test_perf_output.txt | awk '/cycles/ {gsub(/,/, ""); print $1}' | head -1)
INSTRUCTIONS=$(cat /tmp/test_perf_output.txt | awk '/instructions/ {gsub(/,/, ""); print $1}' | head -1)
CACHE_MISSES=$(cat /tmp/test_perf_output.txt | awk '/cache-misses/ {gsub(/,/, ""); print $1}' | head -1)

echo ""
echo "Extracted values:"
echo "Cycles: $CYCLES"
echo "Instructions: $INSTRUCTIONS"
echo "Cache misses: $CACHE_MISSES"

# Test 5: Validate values
if [[ -n "$CYCLES" ]] && [[ "$CYCLES" != "0" ]] && [[ "$CYCLES" -gt 1000 ]]; then
    echo "✅ Cycles extraction working correctly"
else
    echo "❌ Cycles extraction failed or invalid value: $CYCLES"
fi

if [[ -n "$INSTRUCTIONS" ]] && [[ "$INSTRUCTIONS" != "0" ]] && [[ "$INSTRUCTIONS" -gt 1000 ]]; then
    echo "✅ Instructions extraction working correctly"
else
    echo "❌ Instructions extraction failed or invalid value: $INSTRUCTIONS"
fi

if [[ -n "$CACHE_MISSES" ]]; then
    echo "✅ Cache misses extraction working correctly"
else
    echo "❌ Cache misses extraction failed or invalid value: $CACHE_MISSES"
fi

# Cleanup
rm -f /tmp/test_perf_output.txt
kill $SLEEP_PID 2>/dev/null

echo ""
echo "Test completed!" 