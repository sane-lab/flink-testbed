#!/bin/bash

# Setup script to configure perf security for automated execution
# Run this script once with sudo to enable non-root perf access

echo "Setting up perf security for automated execution..."

# Check if running as root
if [[ $EUID -eq 0 ]]; then
    echo "Running as root, configuring perf security..."
    
    # Set perf_event_paranoid to -1 (allow all users)
    echo -1 > /proc/sys/kernel/perf_event_paranoid
    echo "Set perf_event_paranoid to -1"
    
    # Make the setting persistent across reboots
    if [[ -f /etc/sysctl.conf ]]; then
        # Check if setting already exists
        if ! grep -q "kernel.perf_event_paranoid" /etc/sysctl.conf; then
            echo "kernel.perf_event_paranoid = -1" >> /etc/sysctl.conf
            echo "Added persistent setting to /etc/sysctl.conf"
        else
            echo "Setting already exists in /etc/sysctl.conf"
        fi
    else
        echo "/etc/sysctl.conf not found, setting may not persist across reboots"
    fi
    
    # Apply sysctl settings
    sysctl -p >/dev/null 2>&1
    echo "Applied sysctl settings"
    
    # Test perf access
    if perf stat -e cycles sleep 1 >/dev/null 2>&1; then
        echo "Perf security configured successfully!"
        echo "You can now run overhead scripts without sudo"
    else
        echo "Perf configuration failed"
        exit 1
    fi
    
else
    echo "This script must be run as root (use sudo)"
    echo "Usage: sudo $0"
    exit 1
fi

echo ""
echo "Setup complete! You can now run the overhead scripts without sudo."
echo "Example: ./part8-overhead_stock-local.sh" 