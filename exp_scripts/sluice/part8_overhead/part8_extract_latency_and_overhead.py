#!/usr/bin/env python3
"""
Part 8 Overhead Analysis Script - Clean Table Output Only
Generates comprehensive overhead metrics table without verbose analysis text.
"""

import sys
import os
import csv

def find_system_monitor_file(directory):
    """Find system monitor file."""
    try:
        files = os.listdir(directory)
        for filename in files:
            if filename.startswith("system_monitor_"):
                return os.path.join(directory, filename)
        return None
    except:
        return None

def find_monitor_file(directory):
    """Find separate monitor file for CPU cycles."""
    try:
        files = os.listdir(directory)
        for filename in files:
            if filename.startswith("monitor_") and filename.endswith(".out"):
                return os.path.join(directory, filename)
        return None
    except:
        return None

def read_cpu_cycles_from_monitor_file(monitor_file):
    """Read CPU cycles data from separate monitor file."""
    try:
        with open(monitor_file, 'r') as f:
            content = f.read()
        
        cycles_data = {
            'taskmanager': {'total_cycles': 0, 'instructions': 0, 'cache_misses': 0},
            'jobmanager': {'total_cycles': 0, 'instructions': 0, 'cache_misses': 0},
            'kafka_zookeeper': {'total_cycles': 0, 'instructions': 0, 'cache_misses': 0}
        }
        
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            if not line or line.startswith('INFO:') or line.startswith('Timestamp') or line.startswith('perf_'):
                continue
            
            if ',' in line and '[FLINK-' in line or '[KAFKA]' in line or '[ZOOKEEPER]' in line:
                parts = [p.strip() for p in line.split(',')]
                if len(parts) >= 7:
                    try:
                        process_info = parts[2]
                        cycles = int(parts[3]) if parts[3].isdigit() else 0
                        instructions = int(parts[4]) if parts[4].isdigit() else 0
                        cache_misses = int(parts[5]) if parts[5].isdigit() else 0
                        
                        if '[FLINK-PRIMARY]' in process_info:
                            cycles_data['taskmanager']['total_cycles'] = cycles
                            cycles_data['taskmanager']['instructions'] = instructions
                            cycles_data['taskmanager']['cache_misses'] = cache_misses
                        elif '[FLINK-MASTER]' in process_info:
                            cycles_data['jobmanager']['total_cycles'] = cycles
                            cycles_data['jobmanager']['instructions'] = instructions
                            cycles_data['jobmanager']['cache_misses'] = cache_misses
                        elif '[KAFKA]' in process_info or '[ZOOKEEPER]' in process_info:
                            cycles_data['kafka_zookeeper']['total_cycles'] += cycles
                            cycles_data['kafka_zookeeper']['instructions'] += instructions
                            cycles_data['kafka_zookeeper']['cache_misses'] += cache_misses
                        
                    except (ValueError, IndexError):
                        continue
        
        return cycles_data
        
    except Exception:
        return None

def extract_comprehensive_metrics(exp_path):
    """Extract both CPU cycles and system monitoring metrics."""
    try:
        # Find system monitor file
        system_file = find_system_monitor_file(exp_path)
        if not system_file or not os.path.exists(system_file):
            return None
        
        # Read file content
        with open(system_file, 'r') as f:
            content = f.read()
        
        lines = content.split('\n')
        
        # Initialize results for each process group
        result = {
            'taskmanager': {
                'total_cycles': 0, 'instructions': 0, 'cache_misses': 0,
                'avg_rss_kb': 0, 'avg_read_bytes': 0, 'avg_write_bytes': 0,
                'avg_rchar': 0, 'avg_wchar': 0, 'avg_minor_faults': 0,
                'avg_major_faults': 0, 'avg_llc_misses': 0, 'count': 0
            },
            'jobmanager': {
                'total_cycles': 0, 'instructions': 0, 'cache_misses': 0,
                'avg_rss_kb': 0, 'avg_read_bytes': 0, 'avg_write_bytes': 0,
                'avg_rchar': 0, 'avg_wchar': 0, 'avg_minor_faults': 0,
                'avg_major_faults': 0, 'avg_llc_misses': 0, 'count': 0
            },
            'kafka_zookeeper': {
                'total_cycles': 0, 'instructions': 0, 'cache_misses': 0,
                'avg_rss_kb': 0, 'avg_read_bytes': 0, 'avg_write_bytes': 0,
                'avg_rchar': 0, 'avg_wchar': 0, 'avg_minor_faults': 0,
                'avg_major_faults': 0, 'avg_llc_misses': 0, 'count': 0
            },
            'total': {
                'total_cycles': 0, 'instructions': 0, 'cache_misses': 0,
                'avg_rss_kb': 0, 'avg_read_bytes': 0, 'avg_write_bytes': 0,
                'avg_rchar': 0, 'avg_wchar': 0, 'avg_minor_faults': 0,
                'avg_major_faults': 0, 'avg_llc_misses': 0, 'count': 0
            }
        }
        
        # Store first and last values for rate calculation
        process_data = {
            'taskmanager': {'first': None, 'last': None, 'rss_samples': []},
            'jobmanager': {'first': None, 'last': None, 'rss_samples': []},
            'kafka_zookeeper': {'first': None, 'last': None, 'rss_samples': []}
        }
        
        # Parse CSV data (system monitoring metrics)
        csv_started = False
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Look for CSV header
            if line.startswith('Timestamp,PID,ProcessName,RSS_KB,ReadBytes,WriteBytes,RChar,WChar,MinorFaults,MajorFaults,LLC_Misses'):
                csv_started = True
                continue
            
            # Parse CSV data lines (skip INFO lines and CPU cycles lines with process labels)
            if csv_started and ',' in line and not line.startswith('INFO:') and '[FLINK-' not in line and '[KAFKA]' not in line and '[ZOOKEEPER]' not in line:
                parts = [p.strip() for p in line.split(',')]
                if len(parts) >= 11:
                    try:
                        timestamp = parts[0]
                        pid = parts[1]
                        process_name = parts[2]
                        rss_kb = int(parts[3]) if parts[3].isdigit() else 0
                        read_bytes = int(parts[4]) if parts[4].isdigit() else 0
                        write_bytes = int(parts[5]) if parts[5].isdigit() else 0
                        rchar = int(parts[6]) if parts[6].isdigit() else 0
                        wchar = int(parts[7]) if parts[7].isdigit() else 0
                        minor_faults = int(parts[8]) if parts[8].isdigit() else 0
                        major_faults = int(parts[9]) if parts[9].isdigit() else 0
                        llc_misses = int(parts[10]) if parts[10].isdigit() else 0
                        
                        # Parse timestamp for duration calculation
                        try:
                            from datetime import datetime
                            ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                        except:
                            continue
                        
                        # Classify process and store data for rate calculation
                        target_key = None
                        if 'TaskManager' in process_name:
                            target_key = 'taskmanager'
                        elif 'StandaloneSessionClusterEntrypoint' in process_name:
                            target_key = 'jobmanager'
                        elif 'Kafka' in process_name or 'QuorumPeerMain' in process_name:
                            target_key = 'kafka_zookeeper'
                        else:
                            continue
                        
                        data_point = {
                            'timestamp': ts,
                            'rss_kb': rss_kb,
                            'read_bytes': read_bytes,
                            'write_bytes': write_bytes,
                            'rchar': rchar,
                            'wchar': wchar,
                            'minor_faults': minor_faults,
                            'major_faults': major_faults,
                            'llc_misses': llc_misses
                        }
                        
                        # Store first and last data points for rate calculation
                        if process_data[target_key]['first'] is None:
                            process_data[target_key]['first'] = data_point
                        process_data[target_key]['last'] = data_point
                        
                        # Collect RSS samples for averaging (non-cumulative metric)
                        process_data[target_key]['rss_samples'].append(rss_kb)
                        
                    except (ValueError, IndexError):
                        continue
        
        # Calculate rates and averages for each process group
        for target_key in ['taskmanager', 'jobmanager', 'kafka_zookeeper']:
            if process_data[target_key]['first'] and process_data[target_key]['last']:
                first = process_data[target_key]['first']
                last = process_data[target_key]['last']
                
                # Calculate duration in minutes
                duration_seconds = (last['timestamp'] - first['timestamp']).total_seconds()
                duration_minutes = max(duration_seconds / 60.0, 1.0)  # Avoid division by zero
                
                target = result[target_key]
                
                # Calculate per-minute rates from cumulative values
                target['avg_read_bytes'] = max(0, int((last['read_bytes'] - first['read_bytes']) / duration_minutes))
                target['avg_write_bytes'] = max(0, int((last['write_bytes'] - first['write_bytes']) / duration_minutes))
                target['avg_rchar'] = max(0, int((last['rchar'] - first['rchar']) / duration_minutes))
                target['avg_wchar'] = max(0, int((last['wchar'] - first['wchar']) / duration_minutes))
                target['avg_minor_faults'] = max(0, int((last['minor_faults'] - first['minor_faults']) / duration_minutes))
                target['avg_major_faults'] = max(0, int((last['major_faults'] - first['major_faults']) / duration_minutes))
                target['avg_llc_misses'] = max(0, int((last['llc_misses'] - first['llc_misses']) / duration_minutes))
                
                # Calculate average RSS (non-cumulative metric)
                rss_samples = process_data[target_key]['rss_samples']
                if rss_samples:
                    target['avg_rss_kb'] = sum(rss_samples) // len(rss_samples)
                
                target['count'] = len(rss_samples)
        
        # Parse CPU cycles data from system_monitor.csv first
        cpu_cycles_found = False
        for line in lines:
            line = line.strip()
            if not line or line.startswith('INFO:') or line.startswith('perf_'):
                continue
            
            # Look for CPU cycles data lines
            if line.startswith('2025-07-28') and ',' in line:
                parts = [p.strip() for p in line.split(',')]
                if len(parts) >= 7:
                    try:
                        process_info = parts[2]
                        cycles = int(parts[3]) if parts[3].isdigit() else 0
                        instructions = int(parts[4]) if parts[4].isdigit() else 0
                        cache_misses = int(parts[5]) if parts[5].isdigit() else 0
                        
                        if '[FLINK-PRIMARY]' in process_info:
                            result['taskmanager']['total_cycles'] = cycles
                            result['taskmanager']['instructions'] = instructions
                            result['taskmanager']['cache_misses'] = cache_misses
                            cpu_cycles_found = True
                        elif '[FLINK-MASTER]' in process_info:
                            result['jobmanager']['total_cycles'] = cycles
                            result['jobmanager']['instructions'] = instructions
                            result['jobmanager']['cache_misses'] = cache_misses
                            cpu_cycles_found = True
                        elif '[KAFKA]' in process_info or '[ZOOKEEPER]' in process_info:
                            result['kafka_zookeeper']['total_cycles'] += cycles
                            result['kafka_zookeeper']['instructions'] += instructions
                            result['kafka_zookeeper']['cache_misses'] += cache_misses
                            cpu_cycles_found = True
                        
                    except (ValueError, IndexError):
                        continue
        
        # If no CPU cycles found in system_monitor.csv, try separate monitor file
        if not cpu_cycles_found:
            monitor_file = find_monitor_file(exp_path)
            if monitor_file and os.path.exists(monitor_file):
                cycles_data = read_cpu_cycles_from_monitor_file(monitor_file)
                if cycles_data:
                    result['taskmanager']['total_cycles'] = cycles_data['taskmanager']['total_cycles']
                    result['taskmanager']['instructions'] = cycles_data['taskmanager']['instructions']
                    result['taskmanager']['cache_misses'] = cycles_data['taskmanager']['cache_misses']
                    
                    result['jobmanager']['total_cycles'] = cycles_data['jobmanager']['total_cycles']
                    result['jobmanager']['instructions'] = cycles_data['jobmanager']['instructions']
                    result['jobmanager']['cache_misses'] = cycles_data['jobmanager']['cache_misses']
                    
                    result['kafka_zookeeper']['total_cycles'] = cycles_data['kafka_zookeeper']['total_cycles']
                    result['kafka_zookeeper']['instructions'] = cycles_data['kafka_zookeeper']['instructions']
                    result['kafka_zookeeper']['cache_misses'] = cycles_data['kafka_zookeeper']['cache_misses']
        
        # Calculate totals
        result['total']['total_cycles'] = (result['taskmanager']['total_cycles'] + 
                                         result['jobmanager']['total_cycles'] + 
                                         result['kafka_zookeeper']['total_cycles'])
        result['total']['instructions'] = (result['taskmanager']['instructions'] + 
                                         result['jobmanager']['instructions'] + 
                                         result['kafka_zookeeper']['instructions'])
        result['total']['cache_misses'] = (result['taskmanager']['cache_misses'] + 
                                         result['jobmanager']['cache_misses'] + 
                                         result['kafka_zookeeper']['cache_misses'])
        
        # For total system metrics, calculate weighted averages based on process count
        total_count = (result['taskmanager']['count'] + 
                      result['jobmanager']['count'] + 
                      result['kafka_zookeeper']['count'])
        
        if total_count > 0:
            # Average RSS (non-cumulative)
            result['total']['avg_rss_kb'] = ((result['taskmanager']['avg_rss_kb'] * result['taskmanager']['count'] +
                                            result['jobmanager']['avg_rss_kb'] * result['jobmanager']['count'] +
                                            result['kafka_zookeeper']['avg_rss_kb'] * result['kafka_zookeeper']['count']) // total_count)
            
            # Sum per-minute rates for total system throughput
            result['total']['avg_read_bytes'] = (result['taskmanager']['avg_read_bytes'] + 
                                               result['jobmanager']['avg_read_bytes'] + 
                                               result['kafka_zookeeper']['avg_read_bytes'])
            result['total']['avg_write_bytes'] = (result['taskmanager']['avg_write_bytes'] + 
                                                result['jobmanager']['avg_write_bytes'] + 
                                                result['kafka_zookeeper']['avg_write_bytes'])
            result['total']['avg_rchar'] = (result['taskmanager']['avg_rchar'] + 
                                          result['jobmanager']['avg_rchar'] + 
                                          result['kafka_zookeeper']['avg_rchar'])
            result['total']['avg_wchar'] = (result['taskmanager']['avg_wchar'] + 
                                          result['jobmanager']['avg_wchar'] + 
                                          result['kafka_zookeeper']['avg_wchar'])
            result['total']['avg_minor_faults'] = (result['taskmanager']['avg_minor_faults'] + 
                                                  result['jobmanager']['avg_minor_faults'] + 
                                                  result['kafka_zookeeper']['avg_minor_faults'])
            result['total']['avg_major_faults'] = (result['taskmanager']['avg_major_faults'] + 
                                                  result['jobmanager']['avg_major_faults'] + 
                                                  result['kafka_zookeeper']['avg_major_faults'])
            result['total']['avg_llc_misses'] = (result['taskmanager']['avg_llc_misses'] + 
                                               result['jobmanager']['avg_llc_misses'] + 
                                               result['kafka_zookeeper']['avg_llc_misses'])
            
            result['total']['count'] = total_count
        
        return result if result['total']['total_cycles'] > 0 else None
        
    except Exception:
        return None

def main():
    """Main function using predefined experiment configurations."""
    
    # Configuration settings
    raw_data_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/raw/"
    output_dir = "/Users/swrrt/Workplace/BacklogDelayPaper/experiments/figures/part8/"
    
    # Experiment configurations from the original script
    experiments = {
        "Linear-Road": {
            "Without_Sluice": "part8-lr-NoControll-100000000-1360-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            "With_Sluice_5ms": "part8-lr-StreamSluice-5000000-1360-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            "With_Sluice_25ms": "part8-lr-StreamSluice-25000000-1360-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            "With_Sluice_100ms": "part8-lr-StreamSluice-100000000-1360-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
        },
        "Stock": {
            "Without_Sluice": "part8-stock-NoControll-100000000-1360-90-1000-20-1-200-4-1111-1-200-1-166-1-5-1666-3000-100-0.1-false-false-1",
            "With_Sluice_5ms": "part8-stock-StreamSluice-5000000-1360-90-1000-20-1-200-4-1111-1-200-1-166-1-5-1666-3000-100-0.1-false-true-1",
            "With_Sluice_25ms": "part8-stock-StreamSluice-25000000-1360-90-1000-20-1-200-4-1111-1-200-1-166-1-5-1666-3000-100-0.1-false-true-1",
            "With_Sluice_100ms": "part8-stock-StreamSluice-100000000-1360-90-1000-20-1-200-4-1111-1-200-1-166-1-5-1666-3000-100-0.1-false-true-1",
        },
        "Twitter": {
            "Without_Sluice": "part8-twitter-NoControll-100000000-1360-90-3400-1-7-1111-3-166-1-50-1-50-2000-0.1-100--1250-0.0-false-1000-0.8-1",
            "With_Sluice_5ms": "part8-twitter-StreamSluice-5000000-1360-90-3400-1-7-1111-3-166-1-50-1-50-2000-0.1-100--1250-0.0-false-1000-0.8-1",
            "With_Sluice_25ms": "part8-twitter-StreamSluice-25000000-1360-90-3400-1-7-1111-3-166-1-50-1-50-2000-0.1-100--1250-0.0-false-1000-0.8-1",
            "With_Sluice_100ms": "part8-twitter-StreamSluice-100000000-1360-90-3400-1-7-1111-3-166-1-50-1-50-2000-0.1-100--1250-0.0-false-1000-0.8-1",
        },
        "ML-Scoring": {
            "Without_Sluice": "part8-ml-NoControll-100000000-1360-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            "With_Sluice_5ms": "part8-ml-StreamSluice-5000000-1360-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            "With_Sluice_25ms": "part8-ml-StreamSluice-25000000-1360-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            "With_Sluice_100ms": "part8-ml-StreamSluice-100000000-1360-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
        }
    }
    
    results = []
    
    print("Processing Part 8 Overhead Analysis...")
    
    for workload_name, workload_configs in experiments.items():
        for config_name, exp_name in workload_configs.items():
            exp_path = os.path.join(raw_data_dir, exp_name)
            
            if not os.path.exists(exp_path):
                print(f"Experiment not found: {exp_name}")
                continue
            
            metrics = extract_comprehensive_metrics(exp_path)
            if not metrics:
                print(f"No metrics found: {workload_name}-{config_name}")
                continue
            
            # Create rows for each process group
            for process_group in ['taskmanager', 'jobmanager', 'kafka_zookeeper', 'total']:
                group_data = metrics[process_group]
                
                # Map process group names
                if process_group == 'taskmanager':
                    process_name = 'TaskManager'
                elif process_group == 'jobmanager':
                    process_name = 'JobManager'
                elif process_group == 'kafka_zookeeper':
                    process_name = 'Kafka&ZooKeeper'
                else:
                    process_name = 'Total'
                
                result = {
                    'Workload': workload_name,
                    'Configuration': config_name,
                    'Process_Group': process_name,
                    'Total_Cycles': group_data['total_cycles'],
                    'Instructions': group_data['instructions'],
                    'avg_RSS_KB': group_data['avg_rss_kb'],
                    'ReadBytes_per_min': group_data['avg_read_bytes'],
                    'WriteBytes_per_min': group_data['avg_write_bytes'],
                    'RChar_per_min': group_data['avg_rchar'],
                    'WChar_per_min': group_data['avg_wchar'],
                    'MinorFaults_per_min': group_data['avg_minor_faults'],
                    'MajorFaults_per_min': group_data['avg_major_faults'],
                    'LLC_Misses_per_min': group_data['avg_llc_misses']
                }
                results.append(result)
            
            print(f"✓ {workload_name}-{config_name}")
    
    if not results:
        print("No valid results found.")
        return
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Sort results
    results.sort(key=lambda x: (x['Workload'], x['Configuration'], x['Process_Group']))
    
    # Save to CSV
    output_file = os.path.join(output_dir, "part8_overhead_results.csv")
    fieldnames = ['Workload', 'Configuration', 'Process_Group', 'Total_Cycles', 'Instructions',
                  'avg_RSS_KB', 'ReadBytes_per_min', 'WriteBytes_per_min', 'RChar_per_min', 'WChar_per_min',
                  'MinorFaults_per_min', 'MajorFaults_per_min', 'LLC_Misses_per_min']
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\nResults saved to: {output_file}")
    
    # Display summary table
    print(f"\nPart 8 Overhead Analysis Results:")
    print("=" * 180)
    
    # Print header
    header = f"{'Workload':<10} {'Config':<18} {'Process':<15} {'Cycles':<15} {'Instructions':<15} {'RSS_KB':<12} {'ReadB/min':<12} {'WriteB/min':<12} {'RChar/min':<12} {'WChar/min':<12} {'MinorF/min':<12} {'MajorF/min':<12} {'LLCMiss/min':<12}"
    print(header)
    print("-" * 180)
    
    # Print data rows
    for result in results:
        row = f"{result['Workload']:<10} {result['Configuration']:<18} {result['Process_Group']:<15} {result['Total_Cycles']:>14,} {result['Instructions']:>14,} {result['avg_RSS_KB']:>11,} {result['ReadBytes_per_min']:>11,} {result['WriteBytes_per_min']:>11,} {result['RChar_per_min']:>11,} {result['WChar_per_min']:>11,} {result['MinorFaults_per_min']:>11,} {result['MajorFaults_per_min']:>11,} {result['LLC_Misses_per_min']:>11,}"
        print(row)

if __name__ == "__main__":
    main()
