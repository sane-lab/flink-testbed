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
                        
                        # Classify process and accumulate metrics
                        if 'TaskManager' in process_name:
                            target = result['taskmanager']
                        elif 'StandaloneSessionClusterEntrypoint' in process_name:
                            target = result['jobmanager']
                        elif 'Kafka' in process_name or 'QuorumPeerMain' in process_name:
                            target = result['kafka_zookeeper']
                        else:
                            continue
                        
                        # Accumulate values for averaging
                        target['avg_rss_kb'] += rss_kb
                        target['avg_read_bytes'] += read_bytes
                        target['avg_write_bytes'] += write_bytes
                        target['avg_rchar'] += rchar
                        target['avg_wchar'] += wchar
                        target['avg_minor_faults'] += minor_faults
                        target['avg_major_faults'] += major_faults
                        target['avg_llc_misses'] += llc_misses
                        target['count'] += 1
                        
                    except (ValueError, IndexError):
                        continue
        
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
        
        # Calculate averages for system metrics
        for group_name, group_data in result.items():
            if group_data['count'] > 0:
                group_data['avg_rss_kb'] = group_data['avg_rss_kb'] // group_data['count']
                group_data['avg_read_bytes'] = group_data['avg_read_bytes'] // group_data['count']
                group_data['avg_write_bytes'] = group_data['avg_write_bytes'] // group_data['count']
                group_data['avg_rchar'] = group_data['avg_rchar'] // group_data['count']
                group_data['avg_wchar'] = group_data['avg_wchar'] // group_data['count']
                group_data['avg_minor_faults'] = group_data['avg_minor_faults'] // group_data['count']
                group_data['avg_major_faults'] = group_data['avg_major_faults'] // group_data['count']
                group_data['avg_llc_misses'] = group_data['avg_llc_misses'] // group_data['count']
        
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
        
        # For total system metrics, average across all processes
        total_count = (result['taskmanager']['count'] + 
                      result['jobmanager']['count'] + 
                      result['kafka_zookeeper']['count'])
        
        if total_count > 0:
            result['total']['avg_rss_kb'] = ((result['taskmanager']['avg_rss_kb'] * result['taskmanager']['count'] +
                                            result['jobmanager']['avg_rss_kb'] * result['jobmanager']['count'] +
                                            result['kafka_zookeeper']['avg_rss_kb'] * result['kafka_zookeeper']['count']) // total_count)
            result['total']['avg_read_bytes'] = ((result['taskmanager']['avg_read_bytes'] * result['taskmanager']['count'] +
                                                result['jobmanager']['avg_read_bytes'] * result['jobmanager']['count'] +
                                                result['kafka_zookeeper']['avg_read_bytes'] * result['kafka_zookeeper']['count']) // total_count)
            result['total']['avg_write_bytes'] = ((result['taskmanager']['avg_write_bytes'] * result['taskmanager']['count'] +
                                                 result['jobmanager']['avg_write_bytes'] * result['jobmanager']['count'] +
                                                 result['kafka_zookeeper']['avg_write_bytes'] * result['kafka_zookeeper']['count']) // total_count)
            result['total']['avg_rchar'] = ((result['taskmanager']['avg_rchar'] * result['taskmanager']['count'] +
                                           result['jobmanager']['avg_rchar'] * result['jobmanager']['count'] +
                                           result['kafka_zookeeper']['avg_rchar'] * result['kafka_zookeeper']['count']) // total_count)
            result['total']['avg_wchar'] = ((result['taskmanager']['avg_wchar'] * result['taskmanager']['count'] +
                                           result['jobmanager']['avg_wchar'] * result['jobmanager']['count'] +
                                           result['kafka_zookeeper']['avg_wchar'] * result['kafka_zookeeper']['count']) // total_count)
            result['total']['avg_minor_faults'] = ((result['taskmanager']['avg_minor_faults'] * result['taskmanager']['count'] +
                                                   result['jobmanager']['avg_minor_faults'] * result['jobmanager']['count'] +
                                                   result['kafka_zookeeper']['avg_minor_faults'] * result['kafka_zookeeper']['count']) // total_count)
            result['total']['avg_major_faults'] = ((result['taskmanager']['avg_major_faults'] * result['taskmanager']['count'] +
                                                   result['jobmanager']['avg_major_faults'] * result['jobmanager']['count'] +
                                                   result['kafka_zookeeper']['avg_major_faults'] * result['kafka_zookeeper']['count']) // total_count)
            result['total']['avg_llc_misses'] = ((result['taskmanager']['avg_llc_misses'] * result['taskmanager']['count'] +
                                                result['jobmanager']['avg_llc_misses'] * result['jobmanager']['count'] +
                                                result['kafka_zookeeper']['avg_llc_misses'] * result['kafka_zookeeper']['count']) // total_count)
        
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
                    'avg_ReadBytes': group_data['avg_read_bytes'],
                    'avg_WriteBytes': group_data['avg_write_bytes'],
                    'avg_RChar': group_data['avg_rchar'],
                    'avg_WChar': group_data['avg_wchar'],
                    'avg_MinorFaults': group_data['avg_minor_faults'],
                    'avg_MajorFaults': group_data['avg_major_faults'],
                    'avg_LLC_Misses': group_data['avg_llc_misses']
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
                  'avg_RSS_KB', 'avg_ReadBytes', 'avg_WriteBytes', 'avg_RChar', 'avg_WChar',
                  'avg_MinorFaults', 'avg_MajorFaults', 'avg_LLC_Misses']
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\nResults saved to: {output_file}")
    
    # Display summary table
    print(f"\nPart 8 Overhead Analysis Results:")
    print("=" * 180)
    
    # Print header
    header = f"{'Workload':<10} {'Config':<18} {'Process':<15} {'Cycles':<15} {'Instructions':<15} {'RSS_KB':<12} {'ReadBytes':<12} {'WriteBytes':<12} {'RChar':<12} {'WChar':<12} {'MinorFaults':<12} {'MajorFaults':<12} {'LLC_Misses':<12}"
    print(header)
    print("-" * 180)
    
    # Print data rows
    for result in results:
        row = f"{result['Workload']:<10} {result['Configuration']:<18} {result['Process_Group']:<15} {result['Total_Cycles']:>14,} {result['Instructions']:>14,} {result['avg_RSS_KB']:>11,} {result['avg_ReadBytes']:>11,} {result['avg_WriteBytes']:>11,} {result['avg_RChar']:>11,} {result['avg_WChar']:>11,} {result['avg_MinorFaults']:>11,} {result['avg_MajorFaults']:>11,} {result['avg_LLC_Misses']:>11,}"
        print(row)

if __name__ == "__main__":
    main()
