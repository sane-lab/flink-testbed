#!/usr/bin/env python3
"""
Part 8 Overhead Analysis Script - Clean Table Output Only
Generates comprehensive overhead metrics table without verbose analysis text.
"""

import sys
import os
import csv

def find_system_monitor_file(directory):
    """Find system monitor file. Prefers new timestamp-free format, falls back to old timestamped format."""
    try:
        files = os.listdir(directory)
        
        # First, try to find the new timestamp-free format
        for filename in files:
            if filename == "system_monitor.csv":
                return os.path.join(directory, filename)
        
        # Fall back to old timestamped format
        for filename in files:
            if filename.startswith("system_monitor_") and filename.endswith(".csv"):
                return os.path.join(directory, filename)
        
        return None
    except:
        return None

def find_monitor_file(directory):
    """Find separate monitor file for CPU cycles. Prefers new timestamp-free format, falls back to old timestamped format."""
    try:
        files = os.listdir(directory)
        
        # First, try to find the new timestamp-free format
        for filename in files:
            if filename == "monitor.out":
                return os.path.join(directory, filename)
        
        # Fall back to old timestamped format
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
            
            if ',' in line and ('[FLINK-' in line or '[KAFKA]' in line or '[ZOOKEEPER]' in line):
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

def find_jvm_metrics_file(directory):
    """Find JVM metrics file. Prefers new timestamp-free format, falls back to old timestamped format."""
    try:
        files = os.listdir(directory)
        
        # First, try to find the new timestamp-free format
        for filename in files:
            if filename == "jvm_metrics.csv":
                return os.path.join(directory, filename)
        
        # Fall back to old timestamped format
        for filename in files:
            if filename.startswith("jvm_metrics_") and filename.endswith(".csv"):
                return os.path.join(directory, filename)
        
        return None
    except:
        return None

def read_jvm_metrics_from_file(jvm_file):
    """Read JVM metrics data from JVM metrics file."""
    try:
        with open(jvm_file, 'r') as f:
            content = f.read()
        
        jvm_data = {
            'taskmanager': {
                'avg_heap_used': 0, 'max_heap_used': 0, 'avg_heap_committed': 0,
                'avg_old_gen_used': 0, 'max_old_gen_used': 0, 'avg_old_gen_committed': 0,
                'avg_eden_used': 0, 'max_eden_used': 0, 'avg_eden_committed': 0,
                'avg_metaspace_used': 0, 'max_metaspace_used': 0,
                'total_young_gc_count': 0, 'total_young_gc_time': 0,
                'total_old_gc_count': 0, 'total_old_gc_time': 0,
                'avg_thread_count': 0, 'max_thread_count': 0,
                'count': 0
            },
            'jobmanager': {
                'avg_heap_used': 0, 'max_heap_used': 0, 'avg_heap_committed': 0,
                'avg_old_gen_used': 0, 'max_old_gen_used': 0, 'avg_old_gen_committed': 0,
                'avg_eden_used': 0, 'max_eden_used': 0, 'avg_eden_committed': 0,
                'avg_metaspace_used': 0, 'max_metaspace_used': 0,
                'total_young_gc_count': 0, 'total_young_gc_time': 0,
                'total_old_gc_count': 0, 'total_old_gc_time': 0,
                'avg_thread_count': 0, 'max_thread_count': 0,
                'count': 0
            }
        }
        
        lines = content.split('\n')
        for line in lines[1:]:  # Skip header
            line = line.strip()
            if not line or line.startswith('INFO:') or line.startswith('WARNING:'):
                continue
            
            parts = [p.strip() for p in line.split(',')]
            if len(parts) >= 28:  # Ensure we have all required fields
                try:
                    process_name = parts[2]
                    status = parts[3]
                    
                    if status != 'ALIVE':
                        continue
                    
                    # Parse numeric values (handle potential empty values)
                    heap_used = int(parts[4]) if parts[4].isdigit() else 0
                    heap_committed = int(parts[5]) if parts[5].isdigit() else 0  # Fixed: was parts[6]
                    eden_used = int(parts[10]) if parts[10].isdigit() else 0
                    eden_committed = int(parts[11]) if parts[11].isdigit() else 0  # Fixed: was parts[12]
                    old_used = int(parts[16]) if parts[16].isdigit() else 0
                    old_committed = int(parts[17]) if parts[17].isdigit() else 0  # Fixed: was parts[18]
                    metaspace_used = int(parts[19]) if parts[19].isdigit() else 0
                    young_gc_count = int(parts[22]) if parts[22].isdigit() else 0
                    young_gc_time = int(parts[23]) if parts[23].isdigit() else 0
                    old_gc_count = int(parts[24]) if parts[24].isdigit() else 0
                    old_gc_time = int(parts[25]) if parts[25].isdigit() else 0
                    thread_count = int(parts[26]) if parts[26].isdigit() else 0
                    
                    # Determine target based on process name
                    target_key = None
                    if 'TaskManagerRunner' in process_name:
                        target_key = 'taskmanager'
                    elif 'StandaloneSessionClusterEntrypoint' in process_name:
                        target_key = 'jobmanager'
                    else:
                        continue
                    
                    target = jvm_data[target_key]
                    
                    # Accumulate values for averaging
                    target['avg_heap_used'] += heap_used
                    target['max_heap_used'] = max(target['max_heap_used'], heap_used)
                    target['avg_heap_committed'] += heap_committed
                    
                    target['avg_old_gen_used'] += old_used
                    target['max_old_gen_used'] = max(target['max_old_gen_used'], old_used)
                    target['avg_old_gen_committed'] += old_committed
                    
                    target['avg_eden_used'] += eden_used
                    target['max_eden_used'] = max(target['max_eden_used'], eden_used)
                    target['avg_eden_committed'] += eden_committed
                    
                    target['avg_metaspace_used'] += metaspace_used
                    target['max_metaspace_used'] = max(target['max_metaspace_used'], metaspace_used)
                    
                    # For GC counts, keep the maximum (cumulative counters)
                    target['total_young_gc_count'] = max(target['total_young_gc_count'], young_gc_count)
                    target['total_young_gc_time'] = max(target['total_young_gc_time'], young_gc_time)
                    target['total_old_gc_count'] = max(target['total_old_gc_count'], old_gc_count)
                    target['total_old_gc_time'] = max(target['total_old_gc_time'], old_gc_time)
                    
                    target['avg_thread_count'] += thread_count
                    target['max_thread_count'] = max(target['max_thread_count'], thread_count)
                    
                    target['count'] += 1
                    
                except (ValueError, IndexError):
                    continue
        
        # Calculate averages
        for process_type in ['taskmanager', 'jobmanager']:
            target = jvm_data[process_type]
            if target['count'] > 0:
                target['avg_heap_used'] = target['avg_heap_used'] // target['count']
                target['avg_heap_committed'] = target['avg_heap_committed'] // target['count']
                target['avg_old_gen_used'] = target['avg_old_gen_used'] // target['count']
                target['avg_old_gen_committed'] = target['avg_old_gen_committed'] // target['count']
                target['avg_eden_used'] = target['avg_eden_used'] // target['count']
                target['avg_eden_committed'] = target['avg_eden_committed'] // target['count']
                target['avg_metaspace_used'] = target['avg_metaspace_used'] // target['count']
                target['avg_thread_count'] = target['avg_thread_count'] // target['count']
        
        return jvm_data
        
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
            
            # Look for CPU cycles data lines - use flexible date pattern instead of hardcoded date
            if line.startswith('202') and ',' in line and ('[FLINK-' in line or '[KAFKA]' in line or '[ZOOKEEPER]' in line):
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
        
        # Read JVM metrics if available
        jvm_file = find_jvm_metrics_file(exp_path)
        if jvm_file and os.path.exists(jvm_file):
            jvm_data = read_jvm_metrics_from_file(jvm_file)
            if jvm_data:
                # Add JVM metrics to taskmanager and jobmanager
                for process_type in ['taskmanager', 'jobmanager']:
                    if process_type in jvm_data and jvm_data[process_type]['count'] > 0:
                        result[process_type].update({
                            'jvm_avg_heap_used': jvm_data[process_type]['avg_heap_used'],
                            'jvm_max_heap_used': jvm_data[process_type]['max_heap_used'],
                            'jvm_avg_heap_committed': jvm_data[process_type]['avg_heap_committed'],
                            'jvm_avg_old_gen_used': jvm_data[process_type]['avg_old_gen_used'],
                            'jvm_max_old_gen_used': jvm_data[process_type]['max_old_gen_used'],
                            'jvm_avg_old_gen_committed': jvm_data[process_type]['avg_old_gen_committed'],
                            'jvm_avg_eden_used': jvm_data[process_type]['avg_eden_used'],
                            'jvm_max_eden_used': jvm_data[process_type]['max_eden_used'],
                            'jvm_avg_eden_committed': jvm_data[process_type]['avg_eden_committed'],
                            'jvm_avg_metaspace_used': jvm_data[process_type]['avg_metaspace_used'],
                            'jvm_max_metaspace_used': jvm_data[process_type]['max_metaspace_used'],
                            'jvm_total_young_gc_count': jvm_data[process_type]['total_young_gc_count'],
                            'jvm_total_young_gc_time': jvm_data[process_type]['total_young_gc_time'],
                            'jvm_total_old_gc_count': jvm_data[process_type]['total_old_gc_count'],
                            'jvm_total_old_gc_time': jvm_data[process_type]['total_old_gc_time'],
                            'jvm_avg_thread_count': jvm_data[process_type]['avg_thread_count'],
                            'jvm_max_thread_count': jvm_data[process_type]['max_thread_count']
                        })
                    else:
                        # Add default values if no JVM data found
                        result[process_type].update({
                            'jvm_avg_heap_used': 0, 'jvm_max_heap_used': 0, 'jvm_avg_heap_committed': 0,
                            'jvm_avg_old_gen_used': 0, 'jvm_max_old_gen_used': 0, 'jvm_avg_old_gen_committed': 0,
                            'jvm_avg_eden_used': 0, 'jvm_max_eden_used': 0, 'jvm_avg_eden_committed': 0,
                            'jvm_avg_metaspace_used': 0, 'jvm_max_metaspace_used': 0,
                            'jvm_total_young_gc_count': 0, 'jvm_total_young_gc_time': 0,
                            'jvm_total_old_gc_count': 0, 'jvm_total_old_gc_time': 0,
                            'jvm_avg_thread_count': 0, 'jvm_max_thread_count': 0
                        })
        else:
            # Add default JVM values if no JVM file found
            for process_type in ['taskmanager', 'jobmanager']:
                result[process_type].update({
                    'jvm_avg_heap_used': 0, 'jvm_max_heap_used': 0, 'jvm_avg_heap_committed': 0,
                    'jvm_avg_old_gen_used': 0, 'jvm_max_old_gen_used': 0, 'jvm_avg_old_gen_committed': 0,
                    'jvm_avg_eden_used': 0, 'jvm_max_eden_used': 0, 'jvm_avg_eden_committed': 0,
                    'jvm_avg_metaspace_used': 0, 'jvm_max_metaspace_used': 0,
                    'jvm_total_young_gc_count': 0, 'jvm_total_young_gc_time': 0,
                    'jvm_total_old_gc_count': 0, 'jvm_total_old_gc_time': 0,
                    'jvm_avg_thread_count': 0, 'jvm_max_thread_count': 0
                })
        
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
            #"with_Sluice": "part8-lr-StreamSluice-100000000-1360-150-1300-10-1-50-1-333-1-50-1-300-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            #"without_Sluice": "part8-lr-NoControll-100000000-1360-150-1300-10-1-50-1-333-1-50-1-300-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
             "with_Sluice": "part8-lr-StreamSluice-100000000-390-150-1300-10-1-50-1-333-1-50-1-300-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
             "without_Sluice": "part8-lr-NoControll-100000000-390-150-1300-10-1-50-1-333-1-50-1-300-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            # "P4_Sluice": "part8-lr-StreamSluice-100000000-1360-150-1300-10-1-50-1-333-1-50-1-300-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            # "P8_Sluice": "part8-lr-StreamSluice-100000000-1360-150-1300-10-1-50-1-333-1-50-5-300-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            # "P13_Sluice": "part8-lr-StreamSluice-100000000-1360-150-1300-10-1-50-1-333-1-50-10-300-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            # "P23_Sluice": "part8-lr-StreamSluice-100000000-1360-150-1300-10-1-50-1-333-1-50-20-300-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            # "P4_No": "part8-lr-NoControll-100000000-1360-150-1300-10-1-50-1-333-1-50-1-300-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            # "P8_No": "part8-lr-NoControll-100000000-1360-150-1300-10-1-50-1-333-1-50-5-300-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            # "P13_No": "part8-lr-NoControll-100000000-1360-150-1300-10-1-50-1-333-1-50-10-300-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            # "P23_No": "part8-lr-NoControll-100000000-1360-150-1300-10-1-50-1-333-1-50-20-300-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
        },
        "Stock": {
        #    "with_Sluice": "part8-stock-StreamSluice-100000000-1360-90-1000-20-1-200-1-50-1-200-1-166-1-1-50-3000-100-0.1-false-true-1",
        #    "without_Sluice": "part8-stock-NoControll-100000000-1360-90-1000-20-1-200-1-50-1-200-1-166-1-1-50-3000-100-0.1-false-false-1",
            "with_Sluice": "part8-stock-StreamSluice-100000000-390-90-1000-20-1-200-1-50-1-200-1-166-1-1-50-3000-100-0.1-false-true-1",
            "without_Sluice": "part8-stock-NoControll-100000000-390-90-1000-20-1-200-1-50-1-200-1-166-1-1-50-3000-100-0.1-false-false-1",
        #     # "Without_Sluice": "part8-stock-NoControll-100000000-1360-90-1000-20-1-200-4-1111-1-200-1-166-1-5-1666-3000-100-0.1-false-false-1",
        #     # "With_Sluice_5ms": "part8-stock-StreamSluice-5000000-1360-90-1000-20-1-200-4-1111-1-200-1-166-1-5-1666-3000-100-0.1-false-true-1",
        #     # "With_Sluice_25ms": "part8-stock-StreamSluice-25000000-1360-90-1000-20-1-200-4-1111-1-200-1-166-1-5-1666-3000-100-0.1-false-true-1",
        #     # "With_Sluice_100ms": "part8-stock-StreamSluice-100000000-1360-90-1000-20-1-200-4-1111-1-200-1-166-1-5-1666-3000-100-0.1-false-true-1",
        #     "P7_Sluice": "part8-stock-StreamSluice-100000000-360-90-1000-20-1-200-1-200-1-200-1-166-1-1-200-3000-100-0.1-false-true-1",
        #     "P11_Sluice": "part8-stock-StreamSluice-100000000-360-90-1000-20-1-200-1-200-1-200-1-166-1-5-200-3000-100-0.1-false-true-1",
        #     "P16_Sluice": "part8-stock-StreamSluice-100000000-360-90-1000-20-1-200-1-200-1-200-1-166-1-10-200-3000-100-0.1-false-true-1",
        #     "P26_Sluice": "part8-stock-StreamSluice-100000000-360-90-1000-20-1-200-1-200-1-200-1-166-1-20-200-3000-100-0.1-false-true-1",
        #     "P7_No": "part8-stock-NoControll-100000000-360-90-1000-20-1-200-1-200-1-200-1-166-1-1-200-3000-100-0.1-false-false-1",
        #     "P11_No": "part8-stock-NoControll-100000000-360-90-1000-20-1-200-1-200-1-200-1-166-1-5-200-3000-100-0.1-false-false-1",
        #     "P16_No": "part8-stock-NoControll-100000000-360-90-1000-20-1-200-1-200-1-200-1-166-1-10-200-3000-100-0.1-false-false-1",
        #     "P7_Sluice": "part8-stock-StreamSluice-100000000-1360-90-1000-20-1-200-1-200-1-200-1-166-1-1-200-3000-100-0.1-false-true-1",
        #     "P11_Sluice": "part8-stock-StreamSluice-100000000-1360-90-1000-20-1-200-1-200-1-200-1-166-1-5-200-3000-100-0.1-false-true-1",
        #     "P16_Sluice": "part8-stock-StreamSluice-100000000-1360-90-1000-20-1-200-1-200-1-200-1-166-1-10-200-3000-100-0.1-false-true-1",
        #     "P26_Sluice": "part8-stock-StreamSluice-100000000-1360-90-1000-20-1-200-1-200-1-200-1-166-1-20-200-3000-100-0.1-false-true-1",
        #     "P7_No": "part8-stock-NoControll-100000000-1360-90-1000-20-1-200-1-200-1-200-1-166-1-1-200-3000-100-0.1-false-false-1",
        #     "P11_No": "part8-stock-NoControll-100000000-1360-90-1000-20-1-200-1-200-1-200-1-166-1-5-200-3000-100-0.1-false-false-1",
        #     "P16_No": "part8-stock-NoControll-100000000-1360-90-1000-20-1-200-1-200-1-200-1-166-1-10-200-3000-100-0.1-false-false-1",
        #     "P26_No": "part8-stock-NoControll-100000000-1360-90-1000-20-1-200-1-200-1-200-1-166-1-20-200-3000-100-0.1-false-false-1",
        },
        "Twitter": {
            #"with_Sluice": "part8-twitter-StreamSluice-100000000-1360-90-3400-1-1-100-1-50-1-50-1-50-2000-0.1-100--1250-0.0-false-1000-0.8-1",
            #"without_Sluice": "part8-twitter-NoControll-100000000-1360-90-3400-1-1-100-1-50-1-50-1-50-2000-0.1-100--1250-0.0-false-1000-0.8-1",
            "With_Sluice": "part8-twitter-StreamSluice-100000000-390-90-3400-1-1-100-1-50-1-50-1-50-2000-0.1-100--1250-0.0-false-1000-0.8-1",
            "Without_Sluice": "part8-twitter-NoControll-100000000-390-90-3400-1-1-100-1-50-1-50-1-50-2000-0.1-100--1250-0.0-false-1000-0.8-1",
            # "Without_Sluice": "part8-twitter-NoControll-100000000-1360-90-3400-1-7-1111-3-166-1-50-1-50-2000-0.1-100--1250-0.0-false-1000-0.8-1",
            # "With_Sluice_5ms": "part8-twitter-StreamSluice-5000000-1360-90-3400-1-7-1111-3-166-1-50-1-50-2000-0.1-100--1250-0.0-false-1000-0.8-1",
            # "With_Sluice_25ms": "part8-twitter-StreamSluice-25000000-1360-90-3400-1-7-1111-3-166-1-50-1-50-2000-0.1-100--1250-0.0-false-1000-0.8-1",
            # "With_Sluice_100ms": "part8-twitter-StreamSluice-100000000-1360-90-3400-1-7-1111-3-166-1-50-1-50-2000-0.1-100--1250-0.0-false-1000-0.8-1",
        },
        "ML-Scoring": {

            "With_Sluice": "part8-ml-StreamSluice-100000000-390-150-1300-10-1-50-1-50-1-50-1-50-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            "Without_Sluice": "part8-ml-NoControll-100000000-390-150-1300-10-1-50-1-50-1-50-1-50-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            # "Without_Sluice": "part8-ml-NoControll-100000000-1360-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            # "With_Sluice_5ms": "part8-ml-StreamSluice-5000000-1360-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            # "With_Sluice_25ms": "part8-ml-StreamSluice-25000000-1360-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
            # "With_Sluice_100ms": "part8-ml-StreamSluice-100000000-1360-150-1300-10-1-50-1-333-1-50-9-1111-3000-0.1-100-1-25-0.0-false-1000-0.8-1",
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
                    'LLC_Misses_per_min': group_data['avg_llc_misses'],
                    # JVM metrics (only for TaskManager and JobManager)
                    'JVM_Avg_Heap_Used_Bytes': group_data.get('jvm_avg_heap_used', 0),
                    'JVM_Max_Heap_Used_Bytes': group_data.get('jvm_max_heap_used', 0),
                    'JVM_Avg_Heap_Committed_Bytes': group_data.get('jvm_avg_heap_committed', 0),
                    'JVM_Avg_OldGen_Used_Bytes': group_data.get('jvm_avg_old_gen_used', 0),
                    'JVM_Max_OldGen_Used_Bytes': group_data.get('jvm_max_old_gen_used', 0),
                    'JVM_Avg_Eden_Used_Bytes': group_data.get('jvm_avg_eden_used', 0),
                    'JVM_Max_Eden_Used_Bytes': group_data.get('jvm_max_eden_used', 0),
                    'JVM_Avg_Metaspace_Used_Bytes': group_data.get('jvm_avg_metaspace_used', 0),
                    'JVM_Max_Metaspace_Used_Bytes': group_data.get('jvm_max_metaspace_used', 0),
                    'JVM_Total_YoungGC_Count': group_data.get('jvm_total_young_gc_count', 0),
                    'JVM_Total_YoungGC_Time_ms': group_data.get('jvm_total_young_gc_time', 0),
                    'JVM_Total_OldGC_Count': group_data.get('jvm_total_old_gc_count', 0),
                    'JVM_Total_OldGC_Time_ms': group_data.get('jvm_total_old_gc_time', 0),
                    'JVM_Avg_Thread_Count': group_data.get('jvm_avg_thread_count', 0),
                    'JVM_Max_Thread_Count': group_data.get('jvm_max_thread_count', 0)
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
                  'MinorFaults_per_min', 'MajorFaults_per_min', 'LLC_Misses_per_min',
                  'JVM_Avg_Heap_Used_Bytes', 'JVM_Max_Heap_Used_Bytes', 'JVM_Avg_Heap_Committed_Bytes',
                  'JVM_Avg_OldGen_Used_Bytes', 'JVM_Max_OldGen_Used_Bytes', 'JVM_Avg_Eden_Used_Bytes',
                  'JVM_Max_Eden_Used_Bytes', 'JVM_Avg_Metaspace_Used_Bytes', 'JVM_Max_Metaspace_Used_Bytes',
                  'JVM_Total_YoungGC_Count', 'JVM_Total_YoungGC_Time_ms', 'JVM_Total_OldGC_Count',
                  'JVM_Total_OldGC_Time_ms', 'JVM_Avg_Thread_Count', 'JVM_Max_Thread_Count']
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\nResults saved to: {output_file}")
    
    # Display summary table
    print(f"\nPart 8 Overhead Analysis Results with JVM Metrics:")
    print("=" * 220)
    
    # Print header
    header = f"{'Workload':<10} {'Config':<18} {'Process':<15} {'Cycles':<15} {'Instructions':<15} {'RSS_KB':<12} {'ReadB/min':<12} {'WriteB/min':<12} {'JVM_HeapUsed_MB':<16} {'JVM_YoungGC':<12} {'JVM_OldGC':<10} {'JVM_Threads':<12}"
    print(header)
    print("-" * 220)
    
    # Print data rows
    for result in results:
        heap_used_mb = result['JVM_Avg_Heap_Used_Bytes'] // (1024 * 1024) if result['JVM_Avg_Heap_Used_Bytes'] > 0 else 0
        young_gc_info = f"{result['JVM_Total_YoungGC_Count']}/{result['JVM_Total_YoungGC_Time_ms']}ms"
        old_gc_info = f"{result['JVM_Total_OldGC_Count']}/{result['JVM_Total_OldGC_Time_ms']}ms"
        thread_info = f"{result['JVM_Avg_Thread_Count']}/{result['JVM_Max_Thread_Count']}"
        
        row = f"{result['Workload']:<10} {result['Configuration']:<18} {result['Process_Group']:<15} {result['Total_Cycles']:>14,} {result['Instructions']:>14,} {result['avg_RSS_KB']:>11,} {result['ReadBytes_per_min']:>11,} {result['WriteBytes_per_min']:>11,} {heap_used_mb:>15,} {young_gc_info:>11} {old_gc_info:>9} {thread_info:>11}"
        print(row)

if __name__ == "__main__":
    main()
