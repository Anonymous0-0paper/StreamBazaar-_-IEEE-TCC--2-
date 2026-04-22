#!/usr/bin/env python3
import requests, csv, time, sys, os
from datetime import datetime
from threading import Thread

PROMETHEUS = "http://localhost:19090"
OUTPUT = f"metrics_live_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

METRICS = {
    'p50_latency_ms': 'streambazaar_latency_p50_ms',
    'p99_latency_ms': 'streambazaar_latency_p99_ms',
    'p999_latency_ms': 'streambazaar_latency_p999_ms',
    'rue_percent': 'streambazaar_resource_utilization_efficiency{scope="cluster"}',
    'cpu_percent': 'streambazaar_checkpoint_cpu_utilization_percent{scope="cluster"}',
    'memory_percent': 'streambazaar_checkpoint_memory_utilization_percent{scope="cluster"}',
    'network_percent': 'streambazaar_checkpoint_network_utilization_percent{scope="cluster"}',
    'tlvr': 'streambazaar_tail_latency_violation_rate',
    'throughput': 'streambazaar_system_throughput_msgs_per_sec',
    'backlog': 'sum(streambazaar_tenant_backlog)',
    'eei': 'streambazaar_economic_efficiency_index',
    'fpp': 'streambazaar_fairness_performance_product',
    'mis': 'streambazaar_migration_impact_score',
}

def get_metric(query):
    try:
        r = requests.get(f"{PROMETHEUS}/api/v1/query", params={'query': query}, timeout=3)
        if r.status_code == 200 and r.json()['data']['result']:
            return float(r.json()['data']['result'][0]['value'][1])
        return None
    except: return None

with open(OUTPUT, 'w') as f:
    w = csv.writer(f)
    w.writerow(['timestamp'] + list(METRICS.keys()))
    
    print(f"✓ Collecting to: {OUTPUT}")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            ts = datetime.now().isoformat()
            row = [ts]
            for name, query in METRICS.items():
                val = get_metric(query)
                row.append(val if val is not None else '')
            
            with open(OUTPUT, 'a') as f:
                csv.writer(f).writerow(row)
            
            p99 = row[3] if len(row) > 3 else 'N/A'
            p99_str = f"{float(p99):.1f}" if isinstance(p99, (int, float)) else 'N/A'
            print(f"[{ts}] P99: {p99_str}ms", flush=True)
            
            time.sleep(30)
    except KeyboardInterrupt:
        print("\n\nStopped. File:", OUTPUT)
        print(f"Rows saved:", sum(1 for _ in open(OUTPUT)) - 1)
