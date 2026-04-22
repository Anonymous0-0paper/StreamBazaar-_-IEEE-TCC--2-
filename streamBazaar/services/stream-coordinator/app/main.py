import json
import math
import os
import resource
import threading
import time
from collections import defaultdict
from collections import deque
from typing import Dict, List

import redis as redis_lib
import requests
from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
from starlette.responses import Response

# Redis shared state — virtual balances must be consistent across all coordinator
# nodes so the auction is fair even when tenants are sharded across machines.
_REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
_redis_client: redis_lib.Redis | None = None
BALANCE_KEY = "streambazaar:virtual_balance"


def _get_redis() -> redis_lib.Redis | None:
    global _redis_client
    if _redis_client is None:
        try:
            _redis_client = redis_lib.from_url(_REDIS_URL, decode_responses=True, socket_connect_timeout=1)
            _redis_client.ping()
        except Exception:
            _redis_client = None
    return _redis_client


def _redis_get_balance(tenant_id: str, default: float) -> float:
    r = _get_redis()
    if r:
        try:
            v = r.hget(BALANCE_KEY, tenant_id)
            return float(v) if v is not None else default
        except Exception:
            pass
    return default


def _redis_set_balance(tenant_id: str, balance: float) -> None:
    r = _get_redis()
    if r:
        try:
            r.hset(BALANCE_KEY, tenant_id, str(balance))
            return
        except Exception:
            pass


def _redis_incr_balance(tenant_id: str, delta: float, floor: float = 100.0) -> float:
    """Atomically adjust balance by delta (can be negative for deductions)."""
    r = _get_redis()
    if r:
        try:
            pipe = r.pipeline()
            pipe.hincrbyfloat(BALANCE_KEY, tenant_id, delta)
            results = pipe.execute()
            new_val = float(results[0])
            if new_val < floor:
                r.hset(BALANCE_KEY, tenant_id, str(floor))
                return floor
            return new_val
        except Exception:
            pass
    return floor

app = FastAPI(title="StreamBazaar Stream Coordinator", version="0.1.0")

CONSUMED_EVENTS = Counter("streambazaar_stream_events_consumed_total", "Events consumed by stream coordinator", ["topic"])
PUBLISHED_EVENTS = Counter("streambazaar_stream_events_published_total", "Events published by stream coordinator", ["topic"])
CLEARING_CYCLES = Counter("streambazaar_clearing_cycles_total", "Auction clearing cycles completed")
CONTROL_LOOP_ERRORS = Counter("streambazaar_stream_loop_errors_total", "Errors in stream coordinator", ["operation"])
TENANT_BACKLOG = Gauge("streambazaar_tenant_backlog", "Estimated tenant backlog", ["tenant_id"])
TENANT_P99 = Gauge("streambazaar_tenant_p99_latency_ms", "Estimated tenant p99 latency", ["tenant_id"])
TENANT_LAST_BID = Gauge("streambazaar_tenant_last_bid", "Latest bid price per tenant", ["tenant_id"])
MESSAGE_BYTES_IN = Counter(
    "streambazaar_message_bytes_in_total",
    "Total input message bytes seen by stream-coordinator",
    ["topic", "tenant_id", "dataset"],
)
MESSAGE_BYTES_OUT = Counter(
    "streambazaar_message_bytes_out_total",
    "Total output message bytes sent by stream-coordinator",
    ["topic", "tenant_id", "kind"],
)
MESSAGE_IN = Counter(
    "streambazaar_messages_in_total",
    "Total input messages seen by stream-coordinator",
    ["topic", "tenant_id", "dataset"],
)
MESSAGE_OUT = Counter(
    "streambazaar_messages_out_total",
    "Total output messages sent by stream-coordinator",
    ["topic", "tenant_id", "kind"],
)
MESSAGE_LAST_BYTES = Gauge(
    "streambazaar_message_last_bytes",
    "Last message size in bytes",
    ["topic", "tenant_id", "direction"],
)
TENANT_THROUGHPUT = Gauge(
    "streambazaar_throughput_msgs_per_sec",
    "Estimated per-tenant throughput in messages/sec",
    ["tenant_id", "direction"],
)
SYSTEM_THROUGHPUT = Gauge(
    "streambazaar_system_throughput_msgs_per_sec",
    "Estimated cluster-wide throughput in messages/sec",
)
LATENCY_P50 = Gauge("streambazaar_latency_p50_ms", "Estimated p50 latency in ms", ["tenant_id"])
LATENCY_P90 = Gauge("streambazaar_latency_p90_ms", "Estimated p90 latency in ms", ["tenant_id"])
LATENCY_P95 = Gauge("streambazaar_latency_p95_ms", "Estimated p95 latency in ms", ["tenant_id"])
LATENCY_P99 = Gauge("streambazaar_latency_p99_ms", "Estimated p99 latency in ms", ["tenant_id"])
LATENCY_P999 = Gauge("streambazaar_latency_p999_ms", "Estimated p99.9 latency in ms", ["tenant_id"])
MIGRATION_DOWNTIME_SECONDS = Gauge(
    "streambazaar_migration_downtime_seconds",
    "Estimated migration downtime in seconds",
    ["tenant_id"],
)
MIGRATION_TRANSFER_TIME_SECONDS = Gauge(
    "streambazaar_migration_transfer_time_seconds",
    "Estimated migration transfer time in seconds",
    ["tenant_id"],
)
MIGRATION_DOWNTIME_TOTAL = Counter(
    "streambazaar_migration_downtime_accumulated_seconds_total",
    "Accumulated migration downtime in seconds",
    ["tenant_id"],
)
MIGRATION_TRANSFER_TIME_TOTAL = Counter(
    "streambazaar_migration_transfer_time_accumulated_seconds_total",
    "Accumulated migration transfer time in seconds",
    ["tenant_id"],
)
CHECKPOINT_CPU_UTIL = Gauge(
    "streambazaar_checkpoint_cpu_utilization_percent",
    "Checkpoint window CPU utilization percent",
    ["scope", "tenant_id"],
)
CHECKPOINT_MEM_UTIL = Gauge(
    "streambazaar_checkpoint_memory_utilization_percent",
    "Checkpoint window memory utilization percent",
    ["scope", "tenant_id"],
)
CHECKPOINT_NET_UTIL = Gauge(
    "streambazaar_checkpoint_network_utilization_percent",
    "Checkpoint window network utilization percent",
    ["scope", "tenant_id"],
)

RESOURCE_UTILIZATION_EFFICIENCY = Gauge(
    "streambazaar_resource_utilization_efficiency",
    "RUE: arithmetic average of cpu, memory and network utilization",
    ["scope", "tenant_id"],
)
TAIL_LATENCY_VIOLATION_RATE = Gauge(
    "streambazaar_tail_latency_violation_rate",
    "TLVR: ratio of windows where p99.9 exceeds SLA",
    ["scope", "tenant_id"],
)
ECONOMIC_EFFICIENCY_INDEX = Gauge(
    "streambazaar_economic_efficiency_index",
    "EEI: achieved social welfare over greedy optimal welfare",
)
FAIRNESS_PERFORMANCE_PRODUCT = Gauge(
    "streambazaar_fairness_performance_product",
    "FPP: weighted Jain fairness multiplied by normalized throughput",
)
MIGRATION_IMPACT_SCORE = Gauge(
    "streambazaar_migration_impact_score",
    "MIS: average normalized latency degradation accumulated during migrations",
)

DEFAULT_TENANT_PRIORITY = {
    "tenant-fraud": 1.3,
    "tenant-clickstream": 1.0,
    "tenant-ml": 1.5,
    "tenant-iot": 1.1,
}


def _parse_csv(raw: str) -> List[str]:
    return [x.strip() for x in raw.split(",") if x.strip()]


def _parse_priority_map(raw: str) -> Dict[str, float]:
    if not raw:
        return {}
    result: Dict[str, float] = {}
    for item in _parse_csv(raw):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        try:
            result[key.strip()] = float(value.strip())
        except ValueError:
            continue
    return result


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    sorted_vals = sorted(values)
    pos = (q / 100.0) * (len(sorted_vals) - 1)
    low = int(pos)
    high = min(low + 1, len(sorted_vals) - 1)
    weight = pos - low
    return float(sorted_vals[low] * (1.0 - weight) + sorted_vals[high] * weight)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class StreamCoordinator:
    def __init__(self) -> None:
        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.kafka_bootstrap = kafka_bootstrap
        self.tenants = _parse_csv(os.getenv("TENANT_IDS", "tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot"))
        if not self.tenants:
            self.tenants = ["tenant-fraud", "tenant-clickstream", "tenant-ml", "tenant-iot"]

        priority_override = _parse_priority_map(os.getenv("TENANT_PRIORITIES", ""))
        self.tenant_priority = {
            tenant: float(priority_override.get(tenant, DEFAULT_TENANT_PRIORITY.get(tenant, 1.0)))
            for tenant in self.tenants
        }

        input_topic_template = os.getenv("INPUT_TOPIC_TEMPLATE", "tenant.{tenant_id}.input")
        self.output_topic_template = os.getenv("OUTPUT_TOPIC_TEMPLATE", "tenant.{tenant_id}.output")
        self.alloc_topic = os.getenv("ALLOC_TOPIC", "streamBazaar.allocations")
        self.preempt_topic = os.getenv("PREEMPT_TOPIC", "streamBazaar.preemptions")
        self.metrics_topic = os.getenv("METRICS_TOPIC", "streamBazaar.metrics")

        self.consumer_topics = [input_topic_template.format(tenant_id=t) for t in self.tenants]
        self.consumer = None
        self.producer = None
        self.http = requests.Session()
        self.pricing_url = os.getenv("PRICING_URL", "http://pricing-engine:8081/price")
        self.bid_url = os.getenv("BID_URL", "http://auction-orchestrator:8080/bid")
        self.clear_url = os.getenv("CLEAR_URL", "http://auction-orchestrator:8080/auction/clear")
        self.allocate_url = os.getenv("ALLOCATE_URL", "http://resource-allocator:8083/allocate")
        self.migrate_url = os.getenv("MIGRATE_URL", "http://migration-coordinator:8084/migrate")

        self.cluster_slots = int(os.getenv("CLUSTER_SLOTS", "30"))
        self.clear_interval_sec = float(os.getenv("CLEAR_INTERVAL_SEC", "1.0"))
        self.sla_target_ms = float(os.getenv("DEFAULT_SLA_TARGET_MS", "200.0"))
        self.high_priority_threshold = float(os.getenv("HIGH_PRIORITY_THRESHOLD", "1.2"))
        self.network_capacity_mbps = float(os.getenv("NETWORK_CAPACITY_MBPS", "50.0"))
        # State size used for migration cost modelling; can be overridden by benchmark
        self.state_size_kb = float(os.getenv("STATE_SIZE_KB", "256.0"))
        self.throughput_peak = float(os.getenv("THROUGHPUT_PEAK_MSG_PER_SEC", "1000.0"))

        self.scheduler_mode = os.getenv("SCHEDULER_MODE", "streambazaar").strip().lower()
        self.fixed_parallelism_per_tenant = int(os.getenv("FIXED_PARALLELISM_PER_TENANT", "2"))
        self.talos_cooldown_sec = int(os.getenv("TALOS_COOLDOWN_SEC", "90"))
        self.talos_idle_threshold = float(os.getenv("TALOS_IDLE_THRESHOLD", "500"))
        self.ds2_max_scaling_steps = int(os.getenv("DS2_MAX_SCALING_STEPS", "3"))
        self.ds2_stability_sec = int(os.getenv("DS2_STABILITY_SEC", "120"))
        self.capsys_rebalance_sec = float(os.getenv("CAPSYS_REBALANCE_SEC", "30.0"))
        self.capsys_contention_threshold = float(os.getenv("CAPSYS_CONTENTION_THRESHOLD", "0.75"))

        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

        self.last_clear_ts = time.time()
        self.state: Dict[str, Dict[str, float]] = {
            tenant: {
                "backlog": 0.0,
                "sla_pressure": 0.2,
                "current_p99_ms": 120.0,
                "current_p999_ms": 150.0,
                "last_bid": 0.0,
            }
            for tenant in self.tenants
        }

        # Virtual currency system (paper Eq. 2):
        # ω^(t+Δ) = ω^t·(1−γ) + A_i·(α_i + η·U_avg/U_total)
        # γ=0.05 decay per auction interval; A_i=5000 base injection per interval
        self.currency_decay_rate: float = float(os.getenv("CURRENCY_DECAY_RATE", "0.05"))
        self.currency_base_alloc: float = float(os.getenv("CURRENCY_BASE_ALLOC", "5000.0"))
        self.currency_eta: float = float(os.getenv("CURRENCY_ETA", "0.3"))
        self.currency_initial: float = float(os.getenv("CURRENCY_INITIAL", "100000.0"))
        # Per-tenant virtual balances ω_i
        self.virtual_balance: Dict[str, float] = {tenant: self.currency_initial for tenant in self.tenants}
        # Track cumulative utilization for currency injection term U_avg/U_total
        self.utilization_sum: Dict[str, float] = {tenant: 0.0 for tenant in self.tenants}
        self.utilization_cycles: int = 0
        # Aggregate demand D^t(r) accumulated per cycle (Eq. 7: sum of bid × indicator)
        self.aggregate_demand: float = 0.0

        self.window_counts = defaultdict(int)
        self.violation_counts = defaultdict(int)
        self.migration_impact_sum = 0.0
        self.migration_count = 0
        self.last_cycle_metrics_ts = time.time()
        self.prev_backlog = {tenant: 0.0 for tenant in self.tenants}
        self.operator_parallelism = {
            tenant: max(1, min(self.cluster_slots, self.fixed_parallelism_per_tenant)) for tenant in self.tenants
        }
        self.last_scale_ts = {tenant: 0.0 for tenant in self.tenants}
        self.prev_msg_in = {tenant: 0.0 for tenant in self.tenants}
        self.prev_msg_out = {tenant: 0.0 for tenant in self.tenants}
        self.prev_bytes_in = {tenant: 0.0 for tenant in self.tenants}
        self.prev_bytes_out = {tenant: 0.0 for tenant in self.tenants}
        self.msg_in = defaultdict(float)
        self.msg_out = defaultdict(float)
        self.bytes_in = defaultdict(float)
        self.bytes_out = defaultdict(float)
        self.latency_samples_ms = {tenant: deque(maxlen=5000) for tenant in self.tenants}
        self.last_allocation_publish_ts = {tenant: 0.0 for tenant in self.tenants}
        self.last_checkpoint_wall = time.time()
        self.last_checkpoint_cpu = time.process_time()
        self.total_memory_bytes = self._read_total_memory_bytes()
        self.stats = {
            "consumed": 0,
            "published_allocations": 0,
            "published_preemptions": 0,
            "published_metrics": 0,
            "last_loop_error": "",
            "running": False,
        }

    def _read_total_memory_bytes(self) -> float:
        try:
            with open("/proc/meminfo", "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        parts = line.split()
                        return float(parts[1]) * 1024.0
        except Exception:
            pass
        return 1.0

    def _read_rss_bytes(self) -> float:
        try:
            with open("/proc/self/statm", "r", encoding="utf-8") as f:
                parts = f.read().strip().split()
                if len(parts) >= 2:
                    resident_pages = int(parts[1])
                    page_size = int(os.sysconf("SC_PAGE_SIZE"))
                    return float(resident_pages * page_size)
        except Exception:
            pass
        # Fallback to ru_maxrss if statm is unavailable.
        return float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024.0)

    def _set_latency_percentile_gauges(self, tenant_id: str) -> None:
        sample_vals = list(self.latency_samples_ms.get(tenant_id, []))
        if sample_vals:
            p50 = _percentile(sample_vals, 50.0)
            p90 = _percentile(sample_vals, 90.0)
            p95 = _percentile(sample_vals, 95.0)
            p99 = _percentile(sample_vals, 99.0)
            p999 = _percentile(sample_vals, 99.9)
            self.state[tenant_id]["current_p99_ms"] = p99
            self.state[tenant_id]["current_p999_ms"] = p999
        else:
            p99 = self.state[tenant_id]["current_p99_ms"]
            p999 = self.state[tenant_id]["current_p999_ms"]
            p95 = max(1.0, p99 * 0.95)
            p90 = max(1.0, p99 * 0.90)
            p50 = max(1.0, p99 * 0.50)
        LATENCY_P50.labels(tenant_id=tenant_id).set(p50)
        LATENCY_P90.labels(tenant_id=tenant_id).set(p90)
        LATENCY_P95.labels(tenant_id=tenant_id).set(p95)
        LATENCY_P99.labels(tenant_id=tenant_id).set(p99)
        LATENCY_P999.labels(tenant_id=tenant_id).set(p999)

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.stats["running"] = True

    def stop(self) -> None:
        self.stop_event.set()
        self.stats["running"] = False
        try:
            if self.consumer is not None:
                self.consumer.close()
        except Exception:
            pass
        try:
            if self.producer is not None:
                self.producer.flush(timeout=5)
                self.producer.close(timeout=5)
        except Exception:
            pass

    def _ensure_kafka_clients(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.consumer = KafkaConsumer(
                    *self.consumer_topics,
                    bootstrap_servers=self.kafka_bootstrap,
                    group_id=os.getenv("COORDINATOR_GROUP_ID", "stream-coordinator"),
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    consumer_timeout_ms=1000,
                )
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda v: v.encode("utf-8"),
                    acks="all",
                    retries=3,
                )
                return
            except NoBrokersAvailable:
                self.stats["last_loop_error"] = "kafka not ready"
                CONTROL_LOOP_ERRORS.labels(operation="kafka_connect").inc()
                time.sleep(1.0)

    def _post(self, url: str, payload: Dict[str, object]) -> Dict[str, object]:
        response = self.http.post(url, json=payload, timeout=3)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            return data
        return {}

    def _publish(self, topic: str, key: str, payload: Dict[str, object], kind: str = "event") -> None:
        if self.producer is None:
            return
        payload_bytes = len(json.dumps(payload).encode("utf-8"))
        self.producer.send(topic, key=key, value=payload)
        PUBLISHED_EVENTS.labels(topic=topic).inc()
        MESSAGE_OUT.labels(topic=topic, tenant_id=key, kind=kind).inc()
        MESSAGE_BYTES_OUT.labels(topic=topic, tenant_id=key, kind=kind).inc(payload_bytes)
        MESSAGE_LAST_BYTES.labels(topic=topic, tenant_id=key, direction="out").set(payload_bytes)
        self.msg_out[key] += 1.0
        self.bytes_out[key] += float(payload_bytes)

    def _update_virtual_currency(self, granted_map: Dict[str, float]) -> None:
        """Apply virtual currency decay and periodic injection (paper Eq. 2).

        ω_i^(t+Δ) = ω_i^t · (1 − γ) + A_i · (α_i + η · U_i_avg / U_total)

        γ  = decay rate (0.05) — prevents hoarding
        A_i = base allocation per interval
        α_i = tenant priority weight
        η   = utilization-reward coefficient (0.3)
        U_i_avg / U_total = tenant's share of total utilization

        Balances are stored in Redis so all coordinator nodes (shards) share a
        consistent view — critical for fair auction participation across nodes.
        """
        self.utilization_cycles += 1
        total_granted = sum(granted_map.get(t, 0.0) for t in self.tenants)
        u_total = max(total_granted, 1.0)

        for tenant in self.tenants:
            u_i = granted_map.get(tenant, 0.0)
            self.utilization_sum[tenant] += u_i
            u_avg = self.utilization_sum[tenant] / max(float(self.utilization_cycles), 1.0)
            alpha_i = self.tenant_priority.get(tenant, 1.0)
            injection = self.currency_base_alloc * (alpha_i + self.currency_eta * (u_avg / u_total))

            # Read current balance from Redis (shared across nodes), apply decay + injection
            current = _redis_get_balance(tenant, self.virtual_balance.get(tenant, self.currency_initial))
            new_balance = max(100.0, current * (1.0 - self.currency_decay_rate) + injection)
            _redis_set_balance(tenant, new_balance)
            self.virtual_balance[tenant] = new_balance  # keep local cache in sync

    def _deduct_currency(self, tenant_id: str, payment: float) -> None:
        """Atomically deduct second-price payment from virtual balance (budget constraint Eq. line 373)."""
        new_balance = _redis_incr_balance(tenant_id, -payment, floor=100.0)
        self.virtual_balance[tenant_id] = new_balance

    def _estimate_sla_pressure(self, tenant_id: str, event: Dict[str, object]) -> float:
        backlog = self.state[tenant_id]["backlog"]
        base = min(1.5, 0.15 + backlog / 40.0)
        if tenant_id == "tenant-fraud" and bool(event.get("is_fraud", False)):
            base += 0.2
        return max(0.05, min(1.5, base))

    def _update_advanced_metrics(self, allocations: List[Dict[str, object]], cycle_duration_sec: float) -> None:
        # TLVR is tracked over high-priority tenants and aggregated cluster-wide.
        high_priority_tenants = [t for t in self.tenants if self.tenant_priority.get(t, 1.0) >= self.high_priority_threshold]
        for tenant in high_priority_tenants:
            self.window_counts[tenant] += 1
            if self.state[tenant]["current_p999_ms"] > self.sla_target_ms:
                self.violation_counts[tenant] += 1
            total = self.window_counts[tenant]
            rate = float(self.violation_counts[tenant]) / float(total) if total else 0.0
            TAIL_LATENCY_VIOLATION_RATE.labels(scope="tenant", tenant_id=tenant).set(rate)

        total_windows = sum(self.window_counts[t] for t in high_priority_tenants)
        total_violations = sum(self.violation_counts[t] for t in high_priority_tenants)
        cluster_tlvr = (float(total_violations) / float(total_windows)) if total_windows else 0.0
        TAIL_LATENCY_VIOLATION_RATE.labels(scope="cluster", tenant_id="all").set(cluster_tlvr)

        requested_map: Dict[str, float] = {}
        granted_map: Dict[str, float] = {}
        for row in allocations:
            tenant = str(row.get("tenant_id", ""))
            requested_map[tenant] = float(row.get("requested_slots", 0.0))
            granted_map[tenant] = float(row.get("granted_slots", 0.0))

        # EEI: achieved welfare over a simple greedy optimal welfare bound.
        bids = {tenant: float(self.state[tenant]["last_bid"]) for tenant in self.tenants}
        achieved_welfare = 0.0
        for tenant in self.tenants:
            achieved_welfare += bids[tenant] * granted_map.get(tenant, 0.0)

        remaining = float(self.cluster_slots)
        optimal_welfare = 0.0
        for tenant in sorted(self.tenants, key=lambda t: bids[t], reverse=True):
            req = requested_map.get(tenant, 0.0)
            take = min(req, remaining)
            optimal_welfare += bids[tenant] * take
            remaining -= take
            if remaining <= 0:
                break
        eei = achieved_welfare / optimal_welfare if optimal_welfare > 0 else 0.0
        ECONOMIC_EFFICIENCY_INDEX.set(eei)

        # FPP = weighted Jain fairness * normalized throughput.
        # Throughput is measured as completed output rate to avoid double-counting
        # the same message on ingress and egress.
        weighted_utils = []
        total_throughput = 0.0
        total_in_rate = 0.0
        total_out_rate = 0.0
        for tenant in self.tenants:
            in_delta = self.msg_in[tenant] - self.prev_msg_in[tenant]
            out_delta = self.msg_out[tenant] - self.prev_msg_out[tenant]
            throughput = out_delta / max(cycle_duration_sec, 1e-6)
            total_throughput += throughput
            in_rate = in_delta / max(cycle_duration_sec, 1e-6)
            out_rate = out_delta / max(cycle_duration_sec, 1e-6)
            total_in_rate += in_rate
            total_out_rate += out_rate
            TENANT_THROUGHPUT.labels(tenant_id=tenant, direction="in").set(in_rate)
            TENANT_THROUGHPUT.labels(tenant_id=tenant, direction="out").set(out_rate)
            TENANT_THROUGHPUT.labels(tenant_id=tenant, direction="total").set(throughput)
            weighted_utils.append(self.tenant_priority.get(tenant, 1.0) * granted_map.get(tenant, 0.0))
        SYSTEM_THROUGHPUT.set(total_throughput)

        numerator = sum(weighted_utils)
        jain_den = sum(v * v for v in weighted_utils)
        n = len(weighted_utils)
        jain = (numerator * numerator) / (n * jain_den) if n > 0 and jain_den > 0 else 0.0
        norm_tp = min(1.0, total_throughput / max(self.throughput_peak, 1e-6))
        FAIRNESS_PERFORMANCE_PRODUCT.set(jain * norm_tp)

        # RUE from cpu/memory/network utilization proxies.
        # Realistic checkpoint metrics based on process CPU/memory and actual message bytes.
        now_wall = time.time()
        now_cpu = time.process_time()
        wall_delta = max(1e-6, now_wall - self.last_checkpoint_wall)
        cpu_delta = max(0.0, now_cpu - self.last_checkpoint_cpu)
        process_cpu_util = min(100.0, max(0.0, (cpu_delta / wall_delta) * 100.0))
        self.last_checkpoint_wall = now_wall
        self.last_checkpoint_cpu = now_cpu

        rss_bytes = self._read_rss_bytes()
        process_mem_util = min(100.0, max(0.0, (rss_bytes / max(self.total_memory_bytes, 1.0)) * 100.0))

        total_bytes_delta = 0.0
        for tenant in self.tenants:
            total_bytes_delta += (self.bytes_in[tenant] - self.prev_bytes_in[tenant]) + (self.bytes_out[tenant] - self.prev_bytes_out[tenant])
        total_mbps = (total_bytes_delta * 8.0) / max(cycle_duration_sec, 1e-6) / 1_000_000.0
        process_net_util = min(100.0, max(0.0, (total_mbps / max(self.network_capacity_mbps, 1e-6)) * 100.0))

        tenant_rues: List[float] = []
        cpu_vals: List[float] = []
        mem_vals: List[float] = []
        net_vals: List[float] = []
        total_msgs_delta = 0.0
        tenant_msgs_delta: Dict[str, float] = {}
        for tenant in self.tenants:
            in_delta = self.msg_in[tenant] - self.prev_msg_in[tenant]
            out_delta = self.msg_out[tenant] - self.prev_msg_out[tenant]
            msg_delta = max(0.0, in_delta + out_delta)
            tenant_msgs_delta[tenant] = msg_delta
            total_msgs_delta += msg_delta

        for tenant in self.tenants:
            share = tenant_msgs_delta[tenant] / max(total_msgs_delta, 1e-6) if total_msgs_delta > 0 else (1.0 / float(len(self.tenants)))
            cpu_util = process_cpu_util * share
            mem_util = process_mem_util * share
            bytes_delta = (self.bytes_in[tenant] - self.prev_bytes_in[tenant]) + (self.bytes_out[tenant] - self.prev_bytes_out[tenant])
            mbps = (bytes_delta * 8.0) / max(cycle_duration_sec, 1e-6) / 1_000_000.0
            net_util = min(100.0, (mbps / max(self.network_capacity_mbps, 1e-6)) * 100.0)
            rue = (cpu_util + mem_util + net_util) / 3.0
            tenant_rues.append(rue)
            cpu_vals.append(cpu_util)
            mem_vals.append(mem_util)
            net_vals.append(net_util)
            RESOURCE_UTILIZATION_EFFICIENCY.labels(scope="tenant", tenant_id=tenant).set(rue)
            CHECKPOINT_CPU_UTIL.labels(scope="tenant", tenant_id=tenant).set(cpu_util)
            CHECKPOINT_MEM_UTIL.labels(scope="tenant", tenant_id=tenant).set(mem_util)
            CHECKPOINT_NET_UTIL.labels(scope="tenant", tenant_id=tenant).set(net_util)

        RESOURCE_UTILIZATION_EFFICIENCY.labels(scope="cluster", tenant_id="all").set(
            sum(tenant_rues) / float(len(tenant_rues)) if tenant_rues else 0.0
        )
        CHECKPOINT_CPU_UTIL.labels(scope="cluster", tenant_id="all").set(process_cpu_util)
        CHECKPOINT_MEM_UTIL.labels(scope="cluster", tenant_id="all").set(process_mem_util)
        CHECKPOINT_NET_UTIL.labels(scope="cluster", tenant_id="all").set(process_net_util)

        mis = self.migration_impact_sum / float(self.migration_count) if self.migration_count > 0 else 0.0
        MIGRATION_IMPACT_SCORE.set(mis)

        for tenant in self.tenants:
            self.prev_msg_in[tenant] = self.msg_in[tenant]
            self.prev_msg_out[tenant] = self.msg_out[tenant]
            self.prev_bytes_in[tenant] = self.bytes_in[tenant]
            self.prev_bytes_out[tenant] = self.bytes_out[tenant]

    def _fit_to_cluster_budget(self, requested_slots: Dict[str, int]) -> Dict[str, int]:
        bounded = {tenant: max(0, int(v)) for tenant, v in requested_slots.items()}
        total = sum(bounded.values())
        if total <= self.cluster_slots:
            return bounded

        ratio = self.cluster_slots / max(float(total), 1.0)
        scaled = {tenant: int(math.floor(v * ratio)) for tenant, v in bounded.items()}
        for tenant, v in bounded.items():
            if v > 0:
                scaled[tenant] = max(1, scaled[tenant])

        while sum(scaled.values()) > self.cluster_slots:
            biggest = max(scaled, key=lambda t: scaled[t])
            if scaled[biggest] <= 0:
                break
            scaled[biggest] -= 1

        while sum(scaled.values()) < self.cluster_slots:
            neediest = max(self.tenants, key=lambda t: self.state[t]["backlog"])
            scaled[neediest] += 1

        return scaled

    def _build_baseline_allocations(self, tenants_payload: List[Dict[str, float | str]], cycle_duration_sec: float) -> List[Dict[str, object]]:
        now = time.time()
        requested = {str(t["tenant_id"]): int(float(t.get("requested_slots", 0))) for t in tenants_payload}
        grants: Dict[str, int] = {tenant: 0 for tenant in self.tenants}

        if self.scheduler_mode == "flink_default":
            for tenant in self.tenants:
                self.operator_parallelism[tenant] = max(1, min(self.cluster_slots, self.fixed_parallelism_per_tenant))
                grants[tenant] = min(requested.get(tenant, 0), self.operator_parallelism[tenant])

        elif self.scheduler_mode == "talos":
            for tenant in self.tenants:
                current_p = max(1, int(self.operator_parallelism.get(tenant, 1)))
                backlog = self.state[tenant]["backlog"]
                prev = self.prev_backlog.get(tenant, backlog)
                lag_deriv = (backlog - prev) / max(cycle_duration_sec, 1e-6)
                throughput = max((self.msg_in[tenant] - self.prev_msg_in[tenant]) / max(cycle_duration_sec, 1e-6), 1e-6)
                lag_change_rate = lag_deriv / throughput
                in_pool = _clamp(backlog / max(float(self.cluster_slots), 1.0), 0.0, 1.0)
                out_pool = _clamp((self.state[tenant]["current_p99_ms"] / max(self.sla_target_ms, 1.0)) * 0.4, 0.0, 1.0)
                backpressure = max(0.0, self.state[tenant]["current_p99_ms"] - self.sla_target_ms)
                idle_time = max(0.0, 1000.0 - backlog * 10.0)
                in_cooldown = (now - self.last_scale_ts.get(tenant, 0.0)) < self.talos_cooldown_sec

                if not in_cooldown:
                    is_bottleneck = (0.5 < in_pool <= 1.0) and (0.1 < out_pool <= 0.5) and (backpressure > 500.0)
                    if lag_change_rate > 0 and is_bottleneck:
                        target = int(math.ceil(current_p * (lag_change_rate + 1.0)))
                        current_p = max(current_p, min(self.cluster_slots, target))
                        self.last_scale_ts[tenant] = now
                    elif lag_change_rate < 0 and idle_time >= self.talos_idle_threshold and current_p > 1:
                        current_p = current_p - 1
                        self.last_scale_ts[tenant] = now

                self.operator_parallelism[tenant] = max(1, min(self.cluster_slots, current_p))
                grants[tenant] = min(requested.get(tenant, 0), self.operator_parallelism[tenant])

        elif self.scheduler_mode == "ds2":
            for tenant in self.tenants:
                current_p = max(1, int(self.operator_parallelism.get(tenant, 1)))
                last_scale = self.last_scale_ts.get(tenant, 0.0)
                stable = (now - last_scale) >= self.ds2_stability_sec

                p99 = max(self.state[tenant]["current_p99_ms"], 1.0)
                backpressure_wait = max(0.0, self.state[tenant]["current_p99_ms"] - self.sla_target_ms)
                true_processing_ms = max(1e-3, p99 - backpressure_wait)
                processing_capacity = 1000.0 / true_processing_ms
                required_throughput = max(1.0, (requested.get(tenant, 0) / max(cycle_duration_sec, 1e-6)) * 5.0)

                if processing_capacity * current_p < required_throughput:
                    req_p = int(math.ceil(required_throughput / max(processing_capacity, 1e-6)))
                    current_p += min(self.ds2_max_scaling_steps, max(0, req_p - current_p))
                    self.last_scale_ts[tenant] = now
                elif stable and self.state[tenant]["backlog"] < 1.0 and current_p > 1:
                    current_p -= 1
                    self.last_scale_ts[tenant] = now

                self.operator_parallelism[tenant] = max(1, min(self.cluster_slots, current_p))
                grants[tenant] = min(requested.get(tenant, 0), self.operator_parallelism[tenant])

        elif self.scheduler_mode == "capsys":
            for tenant in self.tenants:
                current_p = max(1, int(self.operator_parallelism.get(tenant, 1)))
                last_scale = self.last_scale_ts.get(tenant, 0.0)
                can_rebalance = (now - last_scale) >= self.capsys_rebalance_sec

                backlog = self.state[tenant]["backlog"]
                p99 = max(self.state[tenant]["current_p99_ms"], 1.0)
                bytes_delta = (self.bytes_in[tenant] - self.prev_bytes_in[tenant]) + (self.bytes_out[tenant] - self.prev_bytes_out[tenant])
                mbps = (bytes_delta * 8.0) / max(cycle_duration_sec, 1e-6) / 1_000_000.0
                net_pressure = _clamp(mbps / max(self.network_capacity_mbps, 1e-6), 0.0, 1.0)
                latency_pressure = _clamp(p99 / max(self.sla_target_ms, 1.0), 0.0, 2.0)
                backlog_pressure = _clamp(backlog / max(float(self.cluster_slots), 1.0), 0.0, 2.0)
                contention_score = (0.4 * latency_pressure) + (0.35 * backlog_pressure) + (0.25 * net_pressure)

                if can_rebalance and contention_score >= self.capsys_contention_threshold:
                    current_p = min(self.cluster_slots, current_p + 1)
                    self.last_scale_ts[tenant] = now
                elif can_rebalance and contention_score < 0.35 and current_p > 1:
                    current_p = current_p - 1
                    self.last_scale_ts[tenant] = now

                self.operator_parallelism[tenant] = max(1, min(self.cluster_slots, current_p))
                grants[tenant] = min(requested.get(tenant, 0), self.operator_parallelism[tenant])

        else:
            # streambazaar path should not call this, but keep safe fallback.
            for tenant in self.tenants:
                grants[tenant] = min(requested.get(tenant, 0), max(1, self.fixed_parallelism_per_tenant))

        grants = self._fit_to_cluster_budget(grants)
        self.prev_backlog = {tenant: self.state[tenant]["backlog"] for tenant in self.tenants}

        allocations: List[Dict[str, object]] = []
        for tenant in self.tenants:
            allocations.append(
                {
                    "tenant_id": tenant,
                    "requested_slots": requested.get(tenant, 0),
                    "granted_slots": grants.get(tenant, 0),
                    "baseline_mode": self.scheduler_mode,
                }
            )
        return allocations

    def _drive_control_cycle(self) -> None:
        cycle_now = time.time()
        cycle_duration_sec = max(cycle_now - self.last_cycle_metrics_ts, 1e-6)

        urgency = {tenant: self.state[tenant]["sla_pressure"] for tenant in self.tenants}

        tenants_payload: List[Dict[str, float | str]] = []
        for tenant in self.tenants:
            backlog = self.state[tenant]["backlog"]
            requested = max(1, int(round(backlog)))
            sla_gap = max(0.0, (self.state[tenant]["current_p99_ms"] - self.sla_target_ms) / self.sla_target_ms)
            # Read balance from Redis (shared across nodes) for fresh value
            balance = _redis_get_balance(tenant, self.virtual_balance.get(tenant, self.currency_initial))
            self.virtual_balance[tenant] = balance  # sync local cache
            tenants_payload.append(
                {
                    "tenant_id": tenant,
                    "requested_slots": requested,
                    "priority_weight": self.tenant_priority.get(tenant, 1.0),
                    "sla_gap": sla_gap,
                    "virtual_currency_balance": balance,
                }
            )

        # Aggregate demand D^t(r): sum of all bids submitted this cycle (Eq. 7)
        self.aggregate_demand = sum(self.state[t]["last_bid"] for t in self.tenants)

        if self.scheduler_mode == "streambazaar":
            clear_result = self._post(
                self.clear_url,
                {
                    "resource_units": self.cluster_slots,
                    "min_price": 0.1,
                    "sla_urgency": urgency,
                    # Pass requested_units per tenant so greedy knapsack knows bundle sizes
                    "requested_units": {t["tenant_id"]: t["requested_slots"] for t in tenants_payload},
                },
            )
            alloc_result = self._post(
                self.allocate_url,
                {
                    "total_slots": self.cluster_slots,
                    "tenants": tenants_payload,
                },
            )
            allocations = alloc_result.get("allocations", [])
            if not isinstance(allocations, list):
                allocations = []
        else:
            clear_result = {
                "winners": [],
                "clearing_price": 0.0,
                "revenue": 0.0,
                "mode": self.scheduler_mode,
            }
            allocations = self._build_baseline_allocations(tenants_payload, cycle_duration_sec)
            alloc_result = {"mode": self.scheduler_mode, "allocations": allocations}

        winners = clear_result.get("winners", [])
        winner_ids = set()
        winner_costs: Dict[str, float] = {}
        if isinstance(winners, list):
            for winner in winners:
                if isinstance(winner, dict):
                    tid = str(winner.get("tenant_id", ""))
                    winner_ids.add(tid)
                    winner_costs[tid] = float(winner.get("total_cost", 0.0))

        granted_map: Dict[str, float] = {}
        for allocation in allocations:
            if not isinstance(allocation, dict):
                continue
            tenant_id = str(allocation.get("tenant_id", "unknown"))
            requested = int(float(allocation.get("requested_slots", 0)))
            granted = int(float(allocation.get("granted_slots", 0)))
            granted_map[tenant_id] = float(granted)
            self.state[tenant_id]["backlog"] = max(0.0, self.state[tenant_id]["backlog"] - granted)
            TENANT_BACKLOG.labels(tenant_id=tenant_id).set(self.state[tenant_id]["backlog"])

            # Deduct second-price payment from virtual balance (budget constraint Eq. line 373)
            if self.scheduler_mode == "streambazaar" and tenant_id in winner_ids:
                self._deduct_currency(tenant_id, winner_costs.get(tenant_id, 0.0))

            allocation_event = {
                "timestamp": time.time(),
                "tenant_id": tenant_id,
                "requested_slots": requested,
                "granted_slots": granted,
                "won_auction": tenant_id in winner_ids,
                "clearing_price": clear_result.get("clearing_price", 0.0),
                "revenue": clear_result.get("revenue", 0.0),
                "virtual_balance": round(self.virtual_balance.get(tenant_id, 0.0), 2),
            }
            self._publish(self.alloc_topic, tenant_id, allocation_event, kind="allocation")
            self._publish(self.output_topic_template.format(tenant_id=tenant_id), tenant_id, allocation_event, kind="tenant_output")
            self.stats["published_allocations"] += 1
            self.last_allocation_publish_ts[tenant_id] = time.time()

            sla_gap = max(0.0, (self.state[tenant_id]["current_p99_ms"] - self.sla_target_ms) / self.sla_target_ms)
            if granted < requested or sla_gap > 0.1:
                total_backlog = sum(self.state[t]["backlog"] for t in self.tenants)
                source_util = min(0.99, max(0.05, self.state[tenant_id]["backlog"] / max(float(self.cluster_slots), 1.0)))
                target_util = max(0.05, min(0.95, (total_backlog - self.state[tenant_id]["backlog"]) / max(float(self.cluster_slots), 1.0)))
                migration_start = time.time()
                migration = self._post(
                    self.migrate_url,
                    {
                        "tenant_id": tenant_id,
                        "source": "tm-hot",
                        "target": "tm-cool",
                        "source_utilization": source_util,
                        "target_utilization": target_util,
                        "current_p99_ms": self.state[tenant_id]["current_p99_ms"],
                        "sla_target_ms": self.sla_target_ms,
                        "cooldown_sec": 30.0,
                        "state_size_kb": self.state_size_kb,
                    },
                )
                migrate_elapsed_sec = max(0.0, time.time() - migration_start)
                if str(migration.get("status", "")) == "scheduled":
                    self._publish(self.preempt_topic, tenant_id, migration, kind="preemption")
                    self.stats["published_preemptions"] += 1
                    migration_impact = max(0.0, (self.state[tenant_id]["current_p99_ms"] - self.sla_target_ms) / self.sla_target_ms)
                    self.migration_impact_sum += migration_impact
                    self.migration_count += 1
                    transfer_time_sec = migrate_elapsed_sec
                    downtime_sec = max(0.0, time.time() - self.last_allocation_publish_ts.get(tenant_id, time.time()))
                    MIGRATION_TRANSFER_TIME_SECONDS.labels(tenant_id=tenant_id).set(transfer_time_sec)
                    MIGRATION_DOWNTIME_SECONDS.labels(tenant_id=tenant_id).set(downtime_sec)
                    MIGRATION_TRANSFER_TIME_TOTAL.labels(tenant_id=tenant_id).inc(transfer_time_sec)
                    MIGRATION_DOWNTIME_TOTAL.labels(tenant_id=tenant_id).inc(downtime_sec)

        # Apply virtual currency decay + injection each auction interval (Eq. 2)
        if self.scheduler_mode == "streambazaar":
            self._update_virtual_currency(granted_map)

        self._update_advanced_metrics(allocations=allocations, cycle_duration_sec=cycle_duration_sec)
        self.last_cycle_metrics_ts = cycle_now

        metric_event = {
            "timestamp": time.time(),
            "clear_result": clear_result,
            "alloc_result": alloc_result,
            "stats": self.stats,
        }
        self._publish(self.metrics_topic, "coordinator", metric_event, kind="system_metrics")
        self.stats["published_metrics"] += 1
        CLEARING_CYCLES.inc()

    def _run_loop(self) -> None:
        self._ensure_kafka_clients()
        while not self.stop_event.is_set():
            try:
                if self.consumer is None:
                    self._ensure_kafka_clients()
                    continue
                for msg in self.consumer:
                    if self.stop_event.is_set():
                        break

                    topic = msg.topic
                    event = msg.value if isinstance(msg.value, dict) else {}
                    tenant_id = str(event.get("tenant_id", ""))
                    if tenant_id not in self.state:
                        continue

                    dataset = str(event.get("dataset", event.get("workload", "unknown")))
                    payload_bytes = len(json.dumps(event).encode("utf-8"))

                    CONSUMED_EVENTS.labels(topic=topic).inc()
                    MESSAGE_IN.labels(topic=topic, tenant_id=tenant_id, dataset=dataset).inc()
                    MESSAGE_BYTES_IN.labels(topic=topic, tenant_id=tenant_id, dataset=dataset).inc(payload_bytes)
                    MESSAGE_LAST_BYTES.labels(topic=topic, tenant_id=tenant_id, direction="in").set(payload_bytes)
                    self.stats["consumed"] += 1
                    self.msg_in[tenant_id] += 1.0
                    self.bytes_in[tenant_id] += float(payload_bytes)

                    self.state[tenant_id]["backlog"] = min(50.0, self.state[tenant_id]["backlog"] + 1.0)
                    self.state[tenant_id]["sla_pressure"] = self._estimate_sla_pressure(tenant_id, event)

                    event_ts = event.get("timestamp")
                    if isinstance(event_ts, (int, float)):
                        latency_ms = max(0.0, (time.time() - float(event_ts)) * 1000.0)
                        self.latency_samples_ms[tenant_id].append(latency_ms)

                    TENANT_BACKLOG.labels(tenant_id=tenant_id).set(self.state[tenant_id]["backlog"])
                    self._set_latency_percentile_gauges(tenant_id)
                    TENANT_P99.labels(tenant_id=tenant_id).set(self.state[tenant_id]["current_p99_ms"])

                    queue_backlog = self.state[tenant_id]["backlog"] * 100.0
                    utilization = min(0.95, 0.35 + self.state[tenant_id]["backlog"] / 80.0)

                    if self.scheduler_mode == "streambazaar":
                        pricing = self._post(
                            self.pricing_url,
                            {
                                "tenant_id": tenant_id,
                                "utilization": utilization,
                                "sla_pressure": self.state[tenant_id]["sla_pressure"],
                                "queue_backlog": queue_backlog,
                                # Real virtual balance (Eq. 2) and aggregate demand D^t(r) (Eq. 7)
                                "credit_balance": self.virtual_balance.get(tenant_id, self.currency_initial),
                                "demand": self.aggregate_demand,
                                "capacity": float(self.cluster_slots) * 1000.0,
                            },
                        )
                        bid_floor = float(pricing.get("bid_floor", 0.1))
                        bid_price = round(bid_floor * (1.0 + 0.1 * self.tenant_priority.get(tenant_id, 1.0)), 6)
                        self.state[tenant_id]["last_bid"] = bid_price
                        TENANT_LAST_BID.labels(tenant_id=tenant_id).set(bid_price)
                        self._post(self.bid_url, {"tenant_id": tenant_id, "bid_price": bid_price})
                    else:
                        bid_price = round(0.1 * self.tenant_priority.get(tenant_id, 1.0), 6)
                        self.state[tenant_id]["last_bid"] = bid_price
                        TENANT_LAST_BID.labels(tenant_id=tenant_id).set(bid_price)

                    now = time.time()
                    if now - self.last_clear_ts >= self.clear_interval_sec:
                        self._drive_control_cycle()
                        self.last_clear_ts = now

                if time.time() - self.last_clear_ts >= self.clear_interval_sec:
                    self._drive_control_cycle()
                    self.last_clear_ts = time.time()
            except Exception as exc:
                self.stats["last_loop_error"] = str(exc)
                CONTROL_LOOP_ERRORS.labels(operation="main_loop").inc()
                time.sleep(1.0)


coordinator = StreamCoordinator()


@app.on_event("startup")
def startup_event() -> None:
    coordinator.start()


@app.on_event("shutdown")
def shutdown_event() -> None:
    coordinator.stop()


@app.get("/health")
def health() -> Dict[str, object]:
    return {
        "status": "ok",
        "service": "stream-coordinator",
        "tenants": coordinator.tenants,
        "input_topics": coordinator.consumer_topics,
        "alloc_topic": coordinator.alloc_topic,
        "preempt_topic": coordinator.preempt_topic,
        "metrics_topic": coordinator.metrics_topic,
        "scheduler_mode": coordinator.scheduler_mode,
        "running": coordinator.stats["running"],
        "consumed": coordinator.stats["consumed"],
        "published_allocations": coordinator.stats["published_allocations"],
        "published_preemptions": coordinator.stats["published_preemptions"],
        "published_metrics": coordinator.stats["published_metrics"],
        "last_loop_error": coordinator.stats["last_loop_error"],
    }


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
