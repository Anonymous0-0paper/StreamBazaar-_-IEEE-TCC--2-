from typing import Dict

import docker
import psutil


class ResourceMonitor:
    def __init__(self) -> None:
        self.docker_client = docker.from_env()

    def get_per_tenant_resources(self) -> Dict[str, Dict[str, float]]:
        tenant_resources: Dict[str, Dict[str, float]] = {}
        for container in self.docker_client.containers.list():
            tenant_id = container.labels.get("tenant_id")
            if not tenant_id:
                continue

            stats = container.stats(stream=False)
            cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - stats["precpu_stats"]["cpu_usage"]["total_usage"]
            system_delta = stats["cpu_stats"].get("system_cpu_usage", 0) - stats["precpu_stats"].get("system_cpu_usage", 1)
            cpu_percent = (cpu_delta / system_delta) * 100.0 if system_delta > 0 else 0.0

            memory_usage = float(stats["memory_stats"].get("usage", 0.0))
            memory_limit = float(stats["memory_stats"].get("limit", 1.0))
            memory_percent = (memory_usage / memory_limit) * 100.0 if memory_limit > 0 else 0.0

            network_rx = sum(net["rx_bytes"] for net in stats.get("networks", {}).values())
            network_tx = sum(net["tx_bytes"] for net in stats.get("networks", {}).values())

            tenant_resources[tenant_id] = {
                "cpu_percent": cpu_percent,
                "memory_percent": memory_percent,
                "memory_bytes": memory_usage,
                "network_rx_bytes": float(network_rx),
                "network_tx_bytes": float(network_tx),
            }
        return tenant_resources

    def get_cluster_resources(self) -> Dict[str, float]:
        disk = psutil.disk_io_counters()
        network = psutil.net_io_counters()
        return {
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_io_read": float(disk.read_bytes),
            "disk_io_write": float(disk.write_bytes),
            "network_io_sent": float(network.bytes_sent),
            "network_io_recv": float(network.bytes_recv),
        }
