import random
import time
from itertools import count
from typing import Dict, Iterator
from uuid import uuid4


MERCHANTS = ["A-Mart", "QuickPay", "GrocerX", "OnlineHub"]
PAGES = ["/", "/product", "/cart", "/checkout", "/search"]
PROTOS = ["tcp", "udp", "icmp"]
SERVICES = ["http", "dns", "ftp", "ssh"]


def generate_synthetic_fraud(seed: int = 42) -> Iterator[Dict]:
    random.seed(seed)
    for idx in count(1):
        yield {
            "event_id": f"fraud-synth-{idx}",
            "event_time": time.time(),
            "transaction_id": str(uuid4()),
            "user_id": random.randint(1, 100000),
            "amount": round(random.uniform(1, 1200), 2),
            "merchant": random.choice(MERCHANTS),
            "card1": random.randint(1000, 9999),
            "addr1": random.randint(100, 999),
            "is_fraud": int(random.random() < 0.025),
            "data_source": "synthetic",
        }


def generate_synthetic_criteo(seed: int = 42) -> Iterator[Dict]:
    random.seed(seed)
    for idx in count(1):
        user = random.randint(1, 250000)
        yield {
            "event_id": f"criteo-synth-{idx}",
            "event_time": time.time(),
            "user_id": user,
            "session_id": f"sess-{user}-{idx % 8}",
            "ad_id": random.randint(1, 1000000),
            "campaign_id": random.randint(1, 50000),
            "page": random.choice(PAGES),
            "clicked": int(random.random() < 0.12),
            "label": int(random.random() < 0.12),
            "data_source": "synthetic",
        }


def generate_synthetic_unsw(seed: int = 42) -> Iterator[Dict]:
    random.seed(seed)
    for idx in count(1):
        src = f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
        dst = f"172.16.{random.randint(0, 255)}.{random.randint(1, 254)}"
        yield {
            "event_id": f"unsw-synth-{idx}",
            "event_time": time.time(),
            "flow_id": idx,
            "src_ip": src,
            "dst_ip": dst,
            "proto": random.choice(PROTOS),
            "service": random.choice(SERVICES),
            "duration": round(random.uniform(0.001, 4.0), 6),
            "src_bytes": random.randint(40, 150000),
            "dst_bytes": random.randint(40, 150000),
            "label": int(random.random() < 0.35),
            "data_source": "synthetic",
        }


def generate_synthetic_berkeley(seed: int = 42) -> Iterator[Dict]:
    random.seed(seed)
    for idx in count(1):
        yield {
            "event_id": f"iot-synth-{idx}",
            "event_time": time.time(),
            "reading_id": idx,
            "sensor_id": random.randint(1, 60),
            "temperature": round(random.uniform(14.0, 34.0), 2),
            "humidity": round(random.uniform(20.0, 88.0), 2),
            "light": round(random.uniform(0.0, 1000.0), 2),
            "voltage": round(random.uniform(2.0, 3.4), 3),
            "room": random.choice(["lab-a", "lab-b", "hall", "server-room"]),
            "data_source": "synthetic",
        }
