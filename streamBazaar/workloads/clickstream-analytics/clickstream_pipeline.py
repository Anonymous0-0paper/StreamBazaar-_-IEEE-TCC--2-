import random
import time
from uuid import uuid4

PAGES = ["/", "/product", "/cart", "/checkout", "/search"]


class ClickstreamWorkload:
    def __init__(self, tenant_id: str, input_rate: int) -> None:
        self.tenant_id = tenant_id
        self.input_rate = input_rate

    def generate_workload(self):
        while True:
            event = {
                "tenant_id": self.tenant_id,
                "event_id": str(uuid4()),
                "user_id": random.randint(1, 100000),
                "page": random.choice(PAGES),
                "action": random.choice(["view", "click", "scroll"]),
                "timestamp": time.time(),
                "session_id": str(uuid4()),
            }
            yield event
            time.sleep(1.0 / max(1, self.input_rate))
