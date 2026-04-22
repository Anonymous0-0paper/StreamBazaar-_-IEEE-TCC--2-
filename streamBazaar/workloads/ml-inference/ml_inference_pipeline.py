import random
import time
from uuid import uuid4


class MLInferenceWorkload:
    def __init__(self, tenant_id: str, input_rate: int) -> None:
        self.tenant_id = tenant_id
        self.input_rate = input_rate

    def generate_workload(self):
        while True:
            sample = {
                "tenant_id": self.tenant_id,
                "request_id": str(uuid4()),
                "feature_vector": [random.random() for _ in range(8)],
                "model": "resnet-lite",
                "timestamp": time.time(),
            }
            yield sample
            time.sleep(1.0 / max(1, self.input_rate))
