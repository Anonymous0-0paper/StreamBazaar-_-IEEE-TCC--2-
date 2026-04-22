import random
import time
from uuid import uuid4

MERCHANTS = ["A-Mart", "QuickPay", "GrocerX", "OnlineHub"]


class FraudDetectionWorkload:
    def __init__(self, tenant_id: str, input_rate: int) -> None:
        self.tenant_id = tenant_id
        self.input_rate = input_rate

    def generate_workload(self):
        while True:
            transaction = {
                "tenant_id": self.tenant_id,
                "transaction_id": str(uuid4()),
                "user_id": random.randint(1, 10000),
                "amount": round(random.uniform(1, 1000), 2),
                "merchant": random.choice(MERCHANTS),
                "timestamp": time.time(),
                "is_fraud": random.random() < 0.02,
            }
            yield transaction
            time.sleep(1.0 / max(1, self.input_rate))
