import logging
from itertools import count
from pathlib import Path
from typing import Dict, Iterator

from ..synthetic_fallback import generate_synthetic_fraud
from .base import BaseLoader


class FraudLoader(BaseLoader):
    DATASET_NAME = "fraud"
    PRIORITY = "high"

    def __init__(self, dataset_dir: Path, logger: logging.Logger) -> None:
        super().__init__(dataset_dir=dataset_dir, logger=logger)

    def records(self, subset_lines: int = 0, use_synthetic: bool = False) -> Iterator[Dict]:
        if use_synthetic:
            for rec in generate_synthetic_fraud():
                yield self._annotate(rec, self.DATASET_NAME, self.PRIORITY)
            return

        tx_file = self.dataset_dir / "train_transaction.csv"
        identity_file = self.dataset_dir / "train_identity.csv"
        tx_iter = self._csv_cycle(tx_file, delimiter=",", has_header=True, subset_lines=subset_lines)
        id_iter = self._csv_cycle(identity_file, delimiter=",", has_header=True, subset_lines=subset_lines)

        for idx in count(1):
            tx = next(tx_iter)
            ident = next(id_iter)
            event = {
                "event_id": f"fraud-real-{idx}",
                "transaction_id": tx.get("TransactionID", str(idx)),
                "user_id": self._safe_int(tx.get("card1", "0")),
                "amount": self._safe_float(tx.get("TransactionAmt", "0")),
                "merchant": tx.get("ProductCD", "unknown"),
                "card1": self._safe_int(tx.get("card1", "0")),
                "addr1": self._safe_int(tx.get("addr1", "0")),
                "is_fraud": self._safe_int(tx.get("isFraud", "0")),
                "device_type": ident.get("DeviceType", "unknown"),
                "data_source": "real",
            }
            yield self._annotate(event, self.DATASET_NAME, self.PRIORITY)
