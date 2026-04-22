import logging
from itertools import count
from pathlib import Path
from typing import Dict, Iterator

from ..synthetic_fallback import generate_synthetic_unsw
from .base import BaseLoader


class UNSWLoader(BaseLoader):
    DATASET_NAME = "network-intrusion"
    PRIORITY = "high"

    def __init__(self, dataset_dir: Path, logger: logging.Logger) -> None:
        super().__init__(dataset_dir=dataset_dir, logger=logger)

    def records(self, subset_lines: int = 0, use_synthetic: bool = False) -> Iterator[Dict]:
        if use_synthetic:
            for rec in generate_synthetic_unsw():
                yield self._annotate(rec, self.DATASET_NAME, self.PRIORITY)
            return

        train_file = self.dataset_dir / "UNSW_NB15_training-set.csv"
        test_file = self.dataset_dir / "UNSW_NB15_testing-set.csv"
        train_iter = self._csv_cycle(train_file, delimiter=",", has_header=True, subset_lines=subset_lines)
        test_iter = self._csv_cycle(test_file, delimiter=",", has_header=True, subset_lines=subset_lines)

        for idx in count(1):
            rec = next(train_iter) if idx % 4 != 0 else next(test_iter)
            event = {
                "event_id": f"unsw-real-{idx}",
                "flow_id": self._safe_int(rec.get("id", str(idx)), idx),
                "src_ip": rec.get("srcip", "0.0.0.0"),
                "dst_ip": rec.get("dstip", "0.0.0.0"),
                "proto": rec.get("proto", "unknown"),
                "service": rec.get("service", "unknown"),
                "duration": self._safe_float(rec.get("dur", "0"), 0.0),
                "src_bytes": self._safe_int(rec.get("sbytes", "0"), 0),
                "dst_bytes": self._safe_int(rec.get("dbytes", "0"), 0),
                "label": self._safe_int(rec.get("label", "0"), 0),
                "data_source": "real",
            }
            yield self._annotate(event, self.DATASET_NAME, self.PRIORITY)
