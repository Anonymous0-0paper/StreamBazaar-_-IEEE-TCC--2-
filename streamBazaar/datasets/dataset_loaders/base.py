import csv
import logging
import time
from itertools import cycle, islice
from pathlib import Path
from typing import Dict, Iterable, Iterator, List


class BaseLoader:
    def __init__(self, dataset_dir: Path, logger: logging.Logger) -> None:
        self.dataset_dir = dataset_dir
        self.logger = logger

    def records(self, subset_lines: int = 0, use_synthetic: bool = False) -> Iterator[Dict]:
        raise NotImplementedError

    def _safe_float(self, value: str, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    def _safe_int(self, value: str, default: int = 0) -> int:
        try:
            return int(float(value))
        except Exception:
            return default

    def _csv_cycle(self, file_path: Path, delimiter: str = ",", has_header: bool = True, subset_lines: int = 0) -> Iterator[Dict]:
        if not file_path.exists():
            raise FileNotFoundError(f"Dataset file not found: {file_path}")

        with file_path.open("r", encoding="utf-8", newline="") as handle:
            if has_header:
                reader: Iterable[Dict] = csv.DictReader(handle, delimiter=delimiter)
            else:
                rows = csv.reader(handle, delimiter=delimiter)
                reader = ({"raw": row} for row in rows)

            cached = list(islice(reader, subset_lines)) if subset_lines and subset_lines > 0 else list(reader)
            if not cached:
                raise RuntimeError(f"No records available in {file_path}")

        self.logger.info("Loaded %s records from %s", len(cached), file_path)
        return cycle(cached)

    def _annotate(self, record: Dict, dataset_name: str, priority: str) -> Dict:
        now = time.time()
        result = dict(record)
        result.setdefault("event_time", now)
        result.setdefault("timestamp", now)
        result.setdefault("dataset", dataset_name)
        result.setdefault("priority", priority)
        return result
