import logging
from itertools import count
from pathlib import Path
from typing import Dict, Iterator, List

from ..synthetic_fallback import generate_synthetic_berkeley
from .base import BaseLoader


class BerkeleyLoader(BaseLoader):
    DATASET_NAME = "iot-sensors"
    PRIORITY = "medium"

    def __init__(self, dataset_dir: Path, logger: logging.Logger) -> None:
        super().__init__(dataset_dir=dataset_dir, logger=logger)

    def records(self, subset_lines: int = 0, use_synthetic: bool = False) -> Iterator[Dict]:
        if use_synthetic:
            for rec in generate_synthetic_berkeley():
                yield self._annotate(rec, self.DATASET_NAME, self.PRIORITY)
            return

        data_file = self.dataset_dir / "data.txt"
        lines: List[List[str]] = []
        with data_file.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                if not line.strip() or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) < 5:
                    continue
                lines.append(parts)
                if subset_lines and line_no >= subset_lines:
                    break

        if not lines:
            raise RuntimeError(f"No records available in {data_file}")

        self.logger.info("Loaded %s Berkeley records from %s", len(lines), data_file)

        for idx in count(1):
            row = lines[(idx - 1) % len(lines)]
            event = {
                "event_id": f"berkeley-real-{idx}",
                "reading_id": idx,
                "sensor_id": self._safe_int(row[3], idx % 54),
                "temperature": self._safe_float(row[4], 0.0),
                "humidity": self._safe_float(row[5], 0.0) if len(row) > 5 else 0.0,
                "light": self._safe_float(row[6], 0.0) if len(row) > 6 else 0.0,
                "voltage": self._safe_float(row[7], 0.0) if len(row) > 7 else 0.0,
                "room": f"mote-{self._safe_int(row[3], idx % 54)}",
                "data_source": "real",
            }
            yield self._annotate(event, self.DATASET_NAME, self.PRIORITY)
