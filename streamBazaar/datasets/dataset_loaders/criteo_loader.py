import logging
import csv
from itertools import count
from pathlib import Path
from typing import Dict, Iterator, List

from ..synthetic_fallback import generate_synthetic_criteo
from .base import BaseLoader


class CriteoLoader(BaseLoader):
    DATASET_NAME = "web-analytics"
    PRIORITY = "low"

    def __init__(self, dataset_dir: Path, logger: logging.Logger) -> None:
        super().__init__(dataset_dir=dataset_dir, logger=logger)

    def _resolve_source_file(self) -> Path:
        candidates = ["train.txt", "train_subset.txt", "train.csv", "random_submission.csv"]
        for file_name in candidates:
            candidate = self.dataset_dir / file_name
            if candidate.exists():
                return candidate
        raise FileNotFoundError(f"No Criteo source file found in {self.dataset_dir}. Expected one of: {candidates}")

    def _load_rows(self, source_file: Path, line_cap: int) -> List[Dict]:
        rows: List[Dict] = []
        if source_file.suffix.lower() == ".txt":
            with source_file.open("r", encoding="utf-8") as handle:
                for line_no, raw in enumerate(handle, start=1):
                    parts = raw.rstrip("\n").split("\t")
                    if len(parts) < 2:
                        continue
                    row = {
                        "label": self._safe_int(parts[0], 0),
                        "i1": self._safe_int(parts[1], 0),
                        "tail": parts[2:15],
                    }
                    rows.append(row)
                    if line_cap and line_no >= line_cap:
                        break
            return rows

        with source_file.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for line_no, rec in enumerate(reader, start=1):
                click_value = rec.get("click") or rec.get("label") or rec.get("clicked")
                predicted = rec.get("Predicted")
                if click_value is not None:
                    label = self._safe_int(click_value, 0)
                elif predicted is not None:
                    label = int(self._safe_float(predicted, 0.0) >= 0.5)
                else:
                    label = 0

                raw_id = rec.get("Id") or rec.get("id") or rec.get("user_id") or str(line_no)
                entity_id = self._safe_int(raw_id, line_no)
                row = {
                    "label": label,
                    "i1": entity_id,
                    "tail": [rec.get("Predicted", "0")],
                }
                rows.append(row)
                if line_cap and line_no >= line_cap:
                    break
        return rows

    def records(self, subset_lines: int = 0, use_synthetic: bool = False) -> Iterator[Dict]:
        if use_synthetic:
            for rec in generate_synthetic_criteo():
                yield self._annotate(rec, self.DATASET_NAME, self.PRIORITY)
            return

        train_file = self._resolve_source_file()
        line_cap = subset_lines if subset_lines and subset_lines > 0 else 0
        rows = self._load_rows(train_file, line_cap)

        if not rows:
            raise RuntimeError(f"No records available in {train_file}")

        self.logger.info("Loaded %s Criteo records from %s", len(rows), train_file)

        for idx in count(1):
            row = rows[(idx - 1) % len(rows)]
            event = {
                "event_id": f"criteo-real-{idx}",
                "user_id": row["i1"],
                "session_id": f"sess-{row['i1']}-{idx % 16}",
                "ad_id": row["i1"],
                "campaign_id": self._safe_int(row["tail"][0], 0) if row["tail"] else 0,
                "page": "/ad-click",
                "clicked": row["label"],
                "label": row["label"],
                "data_source": "real",
            }
            yield self._annotate(event, self.DATASET_NAME, self.PRIORITY)
