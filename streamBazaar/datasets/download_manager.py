#!/usr/bin/env python3
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class DatasetSpec:
    key: str
    display_name: str
    destination: Path
    required_files: List[str]
    source_url: str
    kaggle_competition: Optional[str] = None
    min_rows: Dict[str, int] = field(default_factory=dict)
    min_bytes: Dict[str, int] = field(default_factory=dict)
    fallback_to_synthetic: bool = True
    priority: str = "low"


@dataclass
class DatasetStatus:
    key: str
    exists: bool
    validated: bool
    synthetic_fallback: bool
    errors: List[str] = field(default_factory=list)


@dataclass
class DatasetManagerConfig:
    root_dir: Path
    enable_downloads: bool = True
    enable_synthetic_fallback: bool = True
    subset_lines: int = 0
    criteo_subset_lines: int = 500000
    required_free_gb: float = 5.0
    logger_name: str = "streambazaar.datasets"


DEFAULT_DATASET_SPECS: Dict[str, DatasetSpec] = {
    "fraud": DatasetSpec(
        key="fraud",
        display_name="IEEE-CIS Fraud Detection",
        destination=Path("fraud-detection"),
        required_files=["train_transaction.csv", "train_identity.csv"],
        source_url="https://www.kaggle.com/c/ieee-fraud-detection/data",
        kaggle_competition="ieee-fraud-detection",
        min_rows={"train_transaction.csv": 500000, "train_identity.csv": 100000},
        min_bytes={"train_transaction.csv": 50_000_000, "train_identity.csv": 10_000_000},
        priority="high",
    ),
    "web-analytics": DatasetSpec(
        key="web-analytics",
        display_name="Criteo Click Logs",
        destination=Path("web-analytics"),
        required_files=["train.txt"],
        source_url="https://www.kaggle.com/c/criteo-display-ad-challenge/data",
        kaggle_competition="criteo-display-ad-challenge",
        min_rows={"train.txt": 100000, "random_submission.csv": 100000},
        min_bytes={"train.txt": 10_000_000, "random_submission.csv": 1_000_000},
        priority="low",
    ),
    "network-intrusion": DatasetSpec(
        key="network-intrusion",
        display_name="UNSW-NB15",
        destination=Path("network-intrusion"),
        required_files=["UNSW_NB15_training-set.csv", "UNSW_NB15_testing-set.csv"],
        source_url="https://research.unsw.edu.au/projects/unsw-nb15-dataset",
        min_rows={"UNSW_NB15_training-set.csv": 100000, "UNSW_NB15_testing-set.csv": 10000},
        min_bytes={"UNSW_NB15_training-set.csv": 20_000_000, "UNSW_NB15_testing-set.csv": 2_000_000},
        priority="high",
    ),
    "iot-sensors": DatasetSpec(
        key="iot-sensors",
        display_name="Intel Berkeley Lab Sensor Data",
        destination=Path("iot-sensors"),
        required_files=["data.txt"],
        source_url="http://db.csail.mit.edu/labdata/labdata.html",
        min_rows={"data.txt": 100000},
        min_bytes={"data.txt": 1_000_000},
        priority="medium",
    ),
}


class DatasetManager:
    def __init__(self, config: DatasetManagerConfig) -> None:
        self.config = config
        self.root_dir = config.root_dir
        self.logger = logging.getLogger(config.logger_name)
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s"))
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def ensure_datasets(self, dataset_keys: List[str]) -> Dict[str, DatasetStatus]:
        statuses: Dict[str, DatasetStatus] = {}
        self.root_dir.mkdir(parents=True, exist_ok=True)

        for key in dataset_keys:
            spec = self._get_spec(key)
            status = self._ensure_one(spec)
            statuses[key] = status

        return statuses

    def _ensure_one(self, spec: DatasetSpec) -> DatasetStatus:
        self.logger.info("Checking dataset: %s", spec.display_name)
        ds_path = self.root_dir / spec.destination
        ds_path.mkdir(parents=True, exist_ok=True)
        status = DatasetStatus(key=spec.key, exists=False, validated=False, synthetic_fallback=False)

        if self._required_files_exist(spec, ds_path):
            status.exists = True
            status.validated, errors = self.validate_dataset(spec, ds_path)
            status.errors.extend(errors)
            if status.validated:
                return status
            self.logger.warning("Dataset files exist but validation failed for %s", spec.key)

        if self.config.enable_downloads:
            self._check_disk_space_or_raise(ds_path)
            try:
                self._download_dataset(spec, ds_path)
            except Exception as exc:
                status.errors.append(str(exc))
                self.logger.warning("Download failed for %s: %s", spec.key, exc)

        status.exists = self._required_files_exist(spec, ds_path)
        status.validated, errors = self.validate_dataset(spec, ds_path)
        status.errors.extend(errors)

        if not status.validated and self.config.enable_synthetic_fallback and spec.fallback_to_synthetic:
            status.synthetic_fallback = True
            self.logger.warning(
                "Dataset '%s' unavailable or invalid. Using synthetic fallback.",
                spec.key,
            )

        return status

    def _get_spec(self, key: str) -> DatasetSpec:
        if key not in DEFAULT_DATASET_SPECS:
            raise ValueError(f"Unsupported dataset '{key}'. Supported: {sorted(DEFAULT_DATASET_SPECS.keys())}")
        return DEFAULT_DATASET_SPECS[key]

    def dataset_path(self, key: str) -> Path:
        return self.root_dir / self._get_spec(key).destination

    def _web_analytics_candidates(self) -> List[str]:
        return ["train.txt", "train_subset.txt", "train.csv", "random_submission.csv"]

    def _validation_targets(self, spec: DatasetSpec, ds_path: Path) -> List[str]:
        if spec.key == "web-analytics":
            for file_name in self._web_analytics_candidates():
                if (ds_path / file_name).exists():
                    return [file_name]
            return [spec.required_files[0]]
        return list(spec.required_files)

    def validate_dataset(self, spec: DatasetSpec, ds_path: Path) -> Tuple[bool, List[str]]:
        errors: List[str] = []
        for file_name in self._validation_targets(spec, ds_path):
            path = ds_path / file_name
            if not path.exists():
                errors.append(f"Missing required file: {path}")
                continue

            min_bytes = spec.min_bytes.get(file_name)
            if min_bytes is not None and path.stat().st_size < min_bytes:
                errors.append(f"File too small: {path} ({path.stat().st_size} bytes < {min_bytes} bytes)")

            min_rows = spec.min_rows.get(file_name)
            if min_rows is not None:
                rows = self._fast_count_lines(path)
                if rows < min_rows:
                    errors.append(f"Too few records in {path}: {rows} < {min_rows}")

        return len(errors) == 0, errors

    def _required_files_exist(self, spec: DatasetSpec, ds_path: Path) -> bool:
        if spec.key == "web-analytics":
            return any((ds_path / file_name).exists() for file_name in self._web_analytics_candidates())
        return all((ds_path / file_name).exists() for file_name in spec.required_files)

    def _download_dataset(self, spec: DatasetSpec, ds_path: Path) -> None:
        if spec.kaggle_competition:
            self._download_from_kaggle(spec, ds_path)
            return

        if spec.key == "network-intrusion":
            self._download_unsw_from_mirrors(spec, ds_path)
            return

        if spec.key == "iot-sensors":
            data_url = os.getenv("BERKELEY_DATA_URL", "http://db.csail.mit.edu/labdata/data.txt.gz")
            self._download_http_with_progress(data_url, ds_path / "data.txt.gz")
            self._gunzip_if_needed(ds_path / "data.txt.gz", ds_path / "data.txt")
            return

        raise RuntimeError(f"No download strategy for dataset '{spec.key}'")

    def _download_from_kaggle(self, spec: DatasetSpec, ds_path: Path) -> None:
        if not self._has_kaggle_credentials():
            raise RuntimeError(
                "Kaggle credentials not found. Configure KAGGLE_USERNAME/KAGGLE_KEY or ~/.kaggle/kaggle.json"
            )

        if shutil.which("kaggle") is None:
            raise RuntimeError("Kaggle CLI not found. Install with: pip install kaggle")

        self.logger.info("Downloading from Kaggle competition '%s'", spec.kaggle_competition)
        cmd = [
            "kaggle",
            "competitions",
            "download",
            "-c",
            str(spec.kaggle_competition),
            "-p",
            str(ds_path),
            "--force",
        ]
        self._run_command(cmd)

        for file_name in spec.required_files:
            candidate = ds_path / file_name
            zipped = ds_path / f"{file_name}.zip"
            if candidate.exists():
                continue
            if zipped.exists():
                self._run_command(["unzip", "-o", str(zipped), "-d", str(ds_path)])

        if spec.key == "web-analytics":
            self._enforce_criteo_subset(ds_path / "train.txt")

    def _download_unsw_from_mirrors(self, spec: DatasetSpec, ds_path: Path) -> None:
        custom_base = os.getenv("UNSW_NB15_BASE_URL", "").strip().rstrip("/")
        if custom_base:
            urls = [
                (f"{custom_base}/UNSW_NB15_training-set.csv", "UNSW_NB15_training-set.csv"),
                (f"{custom_base}/UNSW_NB15_testing-set.csv", "UNSW_NB15_testing-set.csv"),
            ]
            for url, file_name in urls:
                self._download_http_with_progress(url, ds_path / file_name)
            return

        raise RuntimeError(
            "UNSW-NB15 direct download requires manual retrieval or UNSW_NB15_BASE_URL pointing to mirrored CSV files. "
            f"See source: {spec.source_url}"
        )

    def _enforce_criteo_subset(self, train_path: Path) -> None:
        subset_lines = self.config.criteo_subset_lines
        if subset_lines <= 0 or not train_path.exists():
            return

        subset_path = train_path.with_name("train_subset.txt")
        self.logger.info("Creating Criteo subset (%s lines): %s", subset_lines, subset_path)
        with train_path.open("r", encoding="utf-8") as src, subset_path.open("w", encoding="utf-8") as dst:
            for idx, line in enumerate(src, start=1):
                dst.write(line)
                if idx >= subset_lines:
                    break

        subset_path.replace(train_path)

    def _check_disk_space_or_raise(self, path: Path) -> None:
        usage = shutil.disk_usage(path)
        free_gb = usage.free / (1024 ** 3)
        if free_gb < self.config.required_free_gb:
            raise RuntimeError(
                f"Insufficient disk space at {path}: {free_gb:.2f} GB free < {self.config.required_free_gb:.2f} GB required"
            )

    def _download_http_with_progress(self, url: str, out_path: Path) -> None:
        self.logger.info("Downloading %s -> %s", url, out_path)

        try:
            with urllib.request.urlopen(url, timeout=30) as response, out_path.open("wb") as target:
                total = int(response.headers.get("Content-Length") or 0)
                downloaded = 0
                chunk_size = 1024 * 1024
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    target.write(chunk)
                    downloaded += len(chunk)
                    self._print_progress(downloaded, total)
            if total > 0:
                sys.stdout.write("\n")
                sys.stdout.flush()
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network download failed for {url}: {exc}") from exc

    def _print_progress(self, downloaded: int, total: int) -> None:
        if total <= 0:
            sys.stdout.write(f"\rDownloaded {downloaded / (1024 ** 2):.1f} MB")
            sys.stdout.flush()
            return
        width = 30
        ratio = min(1.0, downloaded / total)
        done = int(width * ratio)
        bar = "#" * done + "-" * (width - done)
        sys.stdout.write(
            f"\r[{bar}] {ratio * 100:6.2f}% ({downloaded / (1024 ** 2):.1f}/{total / (1024 ** 2):.1f} MB)"
        )
        sys.stdout.flush()

    def _gunzip_if_needed(self, src: Path, dst: Path) -> None:
        if not src.exists() or dst.exists():
            return
        self.logger.info("Extracting %s -> %s", src, dst)
        self._run_command(["gzip", "-dc", str(src)], capture_stdout_to=dst)

    def _run_command(self, cmd: List[str], capture_stdout_to: Optional[Path] = None) -> None:
        if capture_stdout_to is None:
            completed = subprocess.run(cmd, check=False, text=True, capture_output=True)
            if completed.returncode != 0:
                raise RuntimeError(
                    f"Command failed ({' '.join(cmd)}): {completed.stderr.strip() or completed.stdout.strip()}"
                )
            return

        with capture_stdout_to.open("wb") as out:
            proc = subprocess.run(cmd, check=False, stdout=out, stderr=subprocess.PIPE)
        if proc.returncode != 0:
            stderr = proc.stderr.decode("utf-8", errors="replace") if proc.stderr else ""
            raise RuntimeError(f"Command failed ({' '.join(cmd)}): {stderr.strip()}")

    def _has_kaggle_credentials(self) -> bool:
        if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
            return True
        kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
        if kaggle_json.exists():
            try:
                data = json.loads(kaggle_json.read_text(encoding="utf-8"))
            except Exception:
                return False
            return bool(data.get("username") and data.get("key"))
        return False

    def _fast_count_lines(self, path: Path) -> int:
        count = 0
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                count += chunk.count(b"\n")
        return count


def network_available(host: str = "8.8.8.8", port: int = 53, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def build_default_manager(root_dir: Path, enable_synthetic_fallback: bool = True, criteo_subset_lines: int = 500000) -> DatasetManager:
    return DatasetManager(
        DatasetManagerConfig(
            root_dir=root_dir,
            enable_downloads=True,
            enable_synthetic_fallback=enable_synthetic_fallback,
            criteo_subset_lines=criteo_subset_lines,
        )
    )
