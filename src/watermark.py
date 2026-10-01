"""High-watermark tracking for incremental extraction."""
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

EPOCH = "1900-01-01T00:00:00"


class LocalWatermarkStore:
    """File-based store; swap for an S3/DynamoDB implementation in AWS."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def _file(self, table: str) -> Path:
        return self.path / f"{table}.json"

    def get(self, table: str) -> str:
        f = self._file(table)
        if not f.exists():
            return EPOCH
        return json.loads(f.read_text())["watermark"]

    def set(self, table: str, value: str) -> None:
        self._file(table).write_text(json.dumps({"watermark": value, "saved_at": datetime.utcnow().isoformat()}))


def next_watermark(current: str, batch_max: Optional[str]) -> str:
    """Only move forward; an empty batch keeps the current watermark."""
    if batch_max is None:
        return current
    return max(current, batch_max)


def incremental_query(source: str, column: str, watermark: str) -> str:
    return f"(SELECT * FROM {source} WHERE {column} > '{watermark}') AS src"
