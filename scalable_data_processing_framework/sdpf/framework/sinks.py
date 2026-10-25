import csv
import json
from typing import List


class CSVSink:
    def __init__(self, path: str):
        self.path, self._writer, self._f = path, None, None

    def write(self, records: List[dict]):
        if not records:
            return
        if self._writer is None:
            self._f = open(self.path, "w", newline="", encoding="utf-8")
            self._writer = csv.DictWriter(self._f, fieldnames=list(records[0]))
            self._writer.writeheader()
        self._writer.writerows(records)

    def close(self):
        if self._f:
            self._f.close()


class JSONSink:
    """Writes newline-delimited JSON (one record per line)."""

    def __init__(self, path: str):
        self._f = open(path, "w", encoding="utf-8")

    def write(self, records: List[dict]):
        for r in records:
            self._f.write(json.dumps(r) + "\n")

    def close(self):
        self._f.close()
