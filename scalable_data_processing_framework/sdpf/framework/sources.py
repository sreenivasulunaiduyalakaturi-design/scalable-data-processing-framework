"""Sources yield *chunks* (lists of records) so memory use stays bounded."""
import csv
from itertools import islice
from typing import Iterable, Iterator, List


class IterableSource:
    def __init__(self, iterable: Iterable[dict], chunk_size: int = 1000):
        self.iterable, self.chunk_size = iterable, chunk_size

    def chunks(self) -> Iterator[List[dict]]:
        it = iter(self.iterable)
        while chunk := list(islice(it, self.chunk_size)):
            yield chunk


class CSVSource:
    def __init__(self, path: str, chunk_size: int = 10_000):
        self.path, self.chunk_size = path, chunk_size

    def chunks(self) -> Iterator[List[dict]]:
        with open(self.path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            while chunk := list(islice(reader, self.chunk_size)):
                yield chunk
