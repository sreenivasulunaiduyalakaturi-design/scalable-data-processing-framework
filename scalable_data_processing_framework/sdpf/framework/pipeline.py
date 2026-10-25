"""A pipeline is an ordered list of record-level steps applied to a chunk.

Steps must be top-level (picklable) functions so they can be shipped to
worker processes.
  - map step    : record -> record | None   (None drops the record)
  - filter step : record -> bool
"""
from typing import Callable, Dict, List, Tuple

Record = Dict[str, object]


class Pipeline:
    def __init__(self):
        self._steps: List[Tuple[str, Callable]] = []

    def map(self, fn: Callable[[Record], Record]) -> "Pipeline":
        self._steps.append(("map", fn))
        return self

    def filter(self, fn: Callable[[Record], bool]) -> "Pipeline":
        self._steps.append(("filter", fn))
        return self

    def run_chunk(self, chunk: List[Record]) -> List[Record]:
        out = []
        for rec in chunk:
            keep = True
            for kind, fn in self._steps:
                if kind == "map":
                    rec = fn(rec)
                    if rec is None:
                        keep = False
                        break
                elif not fn(rec):
                    keep = False
                    break
            if keep:
                out.append(rec)
        return out
