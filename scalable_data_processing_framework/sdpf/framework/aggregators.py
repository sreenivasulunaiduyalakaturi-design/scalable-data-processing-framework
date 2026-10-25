"""Map/reduce style aggregation: `map_chunk` runs in workers, `merge` in parent."""
from collections import defaultdict


class Aggregator:
    def map_chunk(self, records):
        raise NotImplementedError

    def merge(self, a, b):
        raise NotImplementedError

    def initial(self):
        return None


class GroupBySum(Aggregator):
    """Sum `value_key` grouped by `group_key`, also counting rows."""

    def __init__(self, group_key: str, value_key: str):
        self.group_key, self.value_key = group_key, value_key

    def initial(self):
        return {}

    def map_chunk(self, records):
        acc = defaultdict(lambda: {"sum": 0.0, "count": 0})
        for r in records:
            g = acc[r[self.group_key]]
            g["sum"] += float(r[self.value_key])
            g["count"] += 1
        return dict(acc)

    def merge(self, a, b):
        for k, v in b.items():
            cur = a.setdefault(k, {"sum": 0.0, "count": 0})
            cur["sum"] += v["sum"]
            cur["count"] += v["count"]
        return a
