import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import unittest
from framework import GroupBySum, IterableSource, ParallelEngine, Pipeline


def double(r):
    r["v"] *= 2
    return r

def keep_even_id(r):
    return r["id"] % 2 == 0


class EngineTests(unittest.TestCase):
    def test_parallel_matches_serial(self):
        data = [{"id": i, "g": i % 3, "v": i} for i in range(1000)]
        p = Pipeline().map(double).filter(keep_even_id)
        agg = GroupBySum("g", "v")
        res, stats = ParallelEngine(workers=2).run(
            IterableSource(data, chunk_size=64), p, aggregator=agg)
        expected = {}
        for r in data:
            if r["id"] % 2 == 0:
                e = expected.setdefault(r["id"] % 3, {"sum": 0.0, "count": 0})
                e["sum"] += r["id"] * 2; e["count"] += 1
        self.assertEqual(res, expected)
        self.assertEqual(stats.records_in, 1000)
        self.assertEqual(stats.records_out, 500)


if __name__ == "__main__":
    unittest.main()
