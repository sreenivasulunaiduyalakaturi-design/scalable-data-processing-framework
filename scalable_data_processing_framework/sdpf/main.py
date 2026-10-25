"""Demo: process a synthetic sales CSV in parallel.

Usage: python main.py [--rows 500000] [--workers 4]
"""
import argparse
import csv
import logging
import random

from framework import CSVSink, CSVSource, GroupBySum, ParallelEngine, Pipeline


# Pipeline steps must be top-level functions (picklable).
def clean(r):
    try:
        r["amount"] = round(float(r["amount"]), 2)
    except ValueError:
        return None                      # drop malformed rows
    r["category"] = r["category"].strip().lower()
    return r


def positive_only(r):
    return r["amount"] > 0


def add_tax(r):
    r["amount_with_tax"] = round(r["amount"] * 1.18, 2)
    return r


def generate(path, rows):
    cats = ["Books", " electronics", "Toys", "GROCERY", "Fashion"]
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "category", "amount"])
        for i in range(rows):
            amt = random.choice(["bad", -5]) if random.random() < 0.02 else round(random.uniform(1, 500), 2)
            w.writerow([i, random.choice(cats), amt])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=200_000)
    ap.add_argument("--workers", type=int, default=None)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    generate("sales.csv", args.rows)
    pipeline = Pipeline().map(clean).filter(positive_only).map(add_tax)
    engine = ParallelEngine(workers=args.workers)
    totals, stats = engine.run(
        CSVSource("sales.csv", chunk_size=10_000),
        pipeline,
        sink=CSVSink("sales_clean.csv"),
        aggregator=GroupBySum("category", "amount"),
    )
    print(f"\nRecords in: {stats.records_in}, out: {stats.records_out}, "
          f"time: {stats.seconds:.2f}s, {stats.throughput:,.0f} rec/s")
    for cat, v in sorted(totals.items()):
        print(f"{cat:12s} count={v['count']:7d} total={v['sum']:,.2f}")
