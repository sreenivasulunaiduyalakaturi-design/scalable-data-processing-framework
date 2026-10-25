"""Parallel execution engine.

- Chunks are fanned out to a process pool (true CPU parallelism).
- At most `max_in_flight` chunks are pending at once (backpressure), so
  memory stays bounded even for huge inputs.
- Failed chunks are retried; results are written/merged as they complete.
"""
import logging
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass
from os import cpu_count

log = logging.getLogger("sdpf")


@dataclass
class RunStats:
    chunks: int = 0
    records_in: int = 0
    records_out: int = 0
    retries: int = 0
    failed_chunks: int = 0
    seconds: float = 0.0

    @property
    def throughput(self):
        return self.records_in / self.seconds if self.seconds else 0.0


def _worker(pipeline, aggregator, chunk):
    out = pipeline.run_chunk(chunk)
    partial = aggregator.map_chunk(out) if aggregator else None
    return len(chunk), out, partial


class ParallelEngine:
    def __init__(self, workers=None, max_in_flight=None, retries=2):
        self.workers = workers or cpu_count() or 2
        self.max_in_flight = max_in_flight or self.workers * 2
        self.retries = retries

    def run(self, source, pipeline, sink=None, aggregator=None):
        stats, start = RunStats(), time.perf_counter()
        result = aggregator.initial() if aggregator else None
        pending = {}  # future -> (chunk, attempt)

        def submit(pool, chunk, attempt=0):
            pending[pool.submit(_worker, pipeline, aggregator, chunk)] = (chunk, attempt)

        with ProcessPoolExecutor(max_workers=self.workers) as pool:
            for chunk in source.chunks():
                while len(pending) >= self.max_in_flight:      # backpressure
                    result = self._drain(pool, pending, submit, sink, aggregator, result, stats)
                submit(pool, chunk)
                stats.chunks += 1
            while pending:
                result = self._drain(pool, pending, submit, sink, aggregator, result, stats)

        if sink:
            sink.close()
        stats.seconds = time.perf_counter() - start
        log.info("done: %s", stats)
        return result, stats

    def _drain(self, pool, pending, submit, sink, aggregator, result, stats):
        done, _ = wait(pending, return_when=FIRST_COMPLETED)
        for fut in done:
            chunk, attempt = pending.pop(fut)
            try:
                n_in, out, partial = fut.result()
            except Exception as exc:
                if attempt < self.retries:
                    stats.retries += 1
                    log.warning("chunk failed (%s), retry %d", exc, attempt + 1)
                    submit(pool, chunk, attempt + 1)
                else:
                    stats.failed_chunks += 1
                    log.error("chunk permanently failed: %s", exc)
                continue
            stats.records_in += n_in
            stats.records_out += len(out)
            if sink:
                sink.write(out)
            if aggregator:
                result = aggregator.merge(result, partial)
        return result
