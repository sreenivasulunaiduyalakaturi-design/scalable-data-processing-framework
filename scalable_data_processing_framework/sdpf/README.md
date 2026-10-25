# Scalable Data Processing Framework

A small, dependency-free (stdlib only) Python framework for chunked, parallel data processing.

## Architecture
Source -> chunks -> [ProcessPool: Pipeline steps + partial aggregate] -> Sink + merged Aggregator

- **Sources** stream data in chunks (bounded memory).
- **Pipeline** chains `map` / `filter` steps.
- **ParallelEngine** uses a process pool, backpressure (`max_in_flight`) and chunk retries.
- **Aggregator** = map/reduce: partial results per chunk, merged in the parent.
- **Sinks** write results incrementally (CSV / NDJSON).

## Run
    python main.py --rows 500000 --workers 4
    python -m unittest discover tests

## Extend
Add a new Source (implement `chunks()`), Sink (`write`, `close`) or Aggregator (`map_chunk`, `merge`).
Scaling out further: swap `ProcessPoolExecutor` in `engine.py` for Celery/Ray/Dask workers.
