# Project — Data Serialization

A hands-on benchmark comparing JSON, CSV, Avro, and Parquet across
file size, write throughput, read throughput, column scan performance,
and schema evolution behaviour.

No containers required. Pure Python, runs entirely on local.

## What this project answers

- Why does every production data stack default to Parquet?
- Why does Kafka use Avro and not Parquet?
- What actually breaks when a schema changes in each format?
- What is the real file size and read speed cost of human-readable formats at scale?

## Stack

| Tool | Role |
|---|---|
| Python 3.11 | Runtime |
| pandas | In-memory DataFrame |
| fastavro | Avro serialisation / deserialisation |
| pyarrow | Parquet serialisation / deserialisation |
| tabulate | Benchmark output formatting |

## Project structure
01-data-serialization/
├── generate.py        # Synthetic payment event generator (seeded, reproducible)
├── benchmark.py       # Runs all four writers, prints comparison table
├── evolve.py          # Schema evolution experiments across all four formats
├── serialize/
│   ├── json_writer.py
│   ├── csv_writer.py
│   ├── avro_writer.py
│   └── parquet_writer.py
└── data/              # Generated output — gitignored

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python generate.py      # writes data/events_v1.pkl  (~50k rows)
python benchmark.py     # runs all four format writers
python evolve.py        # schema evolution experiments
```

## Benchmark results (50k rows, Apple M2)

| Format  | Size (MB) | vs Parquet | Write (s) | Read (s) | Col scan (s) |
|---------|-----------|------------|-----------|----------|--------------|
| JSON    | 10.1      | 3.7x       | 0.918     | 0.018    | 0.302        |
| CSV     | 4.7       | 1.7x       | 0.815     | 0.012    | 0.196        |
| Avro    | 4.1       | 1.5x       | 0.257     | 0.230    | 0.242        |
| Parquet | 2.7       | 1.0x       | 0.091     | 0.003    | 0.088        |

Col scan = `sum(amount)` reading only the amount column where possible.

## Schema evolution results

| Experiment | JSON | CSV | Avro | Parquet |
|---|---|---|---|---|
| Add nullable column | Silent None | Silent None | Default fill via schema | Null fill via schema |
| Remove column | Ignores extra fields | Ignores extra fields | Projects out cleanly | Projects out cleanly |
| Rename column | KeyError | KeyError | Resolved via aliases | Manual rename at read time |
| Change type long → string | N/A | Silent — all strings | SchemaResolutionError | Coerced |

## Key findings

**On file size:** JSON is 3.7× larger than Parquet. The bloat is structural —
every row repeats all 8 field names as strings. CSV improves on this by moving
field names to a single header row but every value is still untyped text.
Parquet's dictionary encoding collapses high-repetition columns like `currency`
(5 distinct values across 50k rows) to near-zero additional cost.

**On write speed:** Human-readable formats are the slowest to write. JSON and CSV
rely on row-by-row Python loops. Avro and Parquet use C-backed libraries that
process data in bulk, which is why Parquet at 0.091s outpaces JSON at 0.918s
by 10×.

**On column scan:** Parquet is 3.4× faster than the next best format for a single
column read. JSON, CSV, and Avro must deserialise every row to reach one field.
Parquet reads only the target column's byte range — all other columns are skipped
at the OS level. This gap widens with scale.

**On schema evolution:** CSV is the most dangerous format in production — a type
change from `long` to `string` passes silently with no error. Avro throws
immediately. The Avro alias mechanism for column renames has no equivalent in
Parquet, which requires manual handling at read time or a full rewrite job.

**The core rule:** Avro for streaming (row-oriented, schema-enforced, self-describing
per message). Parquet for storage (columnar, compressed, optimised for analytical
column scans). Never CSV or JSON at scale.