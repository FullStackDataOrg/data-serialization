# Teardown — Project 01: Data Serialization

A post-mortem on decisions made, things that broke, and what this
project clarified that documentation alone could not.

## What I built

A benchmark harness that serialises a synthetic 50k-row payment event
dataset into four formats and measures file size, write time, read time,
and single-column scan time. A separate script tests schema evolution
behaviour by writing V1 data and attempting to read it with a mutated
V2 reader schema across all four formats.

## Decisions made during the build

**Seeded random instance over global random state.**
Used `random.Random(seed)` rather than `random.seed()` at module level.
`random.seed()` returns None and mutates global state — any other module
importing random would share and potentially corrupt the sequence.
An isolated instance is deterministic, portable, and has no side effects.

**UUID pools for user_id and merchant_id.**
Generated 10k user IDs and 500 merchant IDs upfront, then sampled from
those pools with `rng.choices()`. Generating a fresh UUID per row would
produce 50k unique actors — unrealistic for payment data and it would
eliminate the cardinality patterns needed for the skewness project later.

**Pickle as the intermediate format.**
`generate.py` writes a pickle, not a CSV or JSON. The benchmark scripts
load from pickle so the in-memory DataFrame is identical across all four
writer runs. If generate wrote to CSV, loading it back would silently
lose type information — booleans become strings, int64 timestamps become
objects — and the Avro and Parquet schema enforcement tests would fail
at write time rather than at the schema evolution stage where they belong.

**NDJSON over JSON array.**
Each line is a self-contained JSON object rather than wrapping everything
in a `[...]` array. NDJSON is what real ingestion systems emit — Kafka
Connect, Fivetran, and most REST pipelines produce one object per line.
A JSON array requires loading the entire file into memory before parsing
the first record.

## What broke during the build

**`random.seed()` returns None.**
First draft assigned `rd_seed = rd.seed(42)` and then called
`rd_seed.choices(...)`. This throws `AttributeError: 'NoneType'...`
immediately. The fix was switching to `random.Random(seed)` which
returns a usable instance.

**`range()` does not accept floats.**
`rng.choices(range(0.01, 5000.99))` throws `TypeError`. `range()` is
integer-only. Replaced with a list comprehension using `rng.uniform()`.

**DataFrame construction was transposed.**
Passing a list of lists to `pd.DataFrame()` produces a DataFrame where
each list becomes a row, not a column. The shape was 8 rows × 50k columns.
Fixed by passing a dict with column names as keys.

**`event_ts` was a scalar, not a list.**
`event_ts = now_ms - rng.randint(0, span_ms)` generates one integer.
Every row got the same timestamp. Fixed with a list comprehension.

## What schema evolution clarified

The most important finding was not in the benchmark table — it was
Experiment 4 in `evolve.py`.

A type change from `long` to `string` on `event_ts` passes silently
through CSV. The value comes back as the string `'1700000000000'` with
no error, no warning, and no indication anything is wrong. Any downstream
aggregation or arithmetic on that column would either throw a late-binding
TypeError or produce silently wrong results.

Avro throws `SchemaResolutionError` at read time. Hard stop. Nothing
downstream receives corrupted data.

This is the production argument for schema-enforced binary formats — not
performance, but failure visibility. A pipeline that fails loudly on
schema mismatch is recoverable. A pipeline that silently propagates the
wrong type is not.

The rename experiment (Experiment 3) clarified a practical gap between
Avro and Parquet. Avro's `aliases` field lets a V2 reader transparently
resolve `transaction_amount` from files that still contain `amount`.
Parquet has no equivalent — renaming requires either a full rewrite of
historical files or manual column renaming in every reader, with no
central contract enforcing consistency.

## What I would do differently

Run the benchmark at multiple row counts — 10k, 50k, 500k — and plot
the scaling behaviour. The col scan gap between Parquet and the row-based
formats should widen non-linearly with scale because Parquet's advantage
is I/O-bound and I/O savings compound. A table of numbers at one row count
does not show whether the advantage is linear or exponential.

Add Protobuf as a fifth format. The current four split cleanly into two
tiers — human-readable and binary — but Protobuf occupies a distinct
third position: binary, schema-enforced, but without the columnar storage
advantage of Parquet and without the streaming self-description of Avro.
Understanding where Protobuf sits relative to Avro in a Kafka context
would complete the picture.