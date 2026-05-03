import json
import csv
import time
from pathlib import Path

import fastavro
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


Path("data").mkdir(exist_ok=True)

# Small sample — schema evolution behaviour is identical at 100 rows or 1M.
# No point waiting for a large write during these experiments.
SAMPLE = [
    {
        "event_id":    f"evt-{i:04d}",
        "user_id":     f"usr-{i % 100:05d}",
        "merchant_id": f"mer-{i % 20:05d}",
        "amount":      round(i * 1.99, 2),
        "currency":    "USD",
        "status":      "approved",
        "event_ts":    1_700_000_000_000 + (i * 1000),
        "is_flagged":  i % 33 == 0,
    }
    for i in range(100)
]

# ── Shared V1 schemas ────────────────────────────────────────────────────────

AVRO_SCHEMA_V1 = {
    "type": "record",
    "name": "PaymentEvent",
    "namespace": "com.fullstackdata",
    "fields": [
        {"name": "event_id",    "type": "string"},
        {"name": "user_id",     "type": "string"},
        {"name": "merchant_id", "type": "string"},
        {"name": "amount",      "type": "double"},
        {"name": "currency",    "type": "string"},
        {"name": "status",      "type": "string"},
        {"name": "event_ts",    "type": "long"},
        {"name": "is_flagged",  "type": "boolean"},
    ],
}

PARQUET_SCHEMA_V1 = pa.schema([
    pa.field("event_id",    pa.string()),
    pa.field("user_id",     pa.string()),
    pa.field("merchant_id", pa.string()),
    pa.field("amount",      pa.float64()),
    pa.field("currency",    pa.string()),
    pa.field("status",      pa.string()),
    pa.field("event_ts",    pa.int64()),
    pa.field("is_flagged",  pa.bool_()),
])


# ── Helpers ──────────────────────────────────────────────────────────────────

def header(title: str) -> None:
    print(f"\n{'─' * 62}")
    print(f"  {title}")
    print(f"{'─' * 62}")


def outcome(fmt: str, ok: bool, message: str) -> None:
    tag = "OK " if ok else "ERR"
    print(f"  [{tag}] {fmt:<10} {message}")


def write_v1_files() -> None:
    # JSON
    with Path("data/evolve.json").open("w") as f:
        for r in SAMPLE:
            f.write(json.dumps(r) + "\n")

    # CSV
    with Path("data/evolve.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(SAMPLE[0].keys()))
        w.writeheader()
        w.writerows(SAMPLE)

    # Avro
    parsed = fastavro.parse_schema(AVRO_SCHEMA_V1)
    with Path("data/evolve.avro").open("wb") as f:
        fastavro.writer(f, parsed, SAMPLE)

    # Parquet
    df = pd.DataFrame(SAMPLE)
    table = pa.Table.from_pandas(df, schema=PARQUET_SCHEMA_V1, preserve_index=False)
    pq.write_table(table, "data/evolve.parquet")


# ── Experiment 1: Add a nullable column ─────────────────────────────────────

def exp_add_column() -> None:
    header("Experiment 1 — Add a nullable column (V1 written, V2 reader expects 'region')")

    # JSON: .get() returns None for missing keys — no error, but also no signal
    try:
        with Path("data/evolve.json").open() as f:
            values = [json.loads(line).get("region", None) for line in f]
        outcome("JSON", True, f"'region' is None for all {len(values)} rows — silent, no default enforcement")
    except Exception as e:
        outcome("JSON", False, str(e))

    # CSV: missing column simply absent from DictReader — same silent behaviour
    try:
        with Path("data/evolve.csv").open() as f:
            rows = list(csv.DictReader(f))
        sample = rows[0].get("region", "NOT PRESENT")
        outcome("CSV", True, f"'region' key absent — .get() returns '{sample}', no error raised")
    except Exception as e:
        outcome("CSV", False, str(e))

    # Avro: union type ["null", "string"] with default null fills missing field cleanly
    schema_v2 = {
        **AVRO_SCHEMA_V1,
        "fields": AVRO_SCHEMA_V1["fields"] + [
            {"name": "region", "type": ["null", "string"], "default": None}
        ],
    }
    try:
        parsed_v2 = fastavro.parse_schema(schema_v2)
        with Path("data/evolve.avro").open("rb") as f:
            rows = list(fastavro.reader(f, reader_schema=parsed_v2))
        outcome("Avro", True, f"'region' filled with None for all {len(rows)} rows via schema default")
    except Exception as e:
        outcome("Avro", False, str(e))

    # Parquet: appending a nullable field to the read schema fills with nulls
    schema_v2_pq = PARQUET_SCHEMA_V1.append(pa.field("region", pa.string()))
    try:
        table = pq.read_table("data/evolve.parquet", schema=schema_v2_pq)
        null_count = table.column("region").null_count
        outcome("Parquet", True, f"'region' column present with {null_count} nulls")
    except Exception as e:
        outcome("Parquet", False, str(e))


# ── Experiment 2: Remove a column ───────────────────────────────────────────

def exp_remove_column() -> None:
    header("Experiment 2 — Remove a column (V2 reader drops 'currency')")

    # JSON: no schema enforcement — extra fields are simply ignored
    try:
        with Path("data/evolve.json").open() as f:
            rows = [json.loads(line) for line in f]
        has = "currency" in rows[0]
        outcome("JSON", True, f"'currency' still present in file — reader just ignores unwanted fields. Present: {has}")
    except Exception as e:
        outcome("JSON", False, str(e))

    # Avro: reader schema omits 'currency' — fastavro projects it out
    schema_v2 = {
        **AVRO_SCHEMA_V1,
        "fields": [f for f in AVRO_SCHEMA_V1["fields"] if f["name"] != "currency"],
    }
    try:
        parsed_v2 = fastavro.parse_schema(schema_v2)
        with Path("data/evolve.avro").open("rb") as f:
            rows = list(fastavro.reader(f, reader_schema=parsed_v2))
        outcome("Avro", True, f"'currency' projected out. Present in record: {'currency' in rows[0]}")
    except Exception as e:
        outcome("Avro", False, str(e))

    # Parquet: columns= at read time acts as projection — only named columns are fetched
    remaining = [f.name for f in PARQUET_SCHEMA_V1 if f.name != "currency"]
    try:
        table = pq.read_table("data/evolve.parquet", columns=remaining)
        outcome("Parquet", True, f"Projected {table.num_columns} columns. 'currency' present: {'currency' in table.schema.names}")
    except Exception as e:
        outcome("Parquet", False, str(e))


# ── Experiment 3: Rename a column ───────────────────────────────────────────

def exp_rename_column() -> None:
    header("Experiment 3 — Rename 'amount' → 'transaction_amount' (the hardest evolution)")

    # JSON: no mechanism for aliases — KeyError on direct access
    try:
        with Path("data/evolve.json").open() as f:
            first = json.loads(f.readline())
        val = first["transaction_amount"]
        outcome("JSON", True, str(val))
    except KeyError:
        outcome("JSON", False, "KeyError: 'transaction_amount' — old key 'amount' still in file, no alias support")

    # Avro: aliases field lets the reader resolve old name → new name transparently
    schema_v2 = {
        **AVRO_SCHEMA_V1,
        "fields": [
            {**f, "name": "transaction_amount", "aliases": ["amount"]}
            if f["name"] == "amount" else f
            for f in AVRO_SCHEMA_V1["fields"]
        ],
    }
    try:
        parsed_v2 = fastavro.parse_schema(schema_v2)
        with Path("data/evolve.avro").open("rb") as f:
            rows = list(fastavro.reader(f, reader_schema=parsed_v2))
        outcome("Avro", True, f"Alias resolved. 'transaction_amount' value: {rows[0].get('transaction_amount')}")
    except Exception as e:
        outcome("Avro", False, str(e))

    # Parquet: no alias support in the format itself — must rename manually at read time
    try:
        table = pq.read_table("data/evolve.parquet")
        renamed = table.rename_columns(
            ["transaction_amount" if f.name == "amount" else f.name for f in table.schema]
        )
        present = "transaction_amount" in renamed.schema.names
        outcome("Parquet", True, f"Manual rename at read time. 'transaction_amount' present: {present}")
    except Exception as e:
        outcome("Parquet", False, str(e))


# ── Experiment 4: Change column type ────────────────────────────────────────

def exp_change_type() -> None:
    header("Experiment 4 — Change 'event_ts' type: long → string")

    # CSV: every value is already a string — type changes are invisible and silent
    try:
        with Path("data/evolve.csv").open() as f:
            first = next(csv.DictReader(f))
        outcome("CSV", True, f"Silent — everything is a string already. Value: '{first['event_ts']}'")
    except Exception as e:
        outcome("CSV", False, str(e))

    # Avro: long → string is not a promotable type — schema resolution fails immediately
    schema_v2 = {
        **AVRO_SCHEMA_V1,
        "fields": [
            {**f, "type": "string"} if f["name"] == "event_ts" else f
            for f in AVRO_SCHEMA_V1["fields"]
        ],
    }
    try:
        parsed_v2 = fastavro.parse_schema(schema_v2)
        with Path("data/evolve.avro").open("rb") as f:
            rows = list(fastavro.reader(f, reader_schema=parsed_v2))
        outcome("Avro", True, f"Coerced — value: {rows[0]['event_ts']}")
    except Exception as e:
        outcome("Avro", False, f"{type(e).__name__} — {str(e)[:70]}")

    # Parquet: passing a mismatched schema raises ArrowInvalid at read time
    schema_v2_pq = pa.schema([
        pa.field("event_id",    pa.string()),
        pa.field("user_id",     pa.string()),
        pa.field("merchant_id", pa.string()),
        pa.field("amount",      pa.float64()),
        pa.field("currency",    pa.string()),
        pa.field("status",      pa.string()),
        pa.field("event_ts",    pa.string()),  # was int64
        pa.field("is_flagged",  pa.bool_()),
    ])
    try:
        table = pq.read_table("data/evolve.parquet", schema=schema_v2_pq)
        outcome("Parquet", True, "Coerced")
    except Exception as e:
        outcome("Parquet", False, f"{type(e).__name__} — {str(e)[:70]}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("\n  Schema Evolution Experiments")
    print("  Write V1 data, attempt to read with a mutated V2 schema.\n")

    write_v1_files()
    print("  V1 files written for all four formats.\n")

    exp_add_column()
    exp_remove_column()
    exp_rename_column()
    exp_change_type()

    print(f"\n{'─' * 62}")
    print("  Key takeaways:")
    print("  Add column    → Avro and Parquet fill nulls. CSV/JSON are silent.")
    print("  Remove column → All formats survive with projection.")
    print("  Rename column → Avro has aliases. Parquet needs manual handling.")
    print("  Change type   → Avro and Parquet enforce and throw. CSV is silent.")
    print(f"{'─' * 62}\n")


if __name__ == "__main__":
    main()