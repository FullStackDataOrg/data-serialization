import time
from pathlib import Path

import fastavro
import pandas as pd


# Schema lives with the writer — in a real Kafka pipeline this would be
# registered in Confluent Schema Registry and referenced by ID in the
# 5-byte message header. Here we embed it in the file header instead,
# which is how Avro container files work outside of streaming.
SCHEMA_V1 = {
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


def write(df: pd.DataFrame, path: str = "data/events.avro") -> dict:
    out = Path(path)
    parsed_schema = fastavro.parse_schema(SCHEMA_V1)

    # fastavro expects a list of dicts — one dict per record
    records = df.to_dict(orient="records")

    t0 = time.perf_counter()
    with out.open("wb") as f:
        fastavro.writer(f, parsed_schema, records)
    write_s = time.perf_counter() - t0

    size_mb = out.stat().st_size / 1_048_576

    # Read: Avro is row-oriented so the reader deserialises every record.
    # The schema is read from the file header automatically — no need to
    # pass it to the reader unless you want schema evolution (see evolve.py).
    t1 = time.perf_counter()
    with out.open("rb") as f:
        rows = sum(1 for _ in fastavro.reader(f))
    read_s = time.perf_counter() - t1

    # Column scan: like JSON and CSV, Avro must deserialise every record
    # to extract one field. Row-oriented = no column skipping on disk.
    t2 = time.perf_counter()
    total = 0.0
    with out.open("rb") as f:
        for record in fastavro.reader(f):
            total += record["amount"]
    col_scan_s = time.perf_counter() - t2

    return {
        "format":     "avro",
        "size_mb":    round(size_mb, 2),
        "write_s":    round(write_s, 3),
        "read_s":     round(read_s, 3),
        "col_scan_s": round(col_scan_s, 3),
        "rows":       rows,
    }