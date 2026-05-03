import json
import time
from pathlib import Path

import pandas as pd


def write(df: pd.DataFrame, path: str = "data/events.json") -> dict:
    out = Path(path)
    t0 = time.perf_counter()

    # NDJSON — one JSON object per line, not a JSON array.
    # This is the format Kafka Connect, Fivetran, and most REST ingestion
    # pipelines emit. Each line is independently parseable.
    with out.open("w") as f:
        for row in df.itertuples(index=False):
            record = {
                "event_id":    row.event_id,
                "user_id":     row.user_id,
                "merchant_id": row.merchant_id,
                "amount":      row.amount,
                "currency":    row.currency,
                "status":      row.status,
                "event_ts":    row.event_ts,
                "is_flagged":  row.is_flagged,
            }
            f.write(json.dumps(record) + "\n")

    write_s = time.perf_counter() - t0
    size_mb = out.stat().st_size / 1_048_576

    # Count rows by counting lines — no deserialisation needed
    t1 = time.perf_counter()
    with out.open() as f:
        rows = sum(1 for _ in f)
    read_s = time.perf_counter() - t1

    # Column scan: to sum 'amount' we must deserialise every line in full.
    # JSON has no concept of skipping to a specific field on disk —
    # every byte must be read and parsed regardless of how many columns you need.
    t2 = time.perf_counter()
    total = 0.0
    with out.open() as f:
        for line in f:
            total += json.loads(line)["amount"]
    col_scan_s = time.perf_counter() - t2

    return {
        "format":     "json",
        "size_mb":    round(size_mb, 2),
        "write_s":    round(write_s, 3),
        "read_s":     round(read_s, 3),
        "col_scan_s": round(col_scan_s, 3),
        "rows":       rows,
    }