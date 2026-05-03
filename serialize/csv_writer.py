import csv
import time
from pathlib import Path

import pandas as pd


FIELDNAMES = [
    "event_id", "user_id", "merchant_id", "amount",
    "currency", "status", "event_ts", "is_flagged",
]


def write(df: pd.DataFrame, path: str = "data/events.csv") -> dict:
    out = Path(path)
    t0 = time.perf_counter()

    with out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in df.itertuples(index=False):
            writer.writerow({
                "event_id":    row.event_id,
                "user_id":     row.user_id,
                "merchant_id": row.merchant_id,
                "amount":      row.amount,
                "currency":    row.currency,
                "status":      row.status,
                "event_ts":    row.event_ts,
                "is_flagged":  row.is_flagged,
            })

    write_s = time.perf_counter() - t0
    size_mb = out.stat().st_size / 1_048_576

    # Subtract 1 for the header row
    t1 = time.perf_counter()
    with out.open() as f:
        rows = sum(1 for _ in f) - 1
    read_s = time.perf_counter() - t1

    # Column scan: CSV has no column skipping — every row must be fully
    # parsed to reach 'amount', even though we only need one field.
    # Also note: every value comes back as a string, so float() is required.
    # This is the silent type-loss problem CSV introduces.
    t2 = time.perf_counter()
    total = 0.0
    with out.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += float(row["amount"])
    col_scan_s = time.perf_counter() - t2

    return {
        "format":     "csv",
        "size_mb":    round(size_mb, 2),
        "write_s":    round(write_s, 3),
        "read_s":     round(read_s, 3),
        "col_scan_s": round(col_scan_s, 3),
        "rows":       rows,
    }