import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# PyArrow schema enforces types at write time — mismatches throw immediately
# rather than silently coercing the way pandas dtypes sometimes do.
SCHEMA_V1 = pa.schema([
    pa.field("event_id",    pa.string()),
    pa.field("user_id",     pa.string()),
    pa.field("merchant_id", pa.string()),
    pa.field("amount",      pa.float64()),
    pa.field("currency",    pa.string()),
    pa.field("status",      pa.string()),
    pa.field("event_ts",    pa.int64()),
    pa.field("is_flagged",  pa.bool_()),
])


def write(df: pd.DataFrame, path: str = "data/events.parquet") -> dict:
    out = Path(path)

    # Convert to Arrow table with explicit schema before writing.
    # preserve_index=False prevents pandas from writing the RangeIndex
    # as a column in the file.
    table = pa.Table.from_pandas(df, schema=SCHEMA_V1, preserve_index=False)

    t0 = time.perf_counter()
    # Snappy: fast compression with moderate size reduction.
    # The alternative is 'zstd' for better compression at slightly higher CPU cost —
    # Delta Lake defaults to snappy, Iceberg defaults to zstd.
    pq.write_table(table, str(out), compression="snappy")
    write_s = time.perf_counter() - t0

    size_mb = out.stat().st_size / 1_048_576

    # Row count from metadata — reads the footer only, zero data pages touched
    t1 = time.perf_counter()
    rows = pq.read_metadata(str(out)).num_rows
    read_s = time.perf_counter() - t1

    # Column scan: THIS is the Parquet superpower.
    # columns=["amount"] tells the reader to fetch only the amount column's
    # byte range from disk. All other columns are physically skipped —
    # the OS never reads those pages. Compare this time against JSON/CSV/Avro.
    t2 = time.perf_counter()
    amount_col = pq.read_table(str(out), columns=["amount"])
    _ = amount_col.column("amount").to_pylist()
    col_scan_s = time.perf_counter() - t2

    return {
        "format":     "parquet",
        "size_mb":    round(size_mb, 2),
        "write_s":    round(write_s, 3),
        "read_s":     round(read_s, 3),
        "col_scan_s": round(col_scan_s, 3),
        "rows":       rows,
    }