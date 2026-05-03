import sys
import time
from pathlib import Path

import pandas as pd
from tabulate import tabulate

from serialize import json_writer, csv_writer, avro_writer, parquet_writer


def load_data() -> pd.DataFrame:
    pkl = Path("data/events_v1.pkl")

    if not pkl.exists():
        print("data/events_v1.pkl not found — run `python generate.py` first.")
        sys.exit(1)

    df = pd.read_pickle(pkl)
    print(f"Loaded {len(df):,} rows from {pkl}\n")
    return df


def run(df: pd.DataFrame) -> list[dict]:
    writers = [
        (json_writer,    "data/events.json"),
        (csv_writer,     "data/events.csv"),
        (avro_writer,    "data/events.avro"),
        (parquet_writer, "data/events.parquet"),
    ]

    results = []
    for module, path in writers:
        fmt = module.__name__.split(".")[-1].replace("_writer", "").upper()
        print(f"  Running {fmt} ...", end="", flush=True)
        t0 = time.perf_counter()
        result = module.write(df, path=path)
        elapsed = time.perf_counter() - t0
        print(f" done in {elapsed:.1f}s")
        results.append(result)

    return results


def print_table(results: list[dict], n_rows: int) -> None:
    # Use Parquet as the size baseline since it is the most compact
    parquet_size = next(r["size_mb"] for r in results if r["format"] == "parquet")

    headers = ["Format", "Size (MB)", "vs Parquet", "Write (s)", "Read (s)", "Col scan (s)"]
    rows = []
    for r in results:
        ratio = f"{r['size_mb'] / parquet_size:.1f}x"
        rows.append([
            r["format"].upper(),
            f"{r['size_mb']:.1f}",
            ratio,
            f"{r['write_s']:.3f}",
            f"{r['read_s']:.3f}",
            f"{r['col_scan_s']:.3f}",
        ])

    print(f"\n{'=' * 60}")
    print(f"  Results — {n_rows:,} rows")
    print(f"{'=' * 60}")
    print(tabulate(rows, headers=headers, tablefmt="rounded_outline"))
    print()
    print("  Col scan = sum(amount) touching only the amount column where possible.")
    print("  JSON / CSV / Avro must deserialise every row to reach one field.")
    print("  Parquet skips all other columns at the OS level.\n")


def main():
    df = load_data()
    results = run(df)
    print_table(results, n_rows=len(df))


if __name__ == "__main__":
    main()