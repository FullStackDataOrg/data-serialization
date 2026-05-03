import argparse
import uuid
import time
import random
from pathlib import Path

import pandas as pd


CURRENCIES = ['USD', 'CAD', 'GBP', 'EUR', 'NGN']

# Weighted so approved dominates, matching real payment data distributions
STATUSES         = ['approved', 'declined', 'pending']
STATUSES_WEIGHTS = [85, 10, 5]


def generate(n_rows: int, seed: int = 42) -> pd.DataFrame:
    # Isolated seeded instance — does not touch global random state,
    # guaranteeing identical output across machines and runs
    rng = random.Random(seed)

    # Build ID pools once upfront so values repeat across rows.
    # Real payment data has ~10k users and ~500 merchants transacting repeatedly,
    # not a unique actor per event. This also controls cardinality for later
    # skewness experiments in project 2.
    users     = [f"usr-{i:05d}" for i in range(1, 10_001)]
    merchants = [f"mer-{i:05d}" for i in range(1, 501)]

    # Capture now inside the function so the timestamp is fresh on each call,
    # not frozen at import time
    now_ms  = int(time.time() * 1000)
    span_ms = 90 * 24 * 60 * 60 * 1000  # 90 days in milliseconds

    data = {
        # uuid4 is random by definition — no need to involve rng here.
        # str() ensures clean serialisation in Avro and Parquet later.
        "event_id":    [str(uuid.uuid4()) for _ in range(n_rows)],

        # Sample from pools — produces realistic repeat-customer behaviour
        "user_id":     rng.choices(users, k=n_rows),
        "merchant_id": rng.choices(merchants, k=n_rows),

        # uniform() generates a float in [a, b] — range() only accepts integers
        "amount":      [round(rng.uniform(0.01, 9999.99), 2) for _ in range(n_rows)],

        "currency":    rng.choices(CURRENCIES, k=n_rows),

        # weights= keeps the list clean while controlling distribution
        "status":      rng.choices(STATUSES, weights=STATUSES_WEIGHTS, k=n_rows),

        # Each event gets its own timestamp — a single randint() would give
        # every row the same value
        "event_ts":    [now_ms - rng.randint(0, span_ms) for _ in range(n_rows)],

        # 3% flagged — weights=[3, 97] is clearer than repeating values in the list
        "is_flagged":  rng.choices([True, False], weights=[3, 97], k=n_rows),
    }

    return pd.DataFrame(data)


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic payment event data")
    parser.add_argument("--rows", type=int, default=50_000,
                        help="Number of rows to generate (default: 50000)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility (default: 42)")
    args = parser.parse_args()

    Path("data").mkdir(exist_ok=True)

    print(f"Generating {args.rows:,} rows with seed={args.seed} ...")
    df = generate(n_rows=args.rows, seed=args.seed)

    out = Path("data/events_v1.pkl")
    df.to_pickle(out)

    size_mb = out.stat().st_size / 1_048_576
    print(f"Saved → {out}  |  {len(df):,} rows  |  {size_mb:.1f} MB")
    print(f"Columns: {list(df.columns)}")
    print(f"Sample:\n{df.head(3).to_string(index=False)}")


if __name__ == "__main__":
    main()