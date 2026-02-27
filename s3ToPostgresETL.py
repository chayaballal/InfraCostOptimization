"""
╔══════════════════════════════════════════════════════════════════╗
║   EC2 CloudWatch Metrics — S3 Parquet → PostgreSQL ETL           ║
║                                                                  ║
║   • Reads Parquet from S3 (using S3 credentials)                 ║
║   • Aggregates raw 1-min data to daily buckets                   ║
║   • Upserts into ec2_metrics_latest                               ║
║   • Automatically refreshes 10/30/60/90-day views                ║
║   • Exposes v_ec2_llm_summary — LLM-ready pivot table            ║
║                                                                  ║
║   Run:  python s3_to_postgres_etl.py                             ║
║   Cron: 0 1 * * * python /path/to/s3_to_postgres_etl.py         ║
╚══════════════════════════════════════════════════════════════════╝

Dependencies:
    pip install boto3 pandas pyarrow sqlalchemy psycopg2-binary python-dotenv s3fs
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional

import boto3
import pandas as pd
import s3fs
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

load_dotenv()


# ──────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────
def load_config() -> dict:
    required = [
        "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY",
        "S3_BUCKET", "S3_PREFIX",
        "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise EnvironmentError(f"Missing .env keys: {missing}")

    return {
        "s3_key":    os.getenv("S3_ACCESS_KEY_ID"),
        "s3_secret": os.getenv("S3_SECRET_ACCESS_KEY"),
        "s3_bucket": os.getenv("S3_BUCKET"),
        "s3_prefix": os.getenv("S3_PREFIX"),
        "db_url": (
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        ),
        # How many days of S3 data to pull in a single run.
        # Set to None to pull everything available.
        "lookback_days": int(os.getenv("ETL_LOOKBACK_DAYS", 90)),
    }


# ──────────────────────────────────────────────────────────────────
# STEP 1: EXTRACT — Read Parquet from S3
# ──────────────────────────────────────────────────────────────────
def extract_from_s3(cfg: dict) -> pd.DataFrame:
    """
    Read all Parquet files from S3 prefix using s3fs + pandas.
    Supports Hive-style partitioned paths (year=.../month=.../day=...).
    """
    s3_path = f"s3://{cfg['s3_bucket']}/{cfg['s3_prefix']}/"
    log.info(f"Reading Parquet from {s3_path} ...")

    fs = s3fs.S3FileSystem(
        key=cfg["s3_key"],
        secret=cfg["s3_secret"],
    )

    # Collect parquet files
    try:
        all_files = fs.glob(f"{cfg['s3_bucket']}/{cfg['s3_prefix']}/**/*.parquet")
    except Exception as e:
        raise RuntimeError(f"Failed to list S3 files: {e}")

    if not all_files:
        raise ValueError(f"No Parquet files found at {s3_path}")

    log.info(f"Found {len(all_files)} Parquet file(s)")

    # Optional: filter to lookback window by parsing Hive partition path
    if cfg.get("lookback_days"):
        cutoff_date = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=cfg["lookback_days"])
        filtered = []
        for f in all_files:
            try:
                # Parse year=/month=/day= from path
                parts = {p.split("=")[0]: int(p.split("=")[1])
                         for p in f.split("/") if "=" in p}
                file_date = pd.Timestamp(
                    year=parts["year"], month=parts["month"], day=parts["day"], tz="UTC"
                )
                if file_date >= cutoff_date:
                    filtered.append(f)
            except Exception:
                filtered.append(f)  # include if can't parse
        log.info(f"After {cfg['lookback_days']}-day filter: {len(filtered)} file(s)")
        all_files = filtered

    # Read into DataFrame
    dfs = []
    for f in all_files:
        with fs.open(f, "rb") as fh:
            dfs.append(pd.read_parquet(fh))

    df = pd.concat(dfs, ignore_index=True)
    log.info(f"Extracted {len(df):,} raw rows from S3")
    return df


# ──────────────────────────────────────────────────────────────────
# STEP 2: TRANSFORM — Aggregate to daily buckets
# ──────────────────────────────────────────────────────────────────
def transform_to_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate sub-minute / minute-level CloudWatch datapoints
    into one row per (instance_id, metric_name, day_bucket).

    Aggregation rules:
      stat_average → mean  (avg of averages → representative daily avg)
      stat_maximum → max   (absolute peak for the day)
      stat_minimum → min   (absolute trough for the day)
      stat_sum     → sum   (cumulative, useful for Network/Disk throughput)
    """
    log.info("Transforming: aggregating to daily buckets...")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df.dropna(subset=["timestamp"], inplace=True)
    df["day_bucket"] = df["timestamp"].dt.date

    group_keys = [
        "instance_id", "instance_name", "instance_type",
        "metric_name", "day_bucket", "category", "unit", "az", "platform",
    ]
    # Keep only columns that exist (guards against schema drift)
    group_keys = [k for k in group_keys if k in df.columns]

    agg_map = {}
    for col, func in [
        ("stat_average", "mean"),
        ("stat_maximum", "max"),
        ("stat_minimum", "min"),
        ("stat_sum",     "sum"),
    ]:
        if col in df.columns:
            agg_map[col] = func

    aggregated = (
        df.groupby(group_keys, dropna=False)
          .agg(agg_map)
          .reset_index()
    )

    # Ensure all stat columns exist (even if source had none)
    for col in ["stat_average", "stat_maximum", "stat_minimum", "stat_sum"]:
        if col not in aggregated.columns:
            aggregated[col] = None

    log.info(
        f"Transformed: {len(aggregated):,} daily rows | "
        f"{aggregated['instance_id'].nunique()} instances | "
        f"{aggregated['metric_name'].nunique()} metrics"
    )
    return aggregated


# ──────────────────────────────────────────────────────────────────
# STEP 3: LOAD — Upsert into PostgreSQL
# ──────────────────────────────────────────────────────────────────
def upsert_to_postgres(df: pd.DataFrame, engine) -> int:
    """
    Upsert daily-aggregated rows into ec2_metrics_latest.
    Conflict key: (instance_id, metric_name, day_bucket)
    On conflict: update all stat columns + loaded_at.
    """
    if df.empty:
        log.warning("Nothing to upsert — DataFrame is empty.")
        return 0

    records = df.to_dict(orient="records")
    inserted = 0

    # Batch in chunks of 1,000 to avoid oversized transactions
    BATCH = 1_000

    def _upsert_batch(conn, batch):
        stmt = insert(
            _get_table_obj(conn)
        ).values(batch)

        update_cols = {
            c.name: c
            for c in stmt.excluded
            if c.name not in ("instance_id", "metric_name", "day_bucket")
        }
        update_cols["loaded_at"] = text("NOW()")

        upsert = stmt.on_conflict_do_update(
            index_elements=["instance_id", "metric_name", "day_bucket"],
            set_=update_cols,
        )
        conn.execute(upsert)

    # Lazy import so we can build the table object inside the connection scope
    from sqlalchemy import Table, MetaData

    def _get_table_obj(conn):
        meta = MetaData()
        meta.reflect(bind=conn, only=["ec2_metrics_latest"])
        return meta.tables["ec2_metrics_latest"]

    log.info(f"Upserting {len(records):,} rows in batches of {BATCH}...")
    with engine.begin() as conn:
        for i in range(0, len(records), BATCH):
            chunk = records[i : i + BATCH]
            _upsert_batch(conn, chunk)
            inserted += len(chunk)
            log.info(f"  Upserted {inserted:,} / {len(records):,}")

    log.info(f"Upsert complete: {inserted:,} rows processed")
    return inserted


# ──────────────────────────────────────────────────────────────────
# STEP 4: VERIFY — Row counts per window
# ──────────────────────────────────────────────────────────────────
def verify_counts(engine):
    """Print row counts for the base table and each window view."""
    queries = {
        "ec2_metrics_latest (all)": "SELECT COUNT(*) FROM ec2_metrics_latest",
        "v_ec2_metrics_10d":       "SELECT COUNT(*) FROM v_ec2_metrics_10d",
        "v_ec2_metrics_30d":       "SELECT COUNT(*) FROM v_ec2_metrics_30d",
        "v_ec2_metrics_60d":       "SELECT COUNT(*) FROM v_ec2_metrics_60d",
        "v_ec2_metrics_90d":       "SELECT COUNT(*) FROM v_ec2_metrics_90d",
        "v_ec2_llm_summary rows":  "SELECT COUNT(*) FROM v_ec2_llm_summary",
    }
    log.info("─" * 55)
    log.info("Verification counts:")
    with engine.connect() as conn:
        for label, sql in queries.items():
            try:
                count = conn.execute(text(sql)).scalar()
                log.info(f"  {label:<35} {count:>10,}")
            except Exception as e:
                log.warning(f"  {label:<35} ERROR: {e}")
    log.info("─" * 55)


# ──────────────────────────────────────────────────────────────────
# OPTIONAL: Preview the LLM summary for a given window
# ──────────────────────────────────────────────────────────────────
def preview_llm_summary(engine, window_days: int = 30, limit: int = 5):
    """Print a sample from v_ec2_llm_summary for quick inspection."""
    sql = text(
        "SELECT instance_id, instance_name, instance_type, "
        "cpu_avg_pct, cpu_peak_pct, cpu_p95_pct, "
        "mem_avg_pct, mem_peak_pct, "
        "ROUND(net_in_bytes_total::numeric / 1e9, 2)  AS net_in_gb, "
        "ROUND(net_out_bytes_total::numeric / 1e9, 2) AS net_out_gb, "
        "sample_days "
        "FROM v_ec2_llm_summary "
        "WHERE window_days = :w "
        "ORDER BY cpu_avg_pct DESC NULLS LAST "
        "LIMIT :l"
    )
    with engine.connect() as conn:
        result = conn.execute(sql, {"w": window_days, "l": limit})
        rows = result.fetchall()
        cols = result.keys()

    if rows:
        df = pd.DataFrame(rows, columns=cols)
        log.info(f"\nLLM Summary preview ({window_days}d window, top {limit} by CPU avg):\n"
                 + df.to_string(index=False))
    else:
        log.info(f"No rows in v_ec2_llm_summary for {window_days}d window yet.")


# ──────────────────────────────────────────────────────────────────
# APPLY SCHEMA — create table + views if they don't exist
# ──────────────────────────────────────────────────────────────────
def apply_schema(engine, schema_file: str = "ec2_metrics_schema.sql"):
    """Execute the DDL file to create table + views (idempotent)."""
    if not os.path.exists(schema_file):
        log.warning(f"Schema file '{schema_file}' not found — skipping DDL apply.")
        return
    log.info(f"Applying schema from {schema_file} ...")
    with open(schema_file) as f:
        ddl = f.read()
    with engine.begin() as conn:
        conn.execute(text(ddl))
    log.info("Schema applied.")


# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────
def main():
    run_time = datetime.now(timezone.utc)
    log.info("=" * 65)
    log.info("  EC2 S3 Parquet → PostgreSQL ETL")
    log.info(f"  Run time : {run_time.isoformat()}")
    log.info("=" * 65)

    cfg = load_config()
    engine = create_engine(cfg["db_url"], pool_pre_ping=True)

    # 0. Apply schema (idempotent — safe to run every time)
    apply_schema(engine)

    # 1. Extract
    raw_df = extract_from_s3(cfg)

    # 2. Transform
    daily_df = transform_to_daily(raw_df)

    # 3. Load
    upsert_to_postgres(daily_df, engine)

    # 4. Verify
    verify_counts(engine)

    # 5. Preview LLM summary (30-day window)
    preview_llm_summary(engine, window_days=30)

    log.info("=" * 65)
    log.info("  ✓ ETL complete")
    log.info("=" * 65)


if __name__ == "__main__":
    main()