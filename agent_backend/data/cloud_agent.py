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
import io
import logging
from datetime import datetime, timezone

import pandas as pd
import s3fs
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from agent_backend.data import ec2_cloudwatch_metrics
from agent_backend.data.database import Database
from agent_backend.agents.cost import cost_agent
import asyncio

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
def extract_from_s3(cfg: dict, engine) -> tuple[pd.DataFrame, str]:
    """
    Read new Parquet files from S3 prefix using s3fs + pandas and a watermark.
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

    # Incremental Loading (Watermark logic)
    with engine.connect() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS etl_watermark (
                process_name VARCHAR(100) PRIMARY KEY,
                last_processed_file VARCHAR(1000)
            )
        '''))
        conn.commit()
        result = conn.execute(text("SELECT last_processed_file FROM etl_watermark WHERE process_name = 's3_metrics'")).scalar()
        last_processed = result if result else ""

    all_files = sorted(all_files)
    new_files = [f for f in all_files if f > last_processed]

    if not new_files:
        log.info("No new files to process based on watermark.")
        return pd.DataFrame(), last_processed
        
    log.info(f"Processing {len(new_files)} new file(s) since last watermark.")

    # Read into DataFrame
    dfs = []
    for f in new_files:
        with fs.open(f, "rb") as fh:
            dfs.append(pd.read_parquet(fh))

    df = pd.concat(dfs, ignore_index=True)
    log.info(f"Extracted {len(df):,} raw rows from S3")
    return df, new_files[-1]


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
    
    # Pre-calculate min/max timestamp per instance per day before grouping
    # so we can compute the active hours (duration of metrics).
    # Since metrics are taken at intervals (e.g. 5m), an isolated point implies at least 5m (0.083 hours).
    ts_agg = df.groupby(["instance_id", "day_bucket"])["timestamp"].agg(["min", "max"]).reset_index()
    ts_agg["daily_active_hours"] = (ts_agg["max"] - ts_agg["min"]).dt.total_seconds() / 3600.0
    ts_agg["daily_active_hours"] = ts_agg["daily_active_hours"].apply(lambda x: max(0.083, x))
    
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
            
    # Merge the calculated daily_active_hours back into the aggregated dataframe
    aggregated = aggregated.merge(ts_agg[["instance_id", "day_bucket", "daily_active_hours"]], on=["instance_id", "day_bucket"], how="left")

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
    Upsert daily-aggregated rows using COPY into a staging table, 
    followed by an INSERT ... ON CONFLICT UPDATE for fast writes.
    """
    if df.empty:
        log.warning("Nothing to upsert — DataFrame is empty.")
        return 0

    inserted = len(df)
    log.info(f"Upserting {inserted:,} rows using COPY and staging table...")

    ensure_cols = ["instance_id", "instance_name", "instance_type", "metric_name", 
                   "day_bucket", "category", "unit", "az", "platform", 
                   "stat_average", "stat_maximum", "stat_minimum", "stat_sum", "daily_active_hours"]
                   
    for col in ensure_cols:
        if col not in df.columns:
            df[col] = None
    df = df[ensure_cols]

    # Create an in-memory CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=False, na_rep='\\N')
    csv_buffer.seek(0)

    columns = list(df.columns)
    columns_str = ", ".join(columns)
    
    with engine.begin() as conn:
        # Create temporary staging table (inherits schema)
        conn.execute(text("""
            CREATE TEMPORARY TABLE staging_metrics (LIKE ec2_metrics_latest INCLUDING ALL)
            ON COMMIT DROP
        """))
        
        # Get raw DBAPI connection to use 'copy_expert'
        dbapi_conn = conn.connection.driver_connection
        cursor = dbapi_conn.cursor()
        
        # COPY into staging table
        copy_sql = f"COPY staging_metrics ({columns_str}) FROM STDIN WITH CSV NULL '\\N'"
        cursor.copy_expert(copy_sql, csv_buffer)
        
        # Insert from staging into target table with ON CONFLICT UPDATE
        update_cols = [c for c in columns if c not in ("instance_id", "metric_name", "day_bucket")]
        # Ensure we don't accidentally try to set primary keys
        set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        set_clause += ", loaded_at = NOW()"
        
        upsert_sql = f"""
            INSERT INTO ec2_metrics_latest ({columns_str})
            SELECT {columns_str} FROM staging_metrics
            ON CONFLICT (instance_id, metric_name, day_bucket) 
            DO UPDATE SET {set_clause}
        """
        
        conn.execute(text(upsert_sql))
        
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

    # 1. Extract from CloudWatch -> S3
    log.info("Running CloudWatch extraction to S3...")
    try:
        ec2_cloudwatch_metrics.main()
    except Exception as e:
        log.error(f"Failed to extract metrics to S3: {e}")
        log.info("Proceeding with existing S3 files (if any)...")

    # 2. Extract from S3 -> DataFrame
    raw_df, latest_file = extract_from_s3(cfg, engine)
    
    if raw_df.empty:
        log.info("ETL skipped as no new data found.")
    else:
        # 2. Transform
        daily_df = transform_to_daily(raw_df)

        # 3. Load
        upsert_to_postgres(daily_df, engine)

        # Update watermark
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO etl_watermark (process_name, last_processed_file)
                VALUES ('s3_metrics', :lf)
                ON CONFLICT (process_name) DO UPDATE SET last_processed_file = EXCLUDED.last_processed_file
            """), {"lf": latest_file})

    # 4. Verify
    verify_counts(engine)

    # 5. Preview LLM summary (30-day window)
    preview_llm_summary(engine, window_days=30)
    
    # 6. Pricing Sync
    log.info("Starting persistent pricing sync...")
    try:
        # Get all instance types seen in the last 30 days to sync
        with engine.connect() as conn:
            res = conn.execute(text("SELECT DISTINCT instance_type FROM ec2_metrics_latest WHERE instance_type IS NOT NULL"))
            instance_types = [r[0] for r in res]
        
        db_url_async = cfg["db_url"].replace("postgresql://", "postgresql+asyncpg://")
        db_async = Database(db_url_async)
        cost_agent.init_pricing(db_async)
        asyncio.run(cost_agent.sync_prices(instance_types, os.getenv("AWS_REGION", "us-east-1")))
        log.info(f"Price sync complete for {len(instance_types)} instance types.")
    except Exception as e:
        log.error(f"Pricing sync failed: {e}")

    log.info("=" * 65)
    log.info("  ✓ ETL complete")
    log.info("=" * 65)


if __name__ == "__main__":
    main()