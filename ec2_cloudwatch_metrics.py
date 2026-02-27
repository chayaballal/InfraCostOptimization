"""
╔══════════════════════════════════════════════════════════════════╗
║         EC2 CloudWatch Metrics → Parquet → S3 Loader             ║
║                                                                  ║
║  • Separate credentials for CloudWatch and S3                    ║
║  • Auto-discovers ALL EC2 instances dynamically                  ║
║  • Pulls all standard EC2 + MemoryUtilization metrics            ║
║  • Restructures into analytics-ready Parquet schema              ║
║  • Hive-style date-partitioned S3 upload                         ║
║  • All secrets loaded from .env                                  ║
╚══════════════════════════════════════════════════════════════════╝

Dependencies:
    pip install boto3 pandas pyarrow python-dotenv

Setup:
    1. Fill in your .env file
    2. python ec2_metrics_to_s3.py
"""

import os
import io
import logging
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Tuple

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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


# ──────────────────────────────────────────────────────────────────
# CONFIG — loaded from .env
# ──────────────────────────────────────────────────────────────────
def load_config() -> Dict[str, Any]:
    """Load and validate all configuration from .env file."""
    load_dotenv()

    required = [
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
        "S3_ACCESS_KEY_ID",  "S3_SECRET_ACCESS_KEY",
        "AWS_REGION", "S3_BUCKET", "S3_PREFIX",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise EnvironmentError(f"Missing required .env keys: {missing}")

    return {
        # CloudWatch credentials
        "cw_access_key":    os.getenv("AWS_ACCESS_KEY_ID"),
        "cw_secret_key":    os.getenv("AWS_SECRET_ACCESS_KEY"),
        # S3 credentials (separate IAM user)
        "s3_access_key":    os.getenv("S3_ACCESS_KEY_ID"),
        "s3_secret_key":    os.getenv("S3_SECRET_ACCESS_KEY"),
        # Common
        "region":           os.getenv("AWS_REGION", "us-east-1"),
        "s3_bucket":        os.getenv("S3_BUCKET"),
        "s3_prefix":        os.getenv("S3_PREFIX", "ec2-cloudwatch-metrics"),
        "lookback_minutes": int(os.getenv("LOOKBACK_MINUTES", 60)),
        "period_seconds":   int(os.getenv("PERIOD_SECONDS", 60)),
        # Memory metric namespace (CWAgent or System/Linux)
        "memory_namespace": os.getenv("MEMORY_METRIC_NAMESPACE", "CWAgent"),
    }


# ──────────────────────────────────────────────────────────────────
# EC2 STANDARD METRICS (namespace: AWS/EC2)
# ──────────────────────────────────────────────────────────────────
EC2_STANDARD_METRICS: List[Dict[str, Any]] = [
    # CPU
    {"metric": "CPUUtilization",              "stats": ["Average", "Maximum", "Minimum"], "category": "CPU"},
    {"metric": "CPUCreditUsage",              "stats": ["Sum","Maximum", "Minimum"],                           "category": "CPU"},
    {"metric": "CPUCreditBalance",            "stats": ["Average"],                       "category": "CPU"},
    {"metric": "CPUSurplusCreditBalance",     "stats": ["Average"],                       "category": "CPU"},
    {"metric": "CPUSurplusCreditsCharged",    "stats": ["Sum"],                           "category": "CPU"},
    # Disk
    {"metric": "DiskReadBytes",               "stats": ["Sum", "Average"],                "category": "Disk"},
    {"metric": "DiskWriteBytes",              "stats": ["Sum", "Average"],                "category": "Disk"},
    {"metric": "DiskReadOps",                 "stats": ["Sum", "Average"],                "category": "Disk"},
    {"metric": "DiskWriteOps",                "stats": ["Sum", "Average"],                "category": "Disk"},
    # Network
    {"metric": "NetworkIn",                   "stats": ["Sum", "Average"],                "category": "Network"},
    {"metric": "NetworkOut",                  "stats": ["Sum", "Average"],                "category": "Network"},
    {"metric": "NetworkPacketsIn",            "stats": ["Sum", "Average"],                "category": "Network"},
    {"metric": "NetworkPacketsOut",           "stats": ["Sum", "Average"],                "category": "Network"},
    # Status Checks
    {"metric": "StatusCheckFailed",           "stats": ["Sum"],                           "category": "StatusCheck"},
    {"metric": "StatusCheckFailed_Instance",  "stats": ["Sum"],                           "category": "StatusCheck"},
    {"metric": "StatusCheckFailed_System",    "stats": ["Sum"],                           "category": "StatusCheck"},
    # EBS (Nitro instances)
    {"metric": "EBSReadBytes",                "stats": ["Sum", "Average"],                "category": "EBS"},
    {"metric": "EBSWriteBytes",               "stats": ["Sum", "Average"],                "category": "EBS"},
    {"metric": "EBSReadOps",                  "stats": ["Sum", "Average"],                "category": "EBS"},
    {"metric": "EBSWriteOps",                 "stats": ["Sum", "Average"],                "category": "EBS"},
    {"metric": "EBSIOBalance%",               "stats": ["Average"],                       "category": "EBS"},
    {"metric": "EBSByteBalance%",             "stats": ["Average"],                       "category": "EBS"},
]

# ──────────────────────────────────────────────────────────────────
# MEMORY METRICS (namespace: CWAgent or System/Linux)
# Published by CloudWatch Agent installed on the instance
# Dimension key differs by namespace — handled dynamically below
# ──────────────────────────────────────────────────────────────────
MEMORY_METRICS: List[Dict[str, Any]] = [
    {"metric": "mem_used_percent",  "stats": ["Average", "Maximum"], "category": "Memory"},
    {"metric": "mem_used",          "stats": ["Average", "Maximum"], "category": "Memory"},
    {"metric": "mem_available",     "stats": ["Average", "Minimum"], "category": "Memory"},
    {"metric": "mem_total",         "stats": ["Average"],            "category": "Memory"},
    {"metric": "mem_cached",        "stats": ["Average"],            "category": "Memory"},
    {"metric": "mem_buffered",      "stats": ["Average"],            "category": "Memory"},
]


# ──────────────────────────────────────────────────────────────────
# AWS CLIENTS — separate sessions for CW and S3
# ──────────────────────────────────────────────────────────────────
def create_clients(cfg: Dict) -> Tuple[Any, Any, Any]:
    """
    Create 3 boto3 clients:
      - ec2_client    → uses CloudWatch credentials (same account)
      - cw_client     → CloudWatch read access
      - s3_client     → separate S3 IAM credentials
    """
    cw_session = boto3.Session(
        aws_access_key_id=cfg["cw_access_key"],
        aws_secret_access_key=cfg["cw_secret_key"],
        region_name=cfg["region"],
    )
    s3_session = boto3.Session(
        aws_access_key_id=cfg["s3_access_key"],
        aws_secret_access_key=cfg["s3_secret_key"],
        region_name=cfg["region"],
    )
    return (
        cw_session.client("ec2"),
        cw_session.client("cloudwatch"),
        s3_session.client("s3"),
    )


# ──────────────────────────────────────────────────────────────────
# EC2 DISCOVERY
# ──────────────────────────────────────────────────────────────────
def discover_ec2_instances(ec2_client) -> List[Dict[str, str]]:
    """Auto-discover all EC2 instances (running/stopped/pending) with metadata."""
    log.info("Discovering EC2 instances...")
    instances = []

    paginator = ec2_client.get_paginator("describe_instances")
    filters   = [{"Name": "instance-state-name", "Values": ["running", "stopped", "pending"]}]

    for page in paginator.paginate(Filters=filters):
        for reservation in page["Reservations"]:
            for inst in reservation["Instances"]:
                name = next(
                    (t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"),
                    "unnamed",
                )
                launch = inst.get("LaunchTime", "")
                instances.append({
                    "instance_id":   inst["InstanceId"],
                    "instance_name": name,
                    "instance_type": inst.get("InstanceType", "unknown"),
                    "state":         inst["State"]["Name"],
                    "az":            inst.get("Placement", {}).get("AvailabilityZone", ""),
                    "private_ip":    inst.get("PrivateIpAddress", ""),
                    "public_ip":     inst.get("PublicIpAddress", ""),
                    "launch_time":   launch.isoformat() if hasattr(launch, "isoformat") else "",
                    "platform":      inst.get("Platform", "linux"),
                })

    log.info(f"Discovered {len(instances)} EC2 instance(s).")
    return instances


# ──────────────────────────────────────────────────────────────────
# METRIC FETCHING
# ──────────────────────────────────────────────────────────────────
def fetch_metric_datapoints(
    cw_client,
    namespace: str,
    dimensions: List[Dict],
    metric_def: Dict[str, Any],
    start_time: datetime,
    end_time: datetime,
    period: int,
) -> List[Dict]:
    """Fetch datapoints for one metric, one namespace, one set of dimensions."""
    metric_name = metric_def["metric"]
    stats       = metric_def["stats"]
    category    = metric_def["category"]

    standard = [s for s in stats if not s.startswith("p")]
    extended = [s for s in stats if s.startswith("p")]

    kwargs = dict(
        Namespace=namespace,
        MetricName=metric_name,
        Dimensions=dimensions,
        StartTime=start_time,
        EndTime=end_time,
        Period=period,
    )
    if standard:
        kwargs["Statistics"] = standard
    if extended:
        kwargs["ExtendedStatistics"] = extended

    try:
        response   = cw_client.get_metric_statistics(**kwargs)
        datapoints = response.get("Datapoints", [])
    except Exception as e:
        err = str(e).lower()
        if "not authorized" in err or "accessdenied" in err:
            log.warning(f"No permission → {namespace}/{metric_name}. Skipping.")
        else:
            log.error(f"Error fetching {namespace}/{metric_name}: {e}")
        return []

    records = []
    for dp in datapoints:
        row = {
            "namespace":   namespace,
            "instance_id": next((d["Value"] for d in dimensions if d["Name"] == "InstanceId"), ""),
            "metric_name": metric_name,
            "category":    category,
            "timestamp":   dp["Timestamp"].astimezone(timezone.utc),
            "unit":        dp.get("Unit", "None"),
            "period_sec":  period,
        }
        for stat in standard:
            row[f"stat_{stat.lower()}"] = dp.get(stat)
        for stat in extended:
            row[f"stat_{stat.lower()}"] = dp.get("ExtendedStatistics", {}).get(stat)
        records.append(row)

    return records


def fetch_instance_metrics(
    cw_client,
    instance_meta: Dict[str, str],
    memory_namespace: str,
    start_time: datetime,
    end_time: datetime,
    period: int,
) -> List[Dict]:
    """
    Fetch ALL metrics for a single EC2 instance:
      1. Standard EC2 metrics   → namespace: AWS/EC2, dimension: InstanceId
      2. Memory metrics          → namespace: CWAgent (or custom), dimension: InstanceId
    """
    instance_id = instance_meta["instance_id"]
    all_records = []

    # ── Standard EC2 metrics ─────────────────────────────────────
    ec2_dims = [{"Name": "InstanceId", "Value": instance_id}]
    for metric_def in EC2_STANDARD_METRICS:
        records = fetch_metric_datapoints(
            cw_client, "AWS/EC2", ec2_dims, metric_def, start_time, end_time, period
        )
        all_records.extend(records)

    # ── Memory metrics (CloudWatch Agent namespace) ───────────────
    # CWAgent uses InstanceId dimension directly
    mem_dims = [{"Name": "InstanceId", "Value": instance_id}]
    for metric_def in MEMORY_METRICS:
        records = fetch_metric_datapoints(
            cw_client, memory_namespace, mem_dims, metric_def, start_time, end_time, period
        )
        all_records.extend(records)

    # Merge instance metadata into each record
    for rec in all_records:
        rec.update({
            "instance_name": instance_meta["instance_name"],
            "instance_type": instance_meta["instance_type"],
            "state":         instance_meta["state"],
            "az":            instance_meta["az"],
            "private_ip":    instance_meta["private_ip"],
            "public_ip":     instance_meta["public_ip"],
            "launch_time":   instance_meta["launch_time"],
            "platform":      instance_meta["platform"],
        })

    total_metrics = len(EC2_STANDARD_METRICS) + len(MEMORY_METRICS)
    log.info(
        f"  [{instance_id}] {instance_meta['instance_name']} → "
        f"{len(all_records)} datapoints across {total_metrics} metrics"
    )
    return all_records


# ──────────────────────────────────────────────────────────────────
# PARALLEL EXTRACTION
# ──────────────────────────────────────────────────────────────────
def extract_all_ec2_metrics(
    cw_client,
    instances: List[Dict],
    memory_namespace: str,
    start_time: datetime,
    end_time: datetime,
    period: int,
) -> pd.DataFrame:
    """Extract metrics for ALL instances in parallel."""
    if not instances:
        log.warning("No EC2 instances found.")
        return pd.DataFrame()

    all_records = []
    max_workers = min(len(instances), 10)

    log.info(f"Fetching metrics for {len(instances)} instance(s) | workers={max_workers}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                fetch_instance_metrics,
                cw_client, inst, memory_namespace, start_time, end_time, period
            ): inst["instance_id"]
            for inst in instances
        }
        for future in as_completed(futures):
            iid = futures[future]
            try:
                all_records.extend(future.result())
            except Exception as e:
                log.error(f"Failed for instance {iid}: {e}")

    if not all_records:
        log.warning("No datapoints retrieved for any instance.")
        return pd.DataFrame()

    return restructure_dataframe(all_records)


# ──────────────────────────────────────────────────────────────────
# RESTRUCTURE — clean analytics-ready schema
# ──────────────────────────────────────────────────────────────────
def restructure_dataframe(records: List[Dict]) -> pd.DataFrame:
    """
    Build a clean, typed, analytics-ready DataFrame.

    Final Schema:
    ┌───────────────────┬─────────────────────────────────────────────┐
    │ extracted_at      │ UTC timestamp this script ran               │
    │ timestamp         │ Metric datapoint timestamp (UTC)            │
    │ instance_id       │ EC2 Instance ID                             │
    │ instance_name     │ Name tag                                    │
    │ instance_type     │ t3.medium / m5.xlarge etc.                  │
    │ state             │ running / stopped / pending                 │
    │ az                │ Availability Zone                           │
    │ private_ip        │ Private IPv4                                │
    │ public_ip         │ Public IPv4                                 │
    │ platform          │ linux / windows                             │
    │ launch_time       │ Instance launch timestamp                   │
    │ namespace         │ AWS/EC2 or CWAgent                          │
    │ category          │ CPU / Memory / Disk / Network / EBS / ...   │
    │ metric_name       │ CloudWatch metric name                      │
    │ unit              │ Percent / Bytes / Count / None etc.         │
    │ period_sec        │ Aggregation period in seconds               │
    │ stat_average      │ Average value                               │
    │ stat_maximum      │ Maximum value                               │
    │ stat_minimum      │ Minimum value                               │
    │ stat_sum          │ Sum value                                   │
    └───────────────────┴─────────────────────────────────────────────┘
    """
    df = pd.DataFrame(records)

    # Ensure all stat columns exist
    for col in ["stat_average", "stat_maximum", "stat_minimum", "stat_sum"]:
        if col not in df.columns:
            df[col] = pd.NA

    df.insert(0, "extracted_at", datetime.now(timezone.utc).isoformat())

    ordered_cols = [
        "extracted_at", "timestamp",
        "instance_id", "instance_name", "instance_type", "state",
        "az", "private_ip", "public_ip", "platform", "launch_time",
        "namespace", "category", "metric_name", "unit", "period_sec",
        "stat_average", "stat_maximum", "stat_minimum", "stat_sum",
    ]
    extra_cols = [c for c in df.columns if c not in ordered_cols]
    df = df[ordered_cols + extra_cols]

    # Type enforcement
    df["timestamp"]    = pd.to_datetime(df["timestamp"], utc=True)
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)
    df["period_sec"]   = df["period_sec"].astype("int32")

    for col in ["stat_average", "stat_maximum", "stat_minimum", "stat_sum"] + extra_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    df.sort_values(["instance_id", "category", "metric_name", "timestamp"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    log.info(f"DataFrame  : {df.shape[0]:,} rows × {df.shape[1]} columns")
    log.info(f"Instances  : {df['instance_id'].nunique()}")
    log.info(f"Namespaces : {df['namespace'].unique().tolist()}")
    log.info(f"Categories : {sorted(df['category'].unique().tolist())}")
    log.info(f"Metrics    : {df['metric_name'].nunique()}")
    log.info(f"Time range : {df['timestamp'].min()} → {df['timestamp'].max()}")

    return df


# ──────────────────────────────────────────────────────────────────
# PARQUET CONVERSION
# ──────────────────────────────────────────────────────────────────
def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to in-memory Parquet bytes (Snappy compressed)."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf   = io.BytesIO()
    pq.write_table(
        table, buf,
        compression="snappy",
        use_dictionary=True,   # efficient for repeated strings
        write_statistics=True, # enables predicate pushdown in Athena/Glue
    )
    buf.seek(0)
    return buf.read()


# ──────────────────────────────────────────────────────────────────
# S3 UPLOAD — Hive-style date-partitioned path
# ──────────────────────────────────────────────────────────────────
def upload_to_s3(s3_client, bucket: str, prefix: str, data: bytes, run_time: datetime) -> str:
    """
    Upload Parquet to S3 with Hive-style partitioning for Athena/Glue compatibility.
    Path: s3://<bucket>/<prefix>/year=YYYY/month=MM/day=DD/metrics_HHMMSS.parquet
    """
    key = (
        f"{prefix}/"
        f"year={run_time.year}/"
        f"month={run_time.month:02d}/"
        f"day={run_time.day:02d}/"
        f"metrics_{run_time.strftime('%H%M%S')}.parquet"
    )
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/octet-stream",
        Metadata={
            "source":       "cloudwatch-ec2-extractor",
            "extracted_at": run_time.isoformat(),
        },
    )
    uri = f"s3://{bucket}/{key}"
    log.info(f"S3 upload  → {uri}")
    return uri


# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────
def main():
    run_time = datetime.now(timezone.utc)
    log.info("=" * 65)
    log.info("  EC2 CloudWatch Metrics → Parquet → S3")
    log.info(f"  Run time : {run_time.isoformat()}")
    log.info("=" * 65)

    # 1. Load config
    cfg = load_config()
    log.info(f"Region          : {cfg['region']}")
    log.info(f"S3 Target       : s3://{cfg['s3_bucket']}/{cfg['s3_prefix']}/")
    log.info(f"Lookback        : {cfg['lookback_minutes']} minutes")
    log.info(f"Period          : {cfg['period_seconds']} seconds")
    log.info(f"Memory NS       : {cfg['memory_namespace']}")

    # 2. Create clients (separate credentials for CW and S3)
    ec2_client, cw_client, s3_client = create_clients(cfg)

    # 3. Discover instances
    instances = discover_ec2_instances(ec2_client)
    if not instances:
        log.warning("No EC2 instances found. Exiting.")
        return

    # 4. Time window (Modified for 30 days)
    end_time   = run_time
    start_time = run_time - timedelta(days=5)
 
    log.info(f"Fetching data from {start_time} to {end_time}")

    # 5. Extract all metrics in parallel
    df = extract_all_ec2_metrics(
        cw_client, instances, cfg["memory_namespace"],
        start_time, end_time, cfg["period_seconds"]
    )
    if df.empty:
        log.warning("No metric data retrieved. Exiting.")
        return

    # 6. Preview
    log.info("\nSample data (first 5 rows):")
    log.info("\n" + df.head(5).to_string(index=False))

    # 7. Convert to Parquet
    log.info("Converting to Parquet (Snappy)...")
    parquet_bytes = to_parquet_bytes(df)
    log.info(f"Parquet size    : {len(parquet_bytes) / 1024:.1f} KB")

    # 8. Upload to S3 using separate S3 credentials
    s3_uri = upload_to_s3(s3_client, cfg["s3_bucket"], cfg["s3_prefix"], parquet_bytes, run_time)

    # 9. Local backup
    local_file = f"ec2_metrics_{run_time.strftime('%Y%m%d_%H%M%S')}.parquet"
    with open(local_file, "wb") as f:
        f.write(parquet_bytes)
    log.info(f"Local copy      : {local_file}")

    log.info("=" * 65)
    log.info(f"  ✓ Done! {len(df):,} rows → {s3_uri}")
    log.info("=" * 65)


if __name__ == "__main__":
    main()


# ──────────────────────────────────────────────────────────────────
# SCHEDULED RUN — uncomment to run every N seconds continuously
# ──────────────────────────────────────────────────────────────────
# import time
# INTERVAL = 300  # 5 minutes
# while True:
#     try:
#         main()
#     except Exception as e:
#         log.error(f"Run failed: {e}")
#     log.info(f"Next run in {INTERVAL}s...")
#     time.sleep(INTERVAL)