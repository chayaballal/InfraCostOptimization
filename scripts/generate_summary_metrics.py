"""
generate_summary_metrics.py — Creates a local summary_metrics.parquet file
with 10 realistic EC2 instances covering varied utilization profiles:

  - Zombie         (cpu_avg < 1%)
  - Under-utilised (cpu_avg < 15%)
  - Healthy
  - CPU-stressed   (cpu_p95 > 80%)
  - Memory-pressured (mem_p95 > 85%)
  - Insufficient data (sample_days < 7)

Run:
    cd /home/surana/ec2-cloudwatch-metrics-extract
    python scripts/generate_summary_metrics.py

Output:
    agent_backend/data/summary_metrics.parquet
"""

import os
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# ── Output path: local, inside the project ────────────────────────
OUTPUT_PATH = Path(__file__).parent.parent / "agent_backend" / "data" / "summary_metrics.parquet"


def build_sample_data() -> pd.DataFrame:
    now = datetime.now(timezone.utc).isoformat()

    instances = [
        # ── 1. Zombie (cpu_avg < 1%) — should be TERMINATED ────────
        {
            "instance_id":   "i-0aa0011zombie",
            "instance_name": "legacy-batch-server",
            "instance_type": "m5.large",
            "az":            "us-east-1a",
            "platform":      "Linux",
            "cpu_avg_pct":   0.4,
            "cpu_peak_pct":  1.2,
            "cpu_p95_pct":   0.9,
            "mem_avg_pct":   3.1,
            "mem_peak_pct":  5.0,
            "mem_p95_pct":   4.5,
            "uptime_hours":  710.0,
            "sample_days":   30,
            "window_days":   30,
            "updated_at":    now,
        },
        # ── 2. Under-utilised (~5% CPU) — DOWNSIZE candidate ────────
        {
            "instance_id":   "i-0bb0022underuse",
            "instance_name": "reporting-service",
            "instance_type": "m5.2xlarge",
            "az":            "us-east-1b",
            "platform":      "Linux",
            "cpu_avg_pct":   5.2,
            "cpu_peak_pct":  18.4,
            "cpu_p95_pct":   12.1,
            "mem_avg_pct":   22.0,
            "mem_peak_pct":  30.5,
            "mem_p95_pct":   28.0,
            "uptime_hours":  712.5,
            "sample_days":   30,
            "window_days":   30,
            "updated_at":    now,
        },
        # ── 3. Under-utilised (~9% CPU) — DOWNSIZE candidate ────────
        {
            "instance_id":   "i-0cc0033lowcpu",
            "instance_name": "dev-api-server",
            "instance_type": "c5.xlarge",
            "az":            "us-east-1c",
            "platform":      "Linux",
            "cpu_avg_pct":   9.1,
            "cpu_peak_pct":  34.0,
            "cpu_p95_pct":   22.5,
            "mem_avg_pct":   41.2,
            "mem_peak_pct":  55.0,
            "mem_p95_pct":   50.3,
            "uptime_hours":  698.0,
            "sample_days":   29,
            "window_days":   30,
            "updated_at":    now,
        },
        # ── 4. Under-utilised (~12% CPU) — DOWNSIZE candidate ───────
        {
            "instance_id":   "i-0dd0044staging",
            "instance_name": "staging-web-app",
            "instance_type": "m5.xlarge",
            "az":            "us-east-1a",
            "platform":      "Linux",
            "cpu_avg_pct":   12.3,
            "cpu_peak_pct":  45.0,
            "cpu_p95_pct":   38.2,
            "mem_avg_pct":   35.5,
            "mem_peak_pct":  52.0,
            "mem_p95_pct":   48.0,
            "uptime_hours":  720.0,
            "sample_days":   30,
            "window_days":   30,
            "updated_at":    now,
        },
        # ── 5. Healthy / Optimal — no change needed ─────────────────
        {
            "instance_id":   "i-0ee0055healthy",
            "instance_name": "prod-web-frontend",
            "instance_type": "t3.medium",
            "az":            "us-east-1b",
            "platform":      "Linux",
            "cpu_avg_pct":   28.5,
            "cpu_peak_pct":  65.0,
            "cpu_p95_pct":   58.0,
            "mem_avg_pct":   52.0,
            "mem_peak_pct":  72.0,
            "mem_p95_pct":   68.0,
            "uptime_hours":  718.0,
            "sample_days":   30,
            "window_days":   30,
            "updated_at":    now,
        },
        # ── 6. Healthy — no change needed ───────────────────────────
        {
            "instance_id":   "i-0ff0066healthy2",
            "instance_name": "prod-auth-service",
            "instance_type": "t3.large",
            "az":            "us-east-1c",
            "platform":      "Linux",
            "cpu_avg_pct":   31.0,
            "cpu_peak_pct":  70.0,
            "cpu_p95_pct":   62.5,
            "mem_avg_pct":   58.0,
            "mem_peak_pct":  75.0,
            "mem_p95_pct":   71.0,
            "uptime_hours":  715.5,
            "sample_days":   30,
            "window_days":   30,
            "updated_at":    now,
        },
        # ── 7. CPU-stressed (p95 > 80%) — UPSIZE needed ─────────────
        {
            "instance_id":   "i-0aa0077cpustress",
            "instance_name": "ml-training-node",
            "instance_type": "c5.2xlarge",
            "az":            "us-east-1a",
            "platform":      "Linux",
            "cpu_avg_pct":   72.5,
            "cpu_peak_pct":  99.2,
            "cpu_p95_pct":   88.4,
            "mem_avg_pct":   45.0,
            "mem_peak_pct":  60.0,
            "mem_p95_pct":   55.0,
            "uptime_hours":  720.0,
            "sample_days":   30,
            "window_days":   30,
            "updated_at":    now,
        },
        # ── 8. Memory-pressured (mem_p95 > 85%) — CHANGE FAMILY ─────
        {
            "instance_id":   "i-0bb0088mempressure",
            "instance_name": "prod-db-cache",
            "instance_type": "r5.large",
            "az":            "us-east-1b",
            "platform":      "Linux",
            "cpu_avg_pct":   18.0,
            "cpu_peak_pct":  40.0,
            "cpu_p95_pct":   32.0,
            "mem_avg_pct":   88.5,
            "mem_peak_pct":  96.0,
            "mem_p95_pct":   92.0,
            "uptime_hours":  720.0,
            "sample_days":   30,
            "window_days":   30,
            "updated_at":    now,
        },
        # ── 9. Oversized — mem and CPU both very low ─────────────────
        {
            "instance_id":   "i-0cc0099downsize",
            "instance_name": "internal-tool-server",
            "instance_type": "r5.2xlarge",
            "az":            "us-east-1c",
            "platform":      "Linux",
            "cpu_avg_pct":   6.8,
            "cpu_peak_pct":  22.0,
            "cpu_p95_pct":   16.0,
            "mem_avg_pct":   14.0,
            "mem_peak_pct":  25.0,
            "mem_p95_pct":   20.0,
            "uptime_hours":  695.0,
            "sample_days":   29,
            "window_days":   30,
            "updated_at":    now,
        },
        # ── 10. Insufficient data (sample_days < 7) ─────────────────
        {
            "instance_id":   "i-0dd00aanewbox",
            "instance_name": "new-service-pod",
            "instance_type": "t3.small",
            "az":            "us-east-1a",
            "platform":      "Linux",
            "cpu_avg_pct":   7.5,
            "cpu_peak_pct":  30.0,
            "cpu_p95_pct":   20.0,
            "mem_avg_pct":   40.0,
            "mem_peak_pct":  55.0,
            "mem_p95_pct":   48.0,
            "uptime_hours":  120.0,
            "sample_days":   5,        # < 7 — insufficient data
            "window_days":   30,
            "updated_at":    now,
        },
    ]

    return pd.DataFrame(instances)


def main():
    df = build_sample_data()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False, engine="pyarrow", compression="snappy")

    print(f"\n✅ Written {len(df)} instances to: {OUTPUT_PATH}\n")
    print(
        df[[
            "instance_id", "instance_name", "instance_type",
            "cpu_avg_pct", "cpu_p95_pct", "mem_p95_pct", "sample_days",
        ]].to_string(index=False)
    )
    print()


if __name__ == "__main__":
    main()
