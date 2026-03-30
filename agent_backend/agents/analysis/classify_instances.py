"""
classify_instances.py — Reads S3 parquet file, classifies instances, and outputs schedule_instances.json
"""
import os
import json
import logging
import boto3
import numpy as np
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(levelname)-8s │ %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent.parent / "data"
OUT_JSON = BASE_DIR / "schedule_instances.json"
LOCAL_PQ = "/tmp/mock_ec2_metrics_30d.parquet"

DAYS_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# Default instance mapping since S3 IDs might not have metadata
TYPE_MAPPING = {
    "CAT-1": "m5.large",
    "CAT-2": "m5.2xlarge",
    "CAT-3": "m5.xlarge",
    "CAT-4": "m5.large",
    "CAT-5": "t3.medium",
    "CAT-6": "c5.xlarge"
}

def download_from_s3():
    bucket = os.getenv('S3_BUCKET')
    prefix = os.getenv('S3_PREFIX', 'ec2-cloudwatch-metrics')
    key = f"{prefix}/mock_ec2_metrics_30d.parquet"
    
    if os.path.exists(LOCAL_PQ):
        log.info(f"Using cached S3 data at {LOCAL_PQ}")
        return pd.read_parquet(LOCAL_PQ)
        
    log.info(f"Downloading s3://{bucket}/{key} to {LOCAL_PQ}...")
    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('S3_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION', 'us-east-1')
    )
    s3.download_file(bucket, key, LOCAL_PQ)
    log.info("Download complete.")
    return pd.read_parquet(LOCAL_PQ)

def classify_instance(df_inst):
    df_inst = df_inst.sort_values("metric_ts")
    df_inst["ts"] = pd.to_datetime(df_inst["metric_ts"])
    df_inst["dow"] = df_inst["ts"].dt.day_name()
    df_inst["hour"] = df_inst["ts"].dt.hour
    
    cpu_series = df_inst["max_cpuutilization"]
    overall_cpu_avg = float(cpu_series.mean())
    overall_cpu_max = float(cpu_series.max())
    cpu_cov = float(cpu_series.std() / overall_cpu_avg if overall_cpu_avg > 0 else 0)
    peak_to_avg = float(overall_cpu_max / overall_cpu_avg if overall_cpu_avg > 0 else 0)
    
    # Calculate trend (slope per week)
    if len(cpu_series) > 1:
        x = np.arange(len(cpu_series))
        slope_per_5min = float(np.polyfit(x, cpu_series, 1)[0])
        trend_slope_pct_per_week = float(slope_per_5min * (12 * 24 * 7))
    else:
        trend_slope_pct_per_week = 0.0
        
    # Day of week profile
    dow_profile = {}
    active_days = 0
    for day in DAYS_ORDER:
        day_df = df_inst[df_inst["dow"] == day]
        if not day_df.empty:
            d_avg = float(day_df["max_cpuutilization"].mean())
            d_max = float(day_df["max_cpuutilization"].max())
            is_active = bool(d_avg > max(overall_cpu_avg * 0.5, 5.0))
            if is_active: active_days += 1
            dow_profile[day] = {"cpu_avg": round(d_avg, 1), "cpu_max": round(d_max, 1), "active": is_active}
        else:
            dow_profile[day] = {"cpu_avg": 0.0, "cpu_max": 0.0, "active": False}
            
    # Calculate simple weekday vs weekend avg
    weekday_df = df_inst[df_inst["dow"].isin(["Monday","Tuesday","Wednesday","Thursday","Friday"])]
    weekend_df = df_inst[df_inst["dow"].isin(["Saturday","Sunday"])]
    weekday_avg = float(weekday_df["max_cpuutilization"].mean()) if not weekday_df.empty else 0.0
    weekend_avg = float(weekend_df["max_cpuutilization"].mean()) if not weekend_df.empty else 0.0
    
    # Classification logic
    category = "CAT-1"
    schedule_type = "single"
    category_label = "Stable"
    job_windows = []
    p95_scale_down_times = {}
    
    if overall_cpu_avg < 2.0 and overall_cpu_max < 5.0:
        category = "CAT-5"
        category_label = "Zombie"
        schedule_type = "terminate"
        
    elif trend_slope_pct_per_week > 1.5:
        category = "CAT-6"
        category_label = "Trending"
        schedule_type = "single"
        
    elif active_days == 2 and peak_to_avg > 5.0:
        category = "CAT-3"
        category_label = "Periodic Burst"
        schedule_type = "time_slot"
        
        # Detect job windows for CAT-3
        for day in DAYS_ORDER:
            if dow_profile[day]["cpu_max"] > overall_cpu_avg * 2:
                day_df = df_inst[df_inst["dow"] == day]
                hourly = day_df.groupby("hour")["max_cpuutilization"].mean()
                burst_hours = hourly[hourly > overall_cpu_avg * 1.5].index.tolist()
                if burst_hours:
                    start_h = int(burst_hours[0])
                    end_h = int(burst_hours[-1])
                    job_windows.append({
                        "day": day,
                        "start_time": f"{start_h:02d}:00",
                        "end_time": f"{(end_h+1)%24:02d}:00",
                        "crosses_midnight": False # simplified
                    })
                    p95_scale_down_times[day] = f"{(end_h+1)%24:02d}:30"

    elif active_days == 5 and (weekday_avg / weekend_avg > 2.0 if weekend_avg > 0 else True):
        category = "CAT-2"
        category_label = "Day-Patterned"
        schedule_type = "day_of_week"
        
    elif peak_to_avg > 8.0 or (peak_to_avg > 4.0 and active_days >= 6):
        category = "CAT-4"
        category_label = "Unpredictable Spiky"
        schedule_type = "autoscaling"

    # Assemble JSON object
    inst_id = str(df_inst["instance_id"].iloc[0])
    return {
        "instance_id": inst_id,
        "instance_name": f"Auto-{category} Server",
        "current_type": TYPE_MAPPING.get(category, "m5.large"),
        "region": "us-east-1",
        "category": category,
        "category_label": category_label,
        "schedule_type": schedule_type,
        "metrics": {
            "active_days_per_week": active_days,
            "cpu_cov": round(cpu_cov, 2),
            "peak_to_avg_ratio": round(peak_to_avg, 2),
            "trend_slope_pct_per_week": round(trend_slope_pct_per_week, 2),
            "overall_cpu_avg": round(overall_cpu_avg, 1),
            "overall_cpu_max": round(overall_cpu_max, 1)
        },
        "day_of_week_profile": dow_profile,
        "job_windows": job_windows,
        "p95_scale_down_times": p95_scale_down_times
    }

def main():
    log.info("Starting instance classification from Parquet...")
    df = download_from_s3()
    log.info(f"Loaded {len(df)} rows across {df['instance_id'].nunique()} instances.")
    
    results = []
    for iid, g in df.groupby("instance_id"):
        result = classify_instance(g)
        results.append(result)
        log.info(f"Classified {iid} as {result['category']} ({result['schedule_type']})")
        
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(results, f, indent=2)
        
    log.info(f"Saved {len(results)} classified instances to {OUT_JSON}")

if __name__ == "__main__":
    main()
