import os
import random
from datetime import timedelta, date
from uuid import uuid4
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load testing environment variables
load_dotenv(".env.testing")

DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5433)}/{os.getenv('DB_NAME')}"
)

engine = create_engine(DB_URL)

def apply_schema():
    print("Applying schema...")
    with open("ec2_metrics_schema.sql") as f:
        ddl = f.read()
    with engine.begin() as conn:
        conn.execute(text(ddl))

def generate_mock_data():
    categories = [
        {"prefix": "mock-over", "count": 30, "cpu_range": (1, 10), "mem_range": (5, 20), "types": ["m5.2xlarge", "c5.4xlarge", "r5.4xlarge"]},
        {"prefix": "mock-risk", "count": 30, "cpu_range": (85, 99), "mem_range": (80, 95), "types": ["t3.micro", "t3.small", "m5.large"]},
        {"prefix": "mock-opt",  "count": 20, "cpu_range": (40, 60), "mem_range": (40, 60), "types": ["m5.large", "c5.xlarge", "r5.xlarge"]},
        {"prefix": "mock-mod",  "count": 20, "cpu_range": (40, 60), "mem_range": (40, 60), "types": ["m4.large", "c4.xlarge", "t2.micro"]},
        {"prefix": "mock-edge-zcpu", "count": 5, "cpu_range": (0, 0), "mem_range": (10, 20), "types": ["m5.large"]},
        {"prefix": "mock-edge-nomem", "count": 5, "cpu_range": (40, 60), "mem_range": None, "types": ["m5.large"]},
    ]

    end_date = date.today()
    start_date = end_date - timedelta(days=30)
    days = [(start_date + timedelta(days=i)) for i in range(31)]

    records = []

    for cat in categories:
        for i in range(cat["count"]):
            iid = f"i-{cat['prefix']}-{uuid4().hex[:8]}"
            iname = f"Instance {cat['prefix'].upper()} {i+1}"
            itype = random.choice(cat["types"])
            
            for d in days:
                # Add CPU
                cpu_avg = random.uniform(*cat["cpu_range"])
                cpu_max = min(100, cpu_avg + random.uniform(0, 15))
                records.append({
                    "instance_id": iid,
                    "instance_name": iname,
                    "instance_type": itype,
                    "metric_name": "CPUUtilization",
                    "day_bucket": d,
                    "category": "CPU",
                    "unit": "Percent",
                    "az": "us-east-1a",
                    "platform": "linux",
                    "stat_average": cpu_avg,
                    "stat_maximum": cpu_max,
                    "stat_minimum": max(0, cpu_avg - 5),
                    "stat_sum": None,
                    "daily_active_hours": 24.0,
                })
                
                # Add Memory if applicable
                if cat["mem_range"] is not None:
                    mem_avg = random.uniform(*cat["mem_range"])
                    mem_max = min(100, mem_avg + random.uniform(0, 10))
                    records.append({
                        "instance_id": iid,
                        "instance_name": iname,
                        "instance_type": itype,
                        "metric_name": "mem_used_percent",
                        "day_bucket": d,
                        "category": "Memory",
                        "unit": "Percent",
                        "az": "us-east-1a",
                        "platform": "linux",
                        "stat_average": mem_avg,
                        "stat_maximum": mem_max,
                        "stat_minimum": max(0, mem_avg - 5),
                        "stat_sum": None,
                        "daily_active_hours": 24.0,
                    })

    df = pd.DataFrame(records)
    print(f"Generated {len(df)} rows across {len(df['instance_id'].unique())} instances.")
    
    print("Clearing existing data...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE ec2_metrics_latest CASCADE;"))
        
    print("Inserting mock data...")
    df.to_sql("ec2_metrics_latest", engine, if_exists="append", index=False, method="multi", chunksize=1000)
    print("Done!")

if __name__ == "__main__":
    apply_schema()
    generate_mock_data()
