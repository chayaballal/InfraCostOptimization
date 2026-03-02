import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(levelname)-8s │ %(message)s")
log = logging.getLogger(__name__)

load_dotenv()

def get_db_engine():
    # Construct connection string from .env
    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(db_url)

def upsert_to_postgres(df, engine):
    """
    Refined upsert function with explicit transaction management.
    """
    def method(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = insert(table.table).values(data)
        
        # Define what to update on conflict
        update_dict = {
            c.name: c for c in stmt.excluded 
            if not c.primary_key
        }
        
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=['instance_id', 'metric_name', 'day_bucket'],
            set_=update_dict
        )
        conn.execute(upsert_stmt)

    log.info(f"Upserting {len(df)} rows to PostgreSQL...")
    
    # --- THIS IS THE KEY CHANGE ---
    with engine.begin() as connection:
        df.to_sql(
            'ec2_metrics_daily', 
            connection,  # Pass the active transaction connection here
            if_exists='append', 
            index=False, 
            method=method
        )
    # ------------------------------

def run_s3_to_postgres_daily():
    # 1. Access S3 Credentials
    os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('S3_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('S3_SECRET_ACCESS_KEY')
    
    bucket = os.getenv('S3_BUCKET')
    prefix = os.getenv('S3_PREFIX')
    s3_path = f"s3://{bucket}/{prefix}/"

    # 2. Extract: Read Parquet from S3
    log.info(f"Reading Parquet data from {s3_path}...")
    # This reads all parquet files within the prefix
    df = pd.read_parquet(s3_path)
    
    # 3. Transform: Aggregate to Daily Buckets
    log.info("Transforming: Resampling to Daily Frequency...")
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['day_bucket'] = df['timestamp'].dt.date # Convert to YYYY-MM-DD
    
    # We group by instance and metric to get the daily profile
    aggregated_df = df.groupby([
        'instance_id', 
        'instance_name', 
        'instance_type', 
        'metric_name', 
        'day_bucket',
        'category',
        'unit',
        'az',
        'platform'
    ]).agg({
        'stat_average': 'mean',   # Average of the 1-minute averages
        'stat_maximum': 'max',    # Absolute peak for the day
        'stat_minimum': 'min',    # Absolute low for the day
        'stat_sum': 'sum'         # Sum of values (Network/Disk throughput)
    }).reset_index()

    # 4. Load: Upsert into Postgres
    engine = get_db_engine()
    upsert_to_postgres(aggregated_df, engine)
    log.info("Success: Data is now aggregated and stored in Postgres.")
    # 5. Verify (Add this to your script)
    with engine.connect() as conn:
        from sqlalchemy import text
        result = conn.execute(text("SELECT count(*) FROM ec2_metrics_daily"))
        count = result.scalar()
        log.info(f"Verification: There are now {count} total rows in the database.")

if __name__ == "__main__":
    run_s3_to_postgres_daily()