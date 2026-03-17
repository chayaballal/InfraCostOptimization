# Implementation Plan: Postgres to S3-Centric Architecture

Move the current PostgreSQL-based metrics and recommendation system to a pure S3 + DataFrame architecture.

## 1. Data Flow Redesign

### Current Flow (Postgres)
`CloudWatch` -> `S3 (Raw)` -> `Postgres (Aggregated)` -> `LLM` -> `Postgres (Savings)`

### Target Flow (S3-Centric)
`CloudWatch` -> `S3 (Raw)` -> `DataFrame ETL (Filtering)` -> `S3 (Summary)` -> `LLM` -> `S3 (Savings Tracker)`

## 2. Component Changes

### A. Data Storage (S3 Structure)
- `s3://bucket/metrics/raw/`: Daily/Hourly raw metrics (Parquet).
- `s3://bucket/metrics/summary/`: Filtered summary metrics for "candidate" instances.
- `s3://bucket/savings/tracker/`: Final recommendations and cost savings data.

### B. ETL Script (Python + Pandas/Polars)
- Replace PostgreSQL `INSERT/UPSERT` logic with S3 writes.
- **Candidate Selection**: Implement logic to identify "interesting" instances (e.g., < 10% CPU, high cost) during the ETL phase.
- **Summary Generation**: Calculate P95, Avg, and Min metrics directly in DataFrames and save as `summary_metrics.parquet`.

### C. LLM Integration
- Update `AnalysisAgent` to read from `s3://bucket/metrics/summary/` instead of querying Postgres views.

### D. Savings Tracker
- Replace `savings_tracker` DB table with a partitioned Parquet file or JSON lines in S3.
- Use DataFrames to update status (Proposed -> Implemented) by rewriting/appending to the S3 folder.

## 3. Tooling & Dependencies
- Use `s3fs` or `awswrangler` for easy Pandas-S3 integration.
- Remove `psycopg2`, `SQLAlchemy`, and any local DB setup code.

## 4. DFD Generation
- Create a Mermaid DFD visualizing the serverless/S3-native flow.
