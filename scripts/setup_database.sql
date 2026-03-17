-- ============================================================================
-- setup_database.sql — Consolidated Database Setup Script
-- ============================================================================
-- Creates ALL tables, indexes, views, and constraints for the
-- EC2 CloudWatch Metrics Analysis platform.
--
-- Sources consolidated:
--   • ec2_metrics_schema.sql   — core metrics table, window views, LLM summary
--   • agent_backend/data/database.py — analysis_cache, savings_tracker, pricing
--   • agent_backend/data/cloud_agent.py — etl_watermark
--
-- Safe to re-run: every statement uses IF NOT EXISTS or CREATE OR REPLACE.
-- ============================================================================

BEGIN;

-- ────────────────────────────────────────────────────────────────────────────
-- 1. TABLES
-- ────────────────────────────────────────────────────────────────────────────

-- 1a. Core metrics table — daily-aggregated CloudWatch data
CREATE TABLE IF NOT EXISTS ec2_metrics_latest (
    instance_id        VARCHAR(64)      NOT NULL,
    instance_name      VARCHAR(255),
    instance_type      VARCHAR(64),
    metric_name        VARCHAR(128)     NOT NULL,
    day_bucket         DATE             NOT NULL,
    category           VARCHAR(64),
    unit               VARCHAR(64),
    az                 VARCHAR(64),
    platform           VARCHAR(64),
    stat_average       DOUBLE PRECISION,
    stat_maximum       DOUBLE PRECISION,
    stat_minimum       DOUBLE PRECISION,
    stat_sum           DOUBLE PRECISION,
    daily_active_hours DOUBLE PRECISION,
    loaded_at          TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_ec2_metrics_latest
        PRIMARY KEY (instance_id, metric_name, day_bucket)
);

-- 1b. ETL watermark — tracks incremental S3 file processing
CREATE TABLE IF NOT EXISTS etl_watermark (
    process_name        VARCHAR(100)  PRIMARY KEY,
    last_processed_file VARCHAR(1000)
);

-- 1c. Analysis cache — stores LLM responses to avoid redundant calls
CREATE TABLE IF NOT EXISTS analysis_cache (
    cache_key     VARCHAR(255) PRIMARY KEY,
    response_text TEXT         NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- 1d. Savings tracker — source of truth for rightsizing recommendations
CREATE TABLE IF NOT EXISTS savings_tracker (
    id                            SERIAL       PRIMARY KEY,
    instance_id                   VARCHAR(50)  NOT NULL,
    instance_name                 VARCHAR(255),
    current_type                  VARCHAR(50),
    recommended_type              VARCHAR(50),
    recommendation                TEXT         NOT NULL,
    current_monthly_cost_usd      NUMERIC(10,2),
    recommended_monthly_cost_usd  NUMERIC(10,2),
    estimated_monthly_saving_usd  NUMERIC(10,2),
    current_monthly_price_usd     NUMERIC(10,2),
    recommended_monthly_price_usd NUMERIC(10,2),
    status                        VARCHAR(20)  NOT NULL DEFAULT 'Proposed',
    window_days                   INT          NOT NULL DEFAULT 30,
    created_at                    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at                    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_savings_instance UNIQUE (instance_id)
);

-- 1e. Pricing cache — hourly on-demand EC2 rates from AWS
CREATE TABLE IF NOT EXISTS ec2_instance_prices (
    instance_type VARCHAR(64)      NOT NULL,
    region        VARCHAR(64)      NOT NULL,
    hourly_usd    DOUBLE PRECISION NOT NULL,
    updated_at    TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_ec2_instance_prices
        PRIMARY KEY (instance_type, region)
);


-- ────────────────────────────────────────────────────────────────────────────
-- 2. INDEXES
-- ────────────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_ec2_metrics_latest_day_bucket
    ON ec2_metrics_latest (day_bucket);

CREATE INDEX IF NOT EXISTS idx_ec2_metrics_latest_metric
    ON ec2_metrics_latest (metric_name);

CREATE INDEX IF NOT EXISTS idx_ec2_metrics_latest_instance_window
    ON ec2_metrics_latest (instance_id, day_bucket);


-- ────────────────────────────────────────────────────────────────────────────
-- 3. CONSTRAINTS (additive — safe on existing tables)
-- ────────────────────────────────────────────────────────────────────────────

-- Composite unique constraint for per-instance / per-window savings tracking
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_savings_instance_window'
          AND conrelid = 'savings_tracker'::regclass
    ) THEN
        ALTER TABLE savings_tracker
        ADD CONSTRAINT uq_savings_instance_window
        UNIQUE (instance_id, window_days);
    END IF;
END $$;

-- Schema evolution — add columns that may be missing on older deployments
ALTER TABLE savings_tracker
    ADD COLUMN IF NOT EXISTS instance_name                 VARCHAR(255),
    ADD COLUMN IF NOT EXISTS current_type                  VARCHAR(50),
    ADD COLUMN IF NOT EXISTS recommended_type              VARCHAR(50),
    ADD COLUMN IF NOT EXISTS recommendation                TEXT,
    ADD COLUMN IF NOT EXISTS current_monthly_cost_usd      NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS recommended_monthly_cost_usd  NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS estimated_monthly_saving_usd  NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS current_monthly_price_usd     NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS recommended_monthly_price_usd NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS status      VARCHAR(20)  NOT NULL DEFAULT 'Proposed',
    ADD COLUMN IF NOT EXISTS window_days INT          NOT NULL DEFAULT 30,
    ADD COLUMN IF NOT EXISTS created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW();


-- ────────────────────────────────────────────────────────────────────────────
-- 4. VIEWS
-- ────────────────────────────────────────────────────────────────────────────

-- 4a. Time-window convenience views (10/30/60/90 day slices)
CREATE OR REPLACE VIEW v_ec2_metrics_10d AS
SELECT *
FROM ec2_metrics_latest
WHERE day_bucket >= CURRENT_DATE - INTERVAL '10 days';

CREATE OR REPLACE VIEW v_ec2_metrics_30d AS
SELECT *
FROM ec2_metrics_latest
WHERE day_bucket >= CURRENT_DATE - INTERVAL '30 days';

CREATE OR REPLACE VIEW v_ec2_metrics_60d AS
SELECT *
FROM ec2_metrics_latest
WHERE day_bucket >= CURRENT_DATE - INTERVAL '60 days';

CREATE OR REPLACE VIEW v_ec2_metrics_90d AS
SELECT *
FROM ec2_metrics_latest
WHERE day_bucket >= CURRENT_DATE - INTERVAL '90 days';

-- 4b. LLM summary view — pre-aggregated stats per instance per window
--     Used by the AI agent for cost/utilization analysis
CREATE OR REPLACE VIEW v_ec2_llm_summary AS
WITH windows(window_days) AS (
    VALUES (10), (30), (60), (90)
),
base AS (
    SELECT
        w.window_days,
        m.instance_id,
        MAX(m.instance_name) AS instance_name,
        MAX(m.instance_type) AS instance_type,
        MAX(m.az)            AS az,
        MAX(m.platform)      AS platform,
        COUNT(DISTINCT m.day_bucket) AS sample_days,
        SUM(m.daily_active_hours) / NULLIF(COUNT(DISTINCT m.metric_name), 0)
            AS uptime_hours,

        -- CPU metrics
        AVG(m.stat_average) FILTER (WHERE m.metric_name = 'CPUUtilization')
            AS cpu_avg_pct,
        MAX(m.stat_maximum) FILTER (WHERE m.metric_name = 'CPUUtilization')
            AS cpu_peak_pct,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY m.stat_average)
            FILTER (WHERE m.metric_name = 'CPUUtilization')
            AS cpu_p95_pct,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY m.stat_average)
            FILTER (WHERE m.metric_name = 'CPUUtilization')
            AS cpu_p99_pct,

        -- Memory metrics
        AVG(m.stat_average) FILTER (WHERE m.metric_name = 'mem_used_percent')
            AS mem_avg_pct,
        MAX(m.stat_maximum) FILTER (WHERE m.metric_name = 'mem_used_percent')
            AS mem_peak_pct,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY m.stat_average)
            FILTER (WHERE m.metric_name = 'mem_used_percent')
            AS mem_p95_pct
    FROM windows w
    JOIN ec2_metrics_latest m
      ON m.day_bucket >= CURRENT_DATE - (w.window_days || ' days')::INTERVAL
    GROUP BY
        w.window_days,
        m.instance_id
)
SELECT
    instance_id,
    instance_name,
    instance_type,
    az,
    platform,
    window_days,
    sample_days,
    uptime_hours,
    cpu_avg_pct,
    cpu_peak_pct,
    cpu_p95_pct,
    cpu_p99_pct,
    mem_avg_pct,
    mem_peak_pct,
    mem_p95_pct
FROM base;


COMMIT;

-- ============================================================================
-- Done. All tables, indexes, views, and constraints are ready.
-- ============================================================================
