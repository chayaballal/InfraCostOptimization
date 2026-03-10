-- EC2 metrics storage + analytics views for recommendation engine.
-- Safe to re-run (idempotent).

CREATE TABLE IF NOT EXISTS ec2_metrics_latest (
    instance_id   VARCHAR(64)  NOT NULL,
    instance_name VARCHAR(255),
    instance_type VARCHAR(64),
    metric_name   VARCHAR(128) NOT NULL,
    day_bucket    DATE         NOT NULL,
    category      VARCHAR(64),
    unit          VARCHAR(64),
    az            VARCHAR(64),
    platform      VARCHAR(64),
    stat_average  DOUBLE PRECISION,
    stat_maximum  DOUBLE PRECISION,
    stat_minimum  DOUBLE PRECISION,
    stat_sum      DOUBLE PRECISION,
    daily_active_hours DOUBLE PRECISION,
    loaded_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_ec2_metrics_latest PRIMARY KEY (instance_id, metric_name, day_bucket)
);

CREATE INDEX IF NOT EXISTS idx_ec2_metrics_latest_day_bucket
    ON ec2_metrics_latest (day_bucket);

CREATE INDEX IF NOT EXISTS idx_ec2_metrics_latest_metric
    ON ec2_metrics_latest (metric_name);

CREATE INDEX IF NOT EXISTS idx_ec2_metrics_latest_instance_window
    ON ec2_metrics_latest (instance_id, day_bucket);

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
        SUM(m.daily_active_hours) / NULLIF(COUNT(DISTINCT m.metric_name), 0) AS uptime_hours,

        AVG(m.stat_average) FILTER (WHERE m.metric_name = 'CPUUtilization') AS cpu_avg_pct,
        MAX(m.stat_maximum) FILTER (WHERE m.metric_name = 'CPUUtilization') AS cpu_peak_pct,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY m.stat_average)
            FILTER (WHERE m.metric_name = 'CPUUtilization') AS cpu_p95_pct,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY m.stat_average)
            FILTER (WHERE m.metric_name = 'CPUUtilization') AS cpu_p99_pct,

        AVG(m.stat_average) FILTER (WHERE m.metric_name = 'mem_used_percent') AS mem_avg_pct,
        MAX(m.stat_maximum) FILTER (WHERE m.metric_name = 'mem_used_percent') AS mem_peak_pct,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY m.stat_average)
            FILTER (WHERE m.metric_name = 'mem_used_percent') AS mem_p95_pct
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

-- Persistent pricing cache
CREATE TABLE IF NOT EXISTS ec2_instance_prices (
    instance_type VARCHAR(64)  NOT NULL,
    region        VARCHAR(64)  NOT NULL,
    hourly_usd    DOUBLE PRECISION NOT NULL,
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_ec2_instance_prices PRIMARY KEY (instance_type, region)
);

