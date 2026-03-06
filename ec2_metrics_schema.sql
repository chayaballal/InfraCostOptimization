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

        AVG(m.stat_average) FILTER (WHERE m.metric_name = 'CPUUtilization') AS cpu_avg_pct,
        MAX(m.stat_maximum) FILTER (WHERE m.metric_name = 'CPUUtilization') AS cpu_peak_pct,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY m.stat_average)
            FILTER (WHERE m.metric_name = 'CPUUtilization') AS cpu_p95_pct,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY m.stat_average)
            FILTER (WHERE m.metric_name = 'CPUUtilization') AS cpu_p99_pct,

        AVG(m.stat_average) FILTER (WHERE m.metric_name = 'mem_used_percent') AS mem_avg_pct,
        MAX(m.stat_maximum) FILTER (WHERE m.metric_name = 'mem_used_percent') AS mem_peak_pct,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY m.stat_average)
            FILTER (WHERE m.metric_name = 'mem_used_percent') AS mem_p95_pct,

        COALESCE(SUM(m.stat_sum) FILTER (WHERE m.metric_name = 'NetworkIn'), 0)  AS net_in_bytes_total,
        COALESCE(SUM(m.stat_sum) FILTER (WHERE m.metric_name = 'NetworkOut'), 0) AS net_out_bytes_total,
        AVG(m.stat_average) FILTER (WHERE m.metric_name = 'NetworkIn')            AS net_in_avg_bytes,
        AVG(m.stat_average) FILTER (WHERE m.metric_name = 'NetworkOut')           AS net_out_avg_bytes,

        COALESCE(SUM(m.stat_sum) FILTER (WHERE m.metric_name = 'DiskReadBytes'), 0)  AS disk_read_bytes_total,
        COALESCE(SUM(m.stat_sum) FILTER (WHERE m.metric_name = 'DiskWriteBytes'), 0) AS disk_write_bytes_total,
        COALESCE(SUM(m.stat_sum) FILTER (WHERE m.metric_name = 'VolumeReadBytes'), 0)  AS ebs_read_bytes_total,
        COALESCE(SUM(m.stat_sum) FILTER (WHERE m.metric_name = 'VolumeWriteBytes'), 0) AS ebs_write_bytes_total,

        AVG(m.stat_average) FILTER (WHERE m.metric_name IN ('EBSIOBalance%', 'EBSByteBalance%'))
            AS ebs_io_balance_avg_pct,
        COALESCE(SUM(m.stat_sum) FILTER (WHERE m.metric_name = 'StatusCheckFailed'), 0)
            AS status_check_failures
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
    cpu_avg_pct,
    cpu_peak_pct,
    cpu_p95_pct,
    cpu_p99_pct,
    mem_avg_pct,
    mem_peak_pct,
    mem_p95_pct,
    net_in_bytes_total,
    net_out_bytes_total,
    net_in_avg_bytes,
    net_out_avg_bytes,
    disk_read_bytes_total,
    disk_write_bytes_total,
    ebs_read_bytes_total,
    ebs_write_bytes_total,
    ebs_io_balance_avg_pct,
    status_check_failures
FROM base;
