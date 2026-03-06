"""
database.py — Async database layer for the EC2 Analysis Agent.

Encapsulates the SQLAlchemy async engine, session factory,
and all raw SQL queries against PostgreSQL.
"""

import logging
from typing import Optional

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

log = logging.getLogger(__name__)


class Database:
    """Owns the async SQLAlchemy engine and provides typed query methods."""

    def __init__(self, db_url: str) -> None:
        self.engine = create_async_engine(db_url, pool_pre_ping=True, echo=False)
        self.session_factory = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    # ── Schema bootstrap ──────────────────────────────────────────

    async def ensure_schema(self) -> None:
        """Create indexes and tables if they don't exist."""
        async with self.engine.begin() as conn:
            try:
                await conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS idx_ec2_metrics_latest_instance_window "
                    "ON ec2_metrics_latest (instance_id, day_bucket);"
                ))
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS analysis_cache (
                        cache_key     VARCHAR(255) PRIMARY KEY,
                        response_text TEXT NOT NULL,
                        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                """))
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS savings_tracker (
                        id                           SERIAL PRIMARY KEY,
                        instance_id                  VARCHAR(50) NOT NULL,
                        instance_name                VARCHAR(255),
                        current_type                 VARCHAR(50),
                        recommended_type             VARCHAR(50),
                        recommendation               TEXT NOT NULL,
                        estimated_monthly_saving_usd NUMERIC(10,2),
                        status                       VARCHAR(20) NOT NULL DEFAULT 'Proposed',
                        window_days                  INT NOT NULL DEFAULT 30,
                        created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        CONSTRAINT uq_savings_instance_window UNIQUE (instance_id, window_days)
                    );
                """))
                log.info("Ensured indexes, analysis_cache, and savings_tracker tables.")
            except Exception as e:
                log.warning(f"Startup DB setup error: {e}")

    # ── Metrics queries ───────────────────────────────────────────

    async def fetch_metrics(
        self,
        window_days: int,
        instance_ids: list[str],
    ) -> list[dict]:
        """Pull from v_ec2_llm_summary for the chosen window."""
        where_clauses = ["window_days = :w"]
        params: dict = {"w": window_days}

        if instance_ids:
            where_clauses.append("instance_id = ANY(:ids)")
            params["ids"] = instance_ids

        sql = text(f"""
            SELECT *
            FROM (
                SELECT DISTINCT ON (instance_id)
                    instance_id, instance_name, instance_type, az, platform,
                    window_days, sample_days,
                    ROUND(cpu_avg_pct::numeric,  2) AS cpu_avg_pct,
                    ROUND(cpu_peak_pct::numeric, 2) AS cpu_peak_pct,
                    ROUND(cpu_p95_pct::numeric,  2) AS cpu_p95_pct,
                    ROUND(cpu_p99_pct::numeric,  2) AS cpu_p99_pct,
                    ROUND(mem_avg_pct::numeric,  2) AS mem_avg_pct,
                    ROUND(mem_peak_pct::numeric, 2) AS mem_peak_pct,
                    ROUND(mem_p95_pct::numeric,  2) AS mem_p95_pct,
                    ROUND((net_in_bytes_total  / 1e9)::numeric, 3) AS net_in_gb,
                    ROUND((net_out_bytes_total / 1e9)::numeric, 3) AS net_out_gb,
                    ROUND((net_in_avg_bytes    / 1e6)::numeric, 3) AS net_in_avg_mbps,
                    ROUND((net_out_avg_bytes   / 1e6)::numeric, 3) AS net_out_avg_mbps,
                    ROUND((disk_read_bytes_total  / 1e9)::numeric, 3) AS disk_read_gb,
                    ROUND((disk_write_bytes_total / 1e9)::numeric, 3) AS disk_write_gb,
                    ROUND((ebs_read_bytes_total   / 1e9)::numeric, 3) AS ebs_read_gb,
                    ROUND((ebs_write_bytes_total  / 1e9)::numeric, 3) AS ebs_write_gb,
                    ROUND(ebs_io_balance_avg_pct::numeric, 2) AS ebs_io_balance_pct,
                    status_check_failures
                FROM v_ec2_llm_summary
                WHERE {" AND ".join(where_clauses)}
                ORDER BY instance_id,
                         sample_days DESC NULLS LAST,
                         cpu_avg_pct DESC NULLS LAST
            ) dedup
            ORDER BY cpu_avg_pct DESC NULLS LAST
        """)

        async with self.session_factory() as session:
            result = await session.execute(sql, params)
            return [dict(r) for r in result.mappings().all()]

    async def fetch_available_instances(self) -> list[dict]:
        """Return distinct instances from the base table for the UI selector."""
        sql = text("""
            SELECT DISTINCT ON (instance_id)
                instance_id, instance_name, instance_type, az, platform
            FROM ec2_metrics_latest
            ORDER BY instance_id, day_bucket DESC
        """)
        async with self.session_factory() as session:
            result = await session.execute(sql)
            return [dict(r) for r in result.mappings().all()]

    # ── Time-series queries ───────────────────────────────────────

    async def fetch_timeseries(
        self, instance_id: str, window_days: int
    ) -> list[dict]:
        """Daily CPU + Memory time-series for a single instance."""
        sql = text("""
            SELECT
                TO_CHAR(day_bucket, 'YYYY-MM-DD') AS date,
                MAX(CASE WHEN metric_name = 'CPUUtilization'  THEN stat_average END) AS cpu_avg,
                MAX(CASE WHEN metric_name = 'CPUUtilization'  THEN stat_maximum END) AS cpu_max,
                MAX(CASE WHEN metric_name = 'mem_used_percent' THEN stat_average END) AS mem_avg,
                MAX(CASE WHEN metric_name = 'mem_used_percent' THEN stat_maximum END) AS mem_max
            FROM ec2_metrics_latest
            WHERE instance_id = :iid
              AND day_bucket >= CURRENT_DATE - CAST(:w AS INTEGER)
            GROUP BY day_bucket
            ORDER BY day_bucket ASC
        """)
        async with self.session_factory() as session:
            result = await session.execute(sql, {"iid": instance_id, "w": window_days})
            return [dict(r) for r in result.mappings().all()]

    async def fetch_timeseries_compare(
        self, instance_ids: list[str], window_days: int
    ) -> list[dict]:
        """Daily CPU + Memory time-series for multiple instances (raw rows)."""
        sql = text("""
            SELECT
                TO_CHAR(day_bucket, 'YYYY-MM-DD') AS date,
                instance_id,
                MAX(CASE WHEN metric_name = 'CPUUtilization'  THEN stat_average END) AS cpu_avg,
                MAX(CASE WHEN metric_name = 'mem_used_percent' THEN stat_average END) AS mem_avg
            FROM ec2_metrics_latest
            WHERE instance_id = ANY(:ids)
              AND day_bucket >= CURRENT_DATE - CAST(:w AS INTEGER)
            GROUP BY day_bucket, instance_id
            ORDER BY day_bucket ASC
        """)
        async with self.session_factory() as session:
            result = await session.execute(sql, {"ids": instance_ids, "w": window_days})
            return [dict(r) for r in result.mappings().all()]

    # ── Auto-select query ─────────────────────────────────────────

    async def auto_select_instances(
        self, window_days: int, condition: str
    ) -> list[str]:
        """Run a dynamic WHERE clause and return matching instance IDs."""
        sql = text(
            f"SELECT instance_id FROM v_ec2_llm_summary "
            f"WHERE window_days = :w AND ({condition})"
        )
        async with self.session_factory() as session:
            result = await session.execute(sql, {"w": window_days})
            return [r["instance_id"] for r in result.mappings().all()]
