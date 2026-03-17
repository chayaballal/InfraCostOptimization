"""
database.py — Async database layer for the EC2 Analysis Agent.

This module encapsulates the SQLAlchemy async engine, session factory,
and all database operations. It uses PostgreSQL as the storage backend
for EC2 metrics, pricing information, and analysis results.

Key Components:
- SQLAlchemy Async Engine with pool_pre_ping for stability.
- Schema Bootstrap: Automated table/index creation on startup.
- Metrics & Timeseries: Querying pre-aggregated CloudWatch data.
- Pricing Cache: Optimized batch upserters using native Postgres logic.
"""

import logging
from typing import Optional

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

log = logging.getLogger(__name__)


class Database:
    """
    Manages async connections and operations for the PostgreSQL database.
    
    Provides specialized methods for fetching EC2 utilization data and
    persisting cost/savings analysis results.
    """

    def __init__(self, db_url: str) -> None:
        """
        Initializes the async engine and session factory with the provided DB URL.
        
        Args:
            db_url: The connection string for the PostgreSQL database.
        """
        self.engine = create_async_engine(db_url, pool_pre_ping=True, echo=False)
        self.session_factory = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    # ── Schema bootstrap ──────────────────────────────────────────

    async def ensure_schema(self) -> None:
        """
        Creates all necessary tables and indexes if they don't already exist.
        
        This handles:
        1. Metrics Indexes: For fast lookups on instance_id and time windows.
        2. Analysis Cache: Stores LLM responses to avoid redundant expensive calls.
        3. Savings Tracker: The 'Source of Truth' for all optimization recommendations.
        4. Pricing Cache: Stores hourly on-demand rates fetched from AWS.
        """
        async with self.engine.begin() as conn:
            try:
                # 1. Performance index for the main metrics table
                await conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS idx_ec2_metrics_latest_instance_window "
                    "ON ec2_metrics_latest (instance_id, day_bucket);"
                ))
                
                # 2. Simple K/V cache for LLM text responses
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS analysis_cache (
                        cache_key     VARCHAR(255) PRIMARY KEY,
                        response_text TEXT NOT NULL,
                        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                """))
                
                # 3. Central table for Rightsizing and Savings recommendations
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS savings_tracker (
                        id                           SERIAL PRIMARY KEY,
                        instance_id                  VARCHAR(50) NOT NULL,
                        instance_name                VARCHAR(255),
                        current_type                 VARCHAR(50),
                        recommended_type             VARCHAR(50),
                        recommendation               TEXT NOT NULL,
                        current_monthly_cost_usd     NUMERIC(10,2),
                        recommended_monthly_cost_usd NUMERIC(10,2),
                        estimated_monthly_saving_usd NUMERIC(10,2),
                        current_monthly_price_usd    NUMERIC(10,2),
                        recommended_monthly_price_usd NUMERIC(10,2),
                        status                       VARCHAR(20) NOT NULL DEFAULT 'Proposed',
                        window_days                  INT NOT NULL DEFAULT 30,
                        created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        CONSTRAINT uq_savings_instance UNIQUE (instance_id)
                    );
                """))
                
                # 4. Local cache for AWS EC2 instance pricing (Linux, shared tenancy)
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS ec2_instance_prices (
                        instance_type VARCHAR(64)  NOT NULL,
                        region        VARCHAR(64)  NOT NULL,
                        hourly_usd    DOUBLE PRECISION NOT NULL,
                        updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                        CONSTRAINT pk_ec2_instance_prices PRIMARY KEY (instance_type, region)
                    );
                """))
                
                # Add columns to existing savings_tracker table if they are missing
                # (Handles iterative schema upgrades without migration scripts)
                await conn.execute(text("""
                    ALTER TABLE savings_tracker
                        ADD COLUMN IF NOT EXISTS instance_name VARCHAR(255),
                        ADD COLUMN IF NOT EXISTS current_type VARCHAR(50),
                        ADD COLUMN IF NOT EXISTS recommended_type VARCHAR(50),
                        ADD COLUMN IF NOT EXISTS recommendation TEXT,
                        ADD COLUMN IF NOT EXISTS current_monthly_cost_usd NUMERIC(10,2),
                        ADD COLUMN IF NOT EXISTS recommended_monthly_cost_usd NUMERIC(10,2),
                        ADD COLUMN IF NOT EXISTS estimated_monthly_saving_usd NUMERIC(10,2),
                        ADD COLUMN IF NOT EXISTS current_monthly_price_usd NUMERIC(10,2),
                        ADD COLUMN IF NOT EXISTS recommended_monthly_price_usd NUMERIC(10,2),
                        ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'Proposed',
                        ADD COLUMN IF NOT EXISTS window_days INT NOT NULL DEFAULT 30,
                        ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
                """))
                
                # Composite unique constraint to handle different lookback windows per instance
                await conn.execute(text("""
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
        """
        Retrieves utilization metrics from the v_ec2_llm_summary view.
        
        This view pre-calculates averages and percentiles (P95, P99) for CPU and Memory,
        de-duplicating instances to return the most relevant metadata per ID.
        """
        where_clauses = ["window_days = :w"]
        params: dict = {"w": window_days}

        if instance_ids:
            where_clauses.append("instance_id = ANY(:ids)")
            params["ids"] = instance_ids

        # DISTINCT ON (instance_id) ensures we get exactly one entry per instance,
        # preferring the one with the most recent metric samples.
        sql = text(f"""
            SELECT *
            FROM (
                SELECT DISTINCT ON (instance_id)
                    instance_id, instance_name, instance_type, az, platform,
                    window_days, sample_days, uptime_hours,
                    ROUND(cpu_avg_pct::numeric,  2) AS cpu_avg_pct,
                    ROUND(cpu_peak_pct::numeric, 2) AS cpu_peak_pct,
                    ROUND(cpu_p95_pct::numeric,  2) AS cpu_p95_pct,
                    ROUND(cpu_p99_pct::numeric,  2) AS cpu_p99_pct,
                    ROUND(mem_avg_pct::numeric,  2) AS mem_avg_pct,
                    ROUND(mem_peak_pct::numeric, 2) AS mem_peak_pct,
                    ROUND(mem_p95_pct::numeric,  2) AS mem_p95_pct
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
        """
        Fetches distinct instance metadata for population of UI selector dropdowns.
        
        Returns:
            A list of dictionaries, each containing instance metadata (id, name, type, etc.).
        """
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
        """
        Fetches 1-day bucketed CPU and Memory averages/peaks for chart visualization.
        
        Args:
            instance_id: The ID of the instance to fetch data for.
            window_days: The number of days of history to retrieve.
            
        Returns:
            A list of dictionaries with date and various metric statistics.
        """
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
        """
        Comparative daily CPU/Mem stats for multiple instances (raw rows).
        
        Args:
            instance_ids: List of instance IDs to compare.
            window_days: Time window for history.
            
        Returns:
            A list of metric data points for multiple instances.
        """
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

    # ── Uptime query ──────────────────────────────────────────────

    async def fetch_uptime(
        self,
        window_days: Optional[int] = None,
        instance_ids: Optional[list[str]] = None,
    ) -> list[dict]:
        """
        Returns per-instance uptime figures (days with metrics and total hours).
        
        uptime_days = total unique days where at least one metric point exists.
        uptime_hours = cumulative active hours (extracted from raw S3 metric timestamps).
        """
        where_clauses = []
        params: dict = {}

        if window_days is not None:
            where_clauses.append("window_days = :w")
            params["w"] = window_days

        if instance_ids:
            where_clauses.append("instance_id = ANY(:ids)")
            params["ids"] = instance_ids

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        sql = text(f"""
            SELECT
                instance_id,
                MAX(instance_name)  AS instance_name,
                MAX(instance_type)  AS instance_type,
                MAX(az)             AS az,
                MAX(platform)       AS platform,
                window_days,
                MAX(sample_days)    AS uptime_days,
                MAX(uptime_hours)   AS uptime_hours
            FROM v_ec2_llm_summary
            {where_sql}
            GROUP BY instance_id, window_days
            ORDER BY instance_id, window_days
        """)
        async with self.session_factory() as session:
            result = await session.execute(sql, params)
            return [dict(r) for r in result.mappings().all()]

    # ── Auto-select query ─────────────────────────────────────────

    async def auto_select_instances(
        self, window_days: int, condition: str
    ) -> list[str]:
        """
        Executes a dynamic WHERE clause generated by the LLM.
        Returns a list of instance IDs matching the natural language criteria.
        """
        sql = text(
            f"SELECT instance_id FROM v_ec2_llm_summary "
            f"WHERE window_days = :w AND ({condition})"
        )
        async with self.session_factory() as session:
            result = await session.execute(sql, {"w": window_days})
            return [r["instance_id"] for r in result.mappings().all()]

    # ── Pricing Cache ─────────────────────────────────────────────

    async def get_cached_prices(self, region: str) -> dict[str, float]:
        """
        Retrieves all cached instance prices for a specific region from the database.
        
        Args:
            region: The AWS region (e.g., 'us-east-1').
            
        Returns:
            A dictionary mapping instance types to their hourly costs in USD.
        """
        sql = text("SELECT instance_type, hourly_usd FROM ec2_instance_prices WHERE region = :region")
        async with self.session_factory() as session:
            result = await session.execute(sql, {"region": region})
            return {r[0]: r[1] for r in result.all()}

    async def upsert_prices(self, prices: list[dict]) -> None:
        """
        Atomically bulk-upserts a list of instance prices.
        
        This uses SQLAlchemy's PostgreSQL native 'ON CONFLICT DO UPDATE' logic.
        This approach is significantly more reliable with the asyncpg driver than raw 
        SQL text batching for large arrays.
        """
        if not prices:
            return
        
        from sqlalchemy import table, column, String, Float, text
        from sqlalchemy.dialects.postgresql import insert

        # Define an ad-hoc table construct for the upsert statement
        ec2_instance_prices = table("ec2_instance_prices",
            column("instance_type", String),
            column("region", String),
            column("hourly_usd", Float),
            column("updated_at")
        )

        # Build the batch insert/upsert statement
        stmt = insert(ec2_instance_prices).values(prices)
        stmt = stmt.on_conflict_do_update(
            index_elements=["instance_type", "region"],  # Conflicts if type+region already exists
            set_=dict(
                hourly_usd=stmt.excluded.hourly_usd,     # Update to the new price
                updated_at=text("NOW()")                 # Refresh the timestamp
            )
        )
        async with self.engine.begin() as conn:
            await conn.execute(stmt)
