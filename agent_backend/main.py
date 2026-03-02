"""
╔══════════════════════════════════════════════════════════════════╗
║         EC2 Analysis Agent — FastAPI Backend                     ║
║                                                                  ║
║  • Pulls aggregated metrics from PostgreSQL                      ║
║  • Formats data optimally for LLM consumption                    ║
║  • Streams Groq LLM response (llama-3.3-70b-versatile)          ║
║  • Returns rightsizing, risk warnings, full markdown report      ║
╚══════════════════════════════════════════════════════════════════╝

Install:
    pip install fastapi uvicorn sqlalchemy asyncpg groq python-dotenv

Run:
    uvicorn main:app --reload --port 8000
"""

import os
import json
import asyncio
import hashlib
import logging
from typing import AsyncGenerator, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from groq import AsyncGroq
from dotenv import load_dotenv
from agent_backend.pricing import format_pricing_for_prompt, get_pricing_table
from agent_backend.catalog import get_catalog, format_catalog_for_prompt

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(levelname)-8s │ %(message)s")
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────
# APP SETUP
# ──────────────────────────────────────────────────────────────────
app = FastAPI(title="EC2 Analysis Agent", version="1.0.0")

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        FRONTEND_URL,
        "http://localhost:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5174",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────────
# DATABASE — async SQLAlchemy
# ──────────────────────────────────────────────────────────────────
DB_URL = (
    f"postgresql+asyncpg://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
)

engine = create_async_engine(DB_URL, pool_pre_ping=True, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

@app.on_event("startup")
async def startup_event():
    """Ensure indexes and cache table on startup."""
    async with engine.begin() as conn:
        try:
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ec2_metrics_latest_instance_window ON ec2_metrics_latest (instance_id, day_bucket);"))
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS analysis_cache (
                    cache_key   VARCHAR(255) PRIMARY KEY,
                    response_text TEXT NOT NULL,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """))
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS savings_tracker (
                    id                        SERIAL PRIMARY KEY,
                    instance_id               VARCHAR(50) NOT NULL,
                    instance_name             VARCHAR(255),
                    current_type              VARCHAR(50),
                    recommended_type          VARCHAR(50),
                    recommendation            TEXT NOT NULL,
                    estimated_monthly_saving_usd NUMERIC(10,2),
                    status                    VARCHAR(20) NOT NULL DEFAULT 'Proposed',
                    window_days               INT NOT NULL DEFAULT 30,
                    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_savings_instance_window UNIQUE (instance_id, window_days)
                );
            """))
            log.info("Ensured indexes, analysis_cache, and savings_tracker tables.")
        except Exception as e:
            log.warning(f"Startup DB setup error: {e}")

    # Start background cache cleanup loop
    asyncio.create_task(_cache_cleanup_loop())
    # Pre-warm the instance catalog in background (non-blocking)
    asyncio.get_event_loop().run_in_executor(None, get_catalog)


CACHE_TTL_HOURS = 24

async def _cache_cleanup_loop():
    """Purge expired cache entries every hour."""
    while True:
        await asyncio.sleep(3600)  # every hour
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(text(
                    f"DELETE FROM analysis_cache WHERE created_at < NOW() - INTERVAL '{CACHE_TTL_HOURS} hours'"
                ))
                await session.commit()
                log.info(f"Cache cleanup: purged {result.rowcount} expired entries.")
        except Exception as e:
            log.warning(f"Cache cleanup error: {e}")


def _build_cache_key(instance_ids: list[str], window_days: int, focus: list[str], question: str | None) -> str:
    """SHA-256 hash of the request parameters."""
    payload = json.dumps({
        "ids": sorted(instance_ids),
        "w": window_days,
        "focus": sorted(focus),
        "q": (question or "").strip().lower(),
    }, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


async def _get_cached_response(cache_key: str) -> str | None:
    """Return cached response text if exists and is fresh, else None."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(
            f"SELECT response_text FROM analysis_cache WHERE cache_key = :k AND created_at > NOW() - INTERVAL '{CACHE_TTL_HOURS} hours'"
        ), {"k": cache_key})
        row = result.scalar_one_or_none()
        return row


async def _save_cached_response(cache_key: str, response_text: str) -> None:
    """Upsert the response into the cache."""
    async with AsyncSessionLocal() as session:
        await session.execute(text("""
            INSERT INTO analysis_cache (cache_key, response_text, created_at)
            VALUES (:k, :t, NOW())
            ON CONFLICT (cache_key) DO UPDATE SET response_text = :t, created_at = NOW()
        """), {"k": cache_key, "t": response_text})
        await session.commit()

# ──────────────────────────────────────────────────────────────────
# GROQ CLIENT
# ──────────────────────────────────────────────────────────────────
groq_client = AsyncGroq(api_key=os.getenv("GROQ_API_KEY"))
GROQ_MODEL  = "llama-3.3-70b-versatile"

# ──────────────────────────────────────────────────────────────────
# REQUEST / RESPONSE MODELS
# ──────────────────────────────────────────────────────────────────
class AnalysisRequest(BaseModel):
    window_days:   int            = 30          # 10 | 30 | 60 | 90
    instance_ids:  list[str]      = []          # empty = all instances
    question:      Optional[str]  = None        # optional free-text question
    focus:         list[str]      = [           # what to include in report
        "rightsizing",
        "risk_warnings",
        "full_report",
    ]

# ──────────────────────────────────────────────────────────────────
# DATA FETCHER — pulls from v_ec2_llm_summary
# ──────────────────────────────────────────────────────────────────
async def fetch_metrics(
    window_days: int,
    instance_ids: list[str],
) -> list[dict]:
    """
    Pull from v_ec2_llm_summary for the chosen window.
    Filters by instance_ids if provided.
    """
    where_clauses = ["window_days = :w"]
    params: dict  = {"w": window_days}

    if instance_ids:
        where_clauses.append("instance_id = ANY(:ids)")
        params["ids"] = instance_ids

    sql = text(f"""
        SELECT *
        FROM (
            SELECT DISTINCT ON (instance_id)
                instance_id,
                instance_name,
                instance_type,
                az,
                platform,
                window_days,
                sample_days,
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
            ORDER BY
                instance_id,
                sample_days DESC NULLS LAST,
                cpu_avg_pct DESC NULLS LAST
        ) dedup
        ORDER BY cpu_avg_pct DESC NULLS LAST
    """)

    async with AsyncSessionLocal() as session:
        result = await session.execute(sql, params)
        rows   = result.mappings().all()

    return [dict(r) for r in rows]


async def fetch_available_instances() -> list[dict]:
    """Return distinct instances from the base table for the UI selector."""
    sql = text("""
        SELECT DISTINCT ON (instance_id)
            instance_id, instance_name, instance_type, az, platform
        FROM ec2_metrics_latest
        ORDER BY instance_id, day_bucket DESC
    """)
    async with AsyncSessionLocal() as session:
        result = await session.execute(sql)
        return [dict(r) for r in result.mappings().all()]


# ──────────────────────────────────────────────────────────────────
# PROMPT BUILDER — formats metrics into LLM-optimal structure
# ──────────────────────────────────────────────────────────────────
def format_metrics_for_llm(rows: list[dict], window_days: int, pricing_markdown: str = "") -> str:
    """
    Convert metric rows into a compact, LLM-readable markdown table
    with a structured summary block. Avoids token waste while keeping
    all signal the LLM needs for accurate recommendations.
    """
    if not rows:
        return "No metrics data available for the selected instances/window."

    lines = [
        f"## EC2 Fleet Utilization Report — Last {window_days} Days\n",
        f"Total instances analysed: **{len(rows)}**\n",
        "### Per-Instance Metrics\n",
        "| Instance ID | Name | Type | AZ | Platform | CPU Avg% | CPU P95% | CPU Peak% "
        "| Mem Avg% | Mem P95% | Net In GB | Net Out GB "
        "| Disk Read GB | Disk Write GB | EBS IO Bal% | Status Fails |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]

    for r in rows:
        def v(key, default="—"):
            val = r.get(key)
            return str(val) if val is not None else default

        lines.append(
            f"| {v('instance_id')} | {v('instance_name')} | {v('instance_type')} "
            f"| {v('az')} | {v('platform')} "
            f"| {v('cpu_avg_pct')} | {v('cpu_p95_pct')} | {v('cpu_peak_pct')} "
            f"| {v('mem_avg_pct')} | {v('mem_p95_pct')} "
            f"| {v('net_in_gb')} | {v('net_out_gb')} "
            f"| {v('disk_read_gb')} | {v('disk_write_gb')} "
            f"| {v('ebs_io_balance_pct')} | {v('status_check_failures')} |"
        )

    # Append raw JSON for deep context — LLMs handle JSON very well
    lines += [
        "\n### Raw JSON (full precision)\n",
        "```json",
        json.dumps(rows, indent=2, default=str),
        "```",
    ]

    # Append pricing reference table if available
    if pricing_markdown:
        lines.append(pricing_markdown)

    return "\n".join(lines)


def build_system_prompt(focus: list[str]) -> str:
    """Build a focused system prompt based on what the user wants."""
    sections = []

    if "rightsizing" in focus:
        sections.append(
            "**RIGHTSIZING RECOMMENDATIONS**: For each instance, recommend the optimal "
            "EC2 instance type based on observed CPU, memory, and network utilisation. "
            "Use AWS instance family knowledge: t3/t4g for burstable, m7i/m7g for "
            "general purpose, c7i/c7g for compute-optimised, r7i/r7g for memory-optimised. "
            "Provide a recommendation table: Instance ID | Current Type | Recommended Type | Reason."
        )

    if "risk_warnings" in focus:
        sections.append(
            "**RISK & PERFORMANCE WARNINGS**: Flag any instance where: "
            "CPU P95 > 80% (over-provisioned demand), "
            "CPU avg < 5% (massively under-utilised / candidate for termination), "
            "Memory P95 > 85% (memory pressure risk), "
            "EBS IO Balance < 20% (I/O throttling risk), "
            "Status check failures > 0 (reliability concern). "
            "Format as a risk table with severity: CRITICAL | HIGH | MEDIUM | LOW."
        )

    if "full_report" in focus:
        sections.append(
            "**FULL MARKDOWN REPORT**: After the tables above, write a complete executive "
            "summary report including: fleet health overview, top 3 cost optimisation "
            "opportunities, top 3 performance risks, recommended action plan with priority "
            "order, and estimated monthly cost savings (use us-east-1 on-demand pricing). "
            "Format with clear markdown headings."
        )

    focus_block = "\n\n".join(sections)

    return f"""You are an expert AWS Cloud Architect and FinOps analyst specialising in EC2 cost optimisation and performance engineering.

You will receive EC2 CloudWatch utilisation metrics aggregated over a time window.
Your job is to analyse the data and produce the following outputs:

{focus_block}

Rules:
- Be specific — reference exact instance IDs, types, and metric values.
- Use markdown formatting throughout (tables, headings, bold, code).
- Use the **provided pricing table** for all cost calculations. Do NOT use memorized or estimated prices. If a price is not in the table, say "pricing unavailable" for that type.
- In the rightsizing recommendation table, include exactly one row per unique instance ID from the input.
- Never repeat an instance ID in any recommendation table.
- If an instance has insufficient data, keep it as a single row for that same instance (do not add a second fallback row).
- If data is missing or a metric shows "—", note it but don't fabricate numbers.
- Think like a senior cloud engineer: consider workload patterns, not just averages.
- At the end, add a "Data Quality Notes" section flagging any instances with fewer than 7 sample days as potentially unreliable."""


def build_user_prompt(
    formatted_data: str,
    window_days: int,
    question: Optional[str],
) -> str:
    prompt = f"Analyse the following EC2 fleet metrics collected over the last **{window_days} days**:\n\n"
    prompt += formatted_data

    if question:
        prompt += f"\n\n---\n**Additional question from user:** {question}"

    return prompt


# ──────────────────────────────────────────────────────────────────
# GROQ STREAMING
# ──────────────────────────────────────────────────────────────────
async def stream_groq_response(
    system_prompt: str,
    user_prompt: str,
) -> AsyncGenerator[str, None]:
    """Stream tokens from Groq as Server-Sent Events with retry mechanism."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            stream = await groq_client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system",  "content": system_prompt},
                    {"role": "user",    "content": user_prompt},
                ],
                temperature=0.001,      # lower = more consistent/factual output
                max_tokens=4096,
                stream=True,
            )

            async for chunk in stream:
                delta = chunk.choices[0].delta.content
                if delta:
                    # SSE format
                    yield f"data: {json.dumps({'token': delta})}\n\n"

            yield "data: [DONE]\n\n"
            return
            
        except Exception as e:
            log.warning(f"Groq API Error on attempt {attempt+1}/{max_retries}: {e}")
            if attempt == max_retries - 1:
                # Send the error to the UI as part of the stream output
                error_msg = f"\\n\\n**Error:** LLM generation failed after retries."
                yield f"data: {json.dumps({'token': error_msg})}\n\n"
                yield "data: [DONE]\n\n"
            else:
                await asyncio.sleep(2 ** attempt)


# ──────────────────────────────────────────────────────────────────
# ROUTES
# ──────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "model": GROQ_MODEL}


@app.get("/instances")
async def list_instances():
    """Return all known EC2 instances for the UI instance selector."""
    try:
        instances = await fetch_available_instances()
        return {"instances": instances}
    except Exception as e:
        log.error(f"Failed to fetch instances: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fleet-summary")
async def fleet_summary(window_days: int = 30):
    """
    Return per-instance CPU and Memory averages for the fleet dashboard.
    Uses v_ec2_llm_summary so no extra DB views are needed.
    """
    try:
        rows = await fetch_metrics(window_days, [])
    except Exception as e:
        log.error(f"Fleet summary fetch failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    instances = []
    for r in rows:
        instances.append({
            "instance_id":   r.get("instance_id"),
            "instance_name": r.get("instance_name") or "unnamed",
            "instance_type": r.get("instance_type"),
            "cpu_avg":       round(float(r["cpu_avg_pct"]), 1) if r.get("cpu_avg_pct") is not None else None,
            "cpu_max":       round(float(r["cpu_peak_pct"]), 1) if r.get("cpu_peak_pct") is not None else None,
            "mem_avg":       round(float(r["mem_avg_pct"]), 1) if r.get("mem_avg_pct") is not None else None,
            "mem_max":       round(float(r["mem_peak_pct"]), 1) if r.get("mem_peak_pct") is not None else None,
        })

    return {"window_days": window_days, "instances": instances}


@app.post("/analyse")
async def analyse(req: AnalysisRequest):
    """
    Main endpoint — fetches metrics, formats them, streams Groq response.
    Returns a Server-Sent Event stream of tokens.
    Uses Postgres-backed cache to avoid duplicate LLM calls.
    """
    log.info(f"Analysis request: window={req.window_days}d instances={req.instance_ids or 'ALL'}")

    # 0. Check cache
    cache_key = _build_cache_key(req.instance_ids, req.window_days, req.focus, req.question)
    cached = await _get_cached_response(cache_key)
    if cached:
        log.info(f"Cache HIT for key={cache_key[:12]}… — serving cached response.")

        async def stream_cached():
            # Deliver cached text in small chunks for smooth SSE rendering
            chunk_size = 80
            for i in range(0, len(cached), chunk_size):
                yield f"data: {json.dumps({'token': cached[i:i+chunk_size]})}\n\n"
                await asyncio.sleep(0.005)  # tiny delay for smooth rendering
            yield "data: [DONE]\n\n"

        return StreamingResponse(
            stream_cached(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                     "Access-Control-Allow-Origin": "*", "X-Cache": "HIT"},
        )

    log.info(f"Cache MISS for key={cache_key[:12]}… — calling Groq.")

    # 1. Fetch data
    try:
        rows = await fetch_metrics(req.window_days, req.instance_ids)
    except Exception as e:
        log.error(f"DB fetch failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for window={req.window_days}d. "
                   "Check that the ETL has run and data exists in ec2_metrics_latest."
        )

    # 2. Fetch real pricing (non-blocking — 3s timeout so it never delays the LLM)
    fleet_types = list({r.get("instance_type") for r in rows if r.get("instance_type")})
    pricing_md = ""
    try:
        pricing_md = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, format_pricing_for_prompt, fleet_types),
            timeout=3.0
        )
    except (asyncio.TimeoutError, Exception) as e:
        log.warning(f"Pricing fetch skipped (timeout or error): {e}")
        pricing_md = ""

    # 2b. Fetch catalog specs (non-blocking — 3s timeout)
    catalog_md = ""
    try:
        catalog_md = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, format_catalog_for_prompt, fleet_types),
            timeout=3.0
        )
    except (asyncio.TimeoutError, Exception) as e:
        log.warning(f"Catalog fetch skipped (timeout or error): {e}")
        catalog_md = ""

    # 3. Format for LLM (includes pricing + catalog specs)
    formatted_data = format_metrics_for_llm(rows, req.window_days, pricing_md + catalog_md)
    system_prompt  = build_system_prompt(req.focus)
    user_prompt    = build_user_prompt(formatted_data, req.window_days, req.question)

    log.info(f"Prompt built: {len(user_prompt)} chars | {len(rows)} instances")

    # 4. Stream Groq response and capture for cache
    collected_tokens = []  # mutable list to collect tokens during streaming

    async def streaming_with_cache():
        async for event in stream_groq_response(system_prompt, user_prompt):
            # Extract token from the SSE event for caching
            if event.startswith("data: ") and "[DONE]" not in event:
                try:
                    payload = json.loads(event[6:].strip())
                    collected_tokens.append(payload.get("token", ""))
                except (json.JSONDecodeError, KeyError):
                    pass
            yield event

        # Save to cache after streaming completes
        full_text = "".join(collected_tokens)
        if full_text and "**Error:**" not in full_text:
            try:
                await _save_cached_response(cache_key, full_text)
                log.info(f"Cached response for key={cache_key[:12]}… ({len(full_text)} chars)")
            except Exception as e:
                log.warning(f"Failed to cache response: {e}")

    return StreamingResponse(
        streaming_with_cache(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*",
            "X-Cache":                     "MISS",
        },
    )


class EvalRequest(BaseModel):
    """Used by the eval runner — accepts synthetic metrics directly, bypasses DB."""
    scenario_id:  str
    metrics:      list[dict]        # pre-built metric rows (same schema as v_ec2_llm_summary)
    window_days:  int = 30


def build_eval_system_prompt() -> str:
    """
    Eval-specific system prompt — forces structured JSON output so the
    eval runner can parse and score it programmatically.
    """
    return """You are an expert AWS Cloud Architect evaluating EC2 instance efficiency.

Analyse the provided EC2 metrics and respond ONLY with a valid JSON object.
Do NOT include markdown fences, prose, or any text outside the JSON.

## MANDATORY RULES — apply in this exact order:

### 1. DATA QUALITY CHECK (check this FIRST)
If sample_days < 7:
  - rightsizing_action MUST be "insufficient_data"
  - confidence MUST be "low"
  - Add flag: low_sample_days
  - rightsizing_reason MUST mention "insufficient data" and the actual number of sample days

### 2. ZOMBIE / TERMINATION CHECK
If cpu_avg_pct < 1.0 AND net_in_gb < 0.1 AND net_out_gb < 0.1:
  - rightsizing_action MUST be "terminate"
  - Add flag: zombie with severity HIGH
  - Reason MUST contain words: "idle", "unused", or "terminate"

### 3. RIGHTSIZING ACTION RULES
Use EXACTLY one of these values for rightsizing_action:
  - "downsize"          → same family, smaller size (e.g. m5.xlarge → m5.large)
  - "upsize"            → same family, larger size
  - "change_family"     → different instance family (e.g. m5 → r5, m5 → c5)
  - "keep"              → current type is appropriate
  - "terminate"         → instance appears idle/unused
  - "insufficient_data" → sample_days < 7

CRITICAL family-change rules:
  - If mem_p95_pct > 85% AND cpu_avg_pct < 40%:
      → action = "change_family", recommend r5/r6i/r7i family (memory-optimised)
      → Do NOT recommend same m5 family at smaller size
  - If cpu_p95_pct > 80% AND mem_avg_pct < 60%:
      → action = "upsize" within compute-optimised family (c5/c6i/c7i)
      → If already on c-family, upsize within it

### 4. RISK FLAG RULES — use EXACTLY these flag names and thresholds:

  flag: "cpu_high"
    → ONLY raise if cpu_p95_pct > 80%
    → severity: CRITICAL if cpu_p95_pct > 90%, else HIGH
    → Do NOT raise cpu_high for low cpu utilisation

  flag: "zombie"
    → raise if cpu_avg_pct < 1.0 AND network near zero
    → severity: HIGH

  flag: "memory_pressure"
    → ONLY raise if mem_p95_pct > 85%
    → severity: CRITICAL if mem_p95_pct > 95%, else HIGH
    → Do NOT raise memory_pressure for mem_p95_pct < 85%

  flag: "io_throttle"
    → ONLY raise if ebs_io_balance_pct < 20%
    → severity: CRITICAL if ebs_io_balance_pct < 10%, else HIGH
    → detail MUST mention: "EBS", "IO balance", the actual percentage, and "throttling"

  flag: "status_check_failures"
    → raise if status_check_failures > 0
    → severity: CRITICAL if failures > 5, HIGH if 1-5
    → detail MUST mention: "status check", the exact number of failures, and "reliability"

  flag: "low_sample_days"
    → raise if sample_days < 7
    → severity: MEDIUM
    → detail MUST mention: "insufficient", "sample", exact number of days, "data quality"

### 5. SEVERITY REFERENCE
  CRITICAL → immediate action required, production risk
  HIGH     → action required soon, significant impact
  MEDIUM   → monitor and plan remediation
  LOW      → informational only

### 6. CONFIDENCE
  "high"   → sample_days >= 15
  "medium" → sample_days 7-14
  "low"    → sample_days < 7

Required JSON structure:
{
  "instances": [
    {
      "instance_id": "i-xxxxx",
      "instance_name": "...",
      "current_type": "m5.large",
      "recommended_type": "t3.medium",
      "rightsizing_action": "downsize | upsize | change_family | keep | terminate | insufficient_data",
      "rightsizing_reason": "one sentence explanation referencing actual metric values",
      "deciding_factors": [
        {
          "metric": "cpu_avg_pct | cpu_p95_pct | mem_p95_pct | ebs_io_balance_pct | status_check_failures | net_in_gb | sample_days",
          "observed_value": 2.1,
          "threshold": 10,
          "direction": "below_threshold | above_threshold",
          "impact": "primary | secondary | irrelevant",
          "explanation": "CPU avg 2.1% is well below 10% — instance is severely underutilised"
        }
      ],
      "risk_flags": [
        {
          "flag": "cpu_high | memory_pressure | io_throttle | status_check_failures | zombie | low_sample_days",
          "severity": "CRITICAL | HIGH | MEDIUM | LOW",
          "detail": "explanation referencing exact metric values"
        }
      ],
      "estimated_monthly_saving_usd": 12.50,
      "confidence": "high | medium | low"
    }
  ],
  "summary": {
    "total_instances": 1,
    "instances_to_downsize": 0,
    "instances_to_upsize": 0,
    "instances_to_terminate": 0,
    "instances_healthy": 1,
    "total_estimated_saving_usd": 0,
    "critical_risks": 0
  }
}

Return ONLY the JSON object. No markdown. No explanation. No preamble."""


async def call_groq_json(system_prompt: str, user_prompt: str) -> dict:
    """Non-streaming Groq call that returns parsed JSON. Used for evals."""
    response = await groq_client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        temperature=0.1,    # very low for eval consistency
        max_tokens=4096,
        stream=False,
    )
    raw = response.choices[0].message.content.strip()
    # Strip markdown fences if model added them despite instructions
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
        raw = raw.rsplit("```", 1)[0]
    return json.loads(raw)


@app.post("/analyse-eval")
async def analyse_eval(req: EvalRequest):
    """
    Eval endpoint — accepts synthetic metric rows directly (no DB).
    Returns structured JSON so the eval runner can score it.
    Temperature is 0.1 for reproducibility.
    """
    log.info(f"Eval request: scenario={req.scenario_id} instances={len(req.metrics)}")

    formatted_data = format_metrics_for_llm(req.metrics, req.window_days)
    system_prompt  = build_eval_system_prompt()
    user_prompt    = (
        f"Analyse the following EC2 metrics (scenario: {req.scenario_id}):\n\n"
        f"{formatted_data}"
    )

    try:
        result = await call_groq_json(system_prompt, user_prompt)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=422, detail=f"LLM returned invalid JSON: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Groq error: {e}")

    return {
        "scenario_id":  req.scenario_id,
        "llm_response": result,
    }



@app.get("/timeseries")
async def get_timeseries(instance_id: str, window_days: int = 30):
    """
    Return daily time-series metrics (CPU, Memory) for a specific instance.
    """
    sql = text("""
        SELECT 
            TO_CHAR(day_bucket, 'YYYY-MM-DD') as date,
            MAX(CASE WHEN metric_name = 'CPUUtilization' THEN stat_average ELSE NULL END) as cpu_avg,
            MAX(CASE WHEN metric_name = 'CPUUtilization' THEN stat_maximum ELSE NULL END) as cpu_max,
            MAX(CASE WHEN metric_name = 'mem_used_percent' THEN stat_average ELSE NULL END) as mem_avg,
            MAX(CASE WHEN metric_name = 'mem_used_percent' THEN stat_maximum ELSE NULL END) as mem_max
        FROM ec2_metrics_latest
        WHERE instance_id = :iid 
          AND day_bucket >= CURRENT_DATE - CAST(:w AS INTEGER)
        GROUP BY day_bucket
        ORDER BY day_bucket ASC
    """)
    
    async with AsyncSessionLocal() as session:
        result = await session.execute(sql, {"iid": instance_id, "w": window_days})
        rows = result.mappings().all()
        
    return {"instance_id": instance_id, "timeseries": [dict(r) for r in rows]}

@app.get("/instance-metrics")
async def instance_metrics(instance_id: str, window_days: int = 30):
    """
    Return metric rows for a single instance from v_ec2_llm_summary.
    Used by live_report.py to fetch real metrics before calling /analyse-eval.
    """
    try:
        rows = await fetch_metrics(window_days, [instance_id])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    return {"instance_id": instance_id, "window_days": window_days, "metrics": rows}


@app.get("/pricing")
async def pricing_endpoint(instance_types: str = ""):
    """
    Return on-demand pricing for the given instance types.
    Query param: ?instance_types=m5.large,t3.medium
    If empty, returns pricing for all cached + common rightsizing targets.
    """
    types = [t.strip() for t in instance_types.split(",") if t.strip()] if instance_types else []
    try:
        table = get_pricing_table(types)
        return {"region": "us-east-1", "pricing": table}
    except Exception as e:
        log.error(f"Pricing fetch failed: {e}")
        raise HTTPException(status_code=500, detail=f"Pricing API error: {e}")


# ── Instance Catalog ─────────────────────────────────────────────
@app.get("/instance-catalog")
async def instance_catalog_endpoint():
    """
    Return the full list of current-generation EC2 instance types
    with vCPU, memory, architecture, and network specs.
    Auto-refreshes from AWS every 24 hours.
    """
    try:
        catalog = await asyncio.get_event_loop().run_in_executor(None, get_catalog)
        return {"count": len(catalog), "catalog": catalog}
    except Exception as e:
        log.error(f"Catalog endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Multi-Instance Time-Series Comparison ────────────────────────
@app.get("/timeseries-compare")
async def timeseries_compare(ids: str, window_days: int = 30):
    """
    Return daily CPU + Memory time-series for multiple instances.
    Query param: ?ids=i-aaa,i-bbb&window_days=30
    Returns data shaped for Recharts multi-line charts:
      [ { date: '2026-02-01', 'i-aaa_cpu': 45, 'i-bbb_cpu': 72 }, ... ]
    """
    instance_ids = [i.strip() for i in ids.split(",") if i.strip()]
    if not instance_ids:
        raise HTTPException(status_code=400, detail="At least one instance_id is required.")
    if len(instance_ids) > 6:
        raise HTTPException(status_code=400, detail="Maximum 6 instances allowed for comparison.")

    sql = text("""
        SELECT
            TO_CHAR(day_bucket, 'YYYY-MM-DD') AS date,
            instance_id,
            MAX(CASE WHEN metric_name = 'CPUUtilization'  THEN stat_average ELSE NULL END) AS cpu_avg,
            MAX(CASE WHEN metric_name = 'mem_used_percent' THEN stat_average ELSE NULL END) AS mem_avg
        FROM ec2_metrics_latest
        WHERE instance_id = ANY(:ids)
          AND day_bucket >= CURRENT_DATE - CAST(:w AS INTEGER)
        GROUP BY day_bucket, instance_id
        ORDER BY day_bucket ASC
    """)

    async with AsyncSessionLocal() as session:
        result = await session.execute(sql, {"ids": instance_ids, "w": window_days})
        rows = result.mappings().all()

    # Pivot into { date → { instance_id_cpu: val, instance_id_mem: val } }
    pivoted: dict[str, dict] = {}
    for r in rows:
        d = r["date"]
        if d not in pivoted:
            pivoted[d] = {"date": d}
        iid = r["instance_id"]
        if r["cpu_avg"] is not None:
            pivoted[d][f"{iid}_cpu"] = round(float(r["cpu_avg"]), 1)
        if r["mem_avg"] is not None:
            pivoted[d][f"{iid}_mem"] = round(float(r["mem_avg"]), 1)

    return {
        "instance_ids": instance_ids,
        "window_days":  window_days,
        "series":       sorted(pivoted.values(), key=lambda x: x["date"]),
    }


# ── Savings Tracker ───────────────────────────────────────────────
class SavingsEntry(BaseModel):
    instance_id:                   str
    instance_name:                 Optional[str] = None
    current_type:                  Optional[str] = None
    recommended_type:              Optional[str] = None
    recommendation:                str
    estimated_monthly_saving_usd:  Optional[float] = None
    window_days:                   Optional[int] = None


@app.post("/savings", status_code=201)
async def create_saving(entry: SavingsEntry):
    """
    Log or update a cost-saving recommendation.
    Upserts on (instance_id, window_days) — prevents duplicates
    when the user clicks Save multiple times for the same analysis.
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("""
            INSERT INTO savings_tracker
                (instance_id, instance_name, current_type, recommended_type,
                 recommendation, estimated_monthly_saving_usd, window_days)
            VALUES
                (:iid, :iname, :ctype, :rtype, :rec, :saving, :win)
            ON CONFLICT (instance_id, window_days)
            DO UPDATE SET
                recommended_type             = COALESCE(EXCLUDED.recommended_type, savings_tracker.recommended_type),
                estimated_monthly_saving_usd = COALESCE(EXCLUDED.estimated_monthly_saving_usd, savings_tracker.estimated_monthly_saving_usd),
                instance_name                = COALESCE(EXCLUDED.instance_name, savings_tracker.instance_name),
                current_type                 = COALESCE(EXCLUDED.current_type, savings_tracker.current_type),
                recommendation               = EXCLUDED.recommendation,
                updated_at                   = NOW()
            RETURNING id, created_at, status
        """), {
            "iid":    entry.instance_id,
            "iname":  entry.instance_name,
            "ctype":  entry.current_type,
            "rtype":  entry.recommended_type,
            "rec":    entry.recommendation,
            "saving": entry.estimated_monthly_saving_usd,
            "win":    entry.window_days,
        })
        await session.commit()
        row = result.mappings().fetchone()
        return {"id": row["id"], "created_at": str(row["created_at"]), "status": row["status"]}



class BulkSavingsRequest(BaseModel):
    markdown_text: str          # Raw LLM output to parse
    window_days:   int = 30
    instances: list[dict] = []  # [{"instance_id": ..., "instance_name": ..., "instance_type": ...}]


def _parse_recommendations(markdown: str, instances: list[dict]) -> dict[str, dict]:
    """
    Parse LLM markdown output to build a map of instance_id → recommendation.
    Handles tables where the LLM follows the prompt's table format:
      Instance ID | Current Type | Recommended Type | Reason
    Also handles variations like "Instance | Current | Recommended | ..."
    """
    import re

    # Build a fast lookup: substring that contains instance ID → full instance dict
    known_ids = {inst["instance_id"] for inst in instances}
    rec_map: dict[str, dict] = {}

    lines = markdown.split("\n")
    headers: list[str] = []
    header_indices: dict[str, int] = {}

    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("|"):
            headers = []
            header_indices = {}
            continue

        cells = [c.strip() for c in stripped.split("|") if c.strip()]

        # Separator row — skip
        if all(re.match(r"^[-:]+$", c) for c in cells):
            continue

        lower_cells = [c.lower() for c in cells]

        # Header row — detect by presence of "recommend" in any cell
        if any("recommend" in c for c in lower_cells):
            headers = lower_cells
            for i, h in enumerate(headers):
                for key in ("instance", "current", "recommend", "saving", "reason", "action"):
                    if key in h:
                        header_indices[key] = i
            continue

        # Data row — only process if we have headers with a "recommend" column
        if "recommend" not in header_indices:
            continue

        rec_idx = header_indices["recommend"]
        if rec_idx >= len(cells):
            continue

        recommended_raw = cells[rec_idx].strip("`* ")

        # Find which instance this row belongs to
        row_text = " ".join(cells)
        matched_iid = None
        for iid in known_ids:
            if iid in row_text:
                matched_iid = iid
                break

        # If no direct ID match, try instance name match
        if not matched_iid:
            for inst in instances:
                if inst.get("instance_name") and inst["instance_name"].lower() in row_text.lower():
                    matched_iid = inst["instance_id"]
                    break

        if matched_iid and recommended_raw:
            # Extract savings estimate (look for $ amount anywhere in the row)
            saving = None
            saving_match = re.search(r"\$\s*([\d,]+(?:\.\d+)?)", row_text)
            if saving_match:
                try:
                    saving = float(saving_match.group(1).replace(",", ""))
                except ValueError:
                    pass

            # Skip "keep" / "no change" / "N/A" type recommendations
            skip_tokens = {"keep", "no change", "n/a", "none", "—", "-", "same"}
            if recommended_raw.lower() not in skip_tokens:
                rec_map[matched_iid] = {
                    "recommended_type": recommended_raw,
                    "saving": saving,
                }

    return rec_map


@app.post("/savings/bulk", status_code=201)
async def create_savings_bulk(req: BulkSavingsRequest):
    """
    Parse the LLM markdown report server-side and upsert savings tracker
    entries for all instances at once. Much more reliable than client-side parsing.
    """
    rec_map = _parse_recommendations(req.markdown_text, req.instances)
    log.info(f"Bulk savings parse: found {len(rec_map)} recommendations from {len(req.instances)} instances")

    saved = []
    async with AsyncSessionLocal() as session:
        for inst in req.instances:
            iid  = inst.get("instance_id")
            rec  = rec_map.get(iid, {})
            result = await session.execute(text("""
                INSERT INTO savings_tracker
                    (instance_id, instance_name, current_type, recommended_type,
                     recommendation, estimated_monthly_saving_usd, window_days)
                VALUES
                    (:iid, :iname, :ctype, :rtype, :rec, :saving, :win)
                ON CONFLICT (instance_id, window_days)
                DO UPDATE SET
                    recommended_type             = COALESCE(EXCLUDED.recommended_type, savings_tracker.recommended_type),
                    estimated_monthly_saving_usd = COALESCE(EXCLUDED.estimated_monthly_saving_usd, savings_tracker.estimated_monthly_saving_usd),
                    instance_name                = COALESCE(EXCLUDED.instance_name, savings_tracker.instance_name),
                    current_type                 = COALESCE(EXCLUDED.current_type, savings_tracker.current_type),
                    recommendation               = EXCLUDED.recommendation,
                    updated_at                   = NOW()
                RETURNING id, status
            """), {
                "iid":    iid,
                "iname":  inst.get("instance_name"),
                "ctype":  inst.get("instance_type"),
                "rtype":  rec.get("recommended_type"),
                "saving": rec.get("saving"),
                "rec":    f"Full report analysis — {req.window_days}d window",
                "win":    req.window_days,
            })
            row = result.mappings().fetchone()
            saved.append({"id": row["id"], "instance_id": iid,
                          "recommended_type": rec.get("recommended_type"),
                          "status": row["status"]})
        await session.commit()

    return {"saved": len(saved), "parsed_recommendations": len(rec_map), "entries": saved}


@app.get("/savings")
async def list_savings(instance_id: Optional[str] = None, status: Optional[str] = None):
    """Fetch all tracked recommendations, optionally filtered by instance or status."""
    where = ["1=1"]
    params: dict = {}
    if instance_id:
        where.append("instance_id = :iid")
        params["iid"] = instance_id
    if status:
        where.append("status = :status")
        params["status"] = status

    sql = text(f"""
        SELECT id, instance_id, instance_name, current_type, recommended_type,
               recommendation, estimated_monthly_saving_usd, status,
               window_days, created_at, updated_at
        FROM savings_tracker
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
    """)
    async with AsyncSessionLocal() as session:
        result = await session.execute(sql, params)
        rows = result.mappings().all()

    total_saving = sum(
        float(r["estimated_monthly_saving_usd"] or 0)
        for r in rows if r["status"] == "Implemented"
    )
    return {
        "entries": [dict(r) for r in rows],
        "total_entries": len(rows),
        "total_implemented_saving_usd": round(total_saving, 2),
    }


@app.patch("/savings/{entry_id}")
async def update_saving_status(entry_id: int, status: str):
    """
    Update the tracking status of a recommendation.
    Valid statuses: Proposed, Investigating, Implemented, Rejected
    """
    valid = {"Proposed", "Investigating", "Implemented", "Rejected"}
    if status not in valid:
        raise HTTPException(status_code=400, detail=f"Status must be one of {valid}")

    async with AsyncSessionLocal() as session:
        result = await session.execute(text("""
            UPDATE savings_tracker
            SET status = :status, updated_at = NOW()
            WHERE id = :id
            RETURNING id, status, updated_at
        """), {"status": status, "id": entry_id})
        await session.commit()
        row = result.mappings().fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Entry {entry_id} not found.")
        return dict(row)


@app.get("/preview-prompt")
async def preview_prompt(window_days: int = 30):
    """Debug endpoint — returns the formatted prompt without calling Groq."""
    rows           = await fetch_metrics(window_days, [])
    formatted_data = format_metrics_for_llm(rows, window_days)
    system_prompt  = build_system_prompt(["rightsizing", "risk_warnings", "full_report"])
    user_prompt    = build_user_prompt(formatted_data, window_days, None)
    return {
        "instance_count": len(rows),
        "prompt_chars":   len(user_prompt),
        "system_prompt":  system_prompt,
        "user_prompt":    user_prompt,
    }
