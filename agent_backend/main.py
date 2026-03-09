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

    source .venv/bin/activate
    uv run uvicorn agent_backend.main:app --reload --port 8000
"""

import os
import json
import asyncio
import logging
import re
from typing import AsyncGenerator, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from dotenv import load_dotenv
from sqlalchemy import text

from agent_backend.database import Database
from agent_backend.cache import AnalysisCache
from agent_backend.llm_service import LLMService
from agent_backend.prompt_builder import PromptBuilder
from agent_backend.savings import SavingsTracker
from agent_backend.pricing import format_pricing_for_prompt, get_pricing_table
from agent_backend.mcp_aws_pricing import compare_instance_costs, normalize_region

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(levelname)-8s │ %(message)s")
log = logging.getLogger(__name__)


def format_catalog_for_prompt(instance_types: list[str]) -> str:
    """
    Catalog enrichment is optional. Return empty markdown when catalog module is unavailable.
    """
    return ""


def get_catalog() -> list[dict]:
    """
    Catalog endpoint fallback to keep API stable when catalog module is unavailable.
    """
    return []

# ──────────────────────────────────────────────────────────────────
# APP + SERVICE INSTANTIATION
# ──────────────────────────────────────────────────────────────────
app = FastAPI(title="EC2 Analysis Agent", version="2.0.0")

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

DB_URL = (
    f"postgresql+asyncpg://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
)

db      = Database(DB_URL)
cache   = AnalysisCache(db)
llm     = LLMService(api_key=os.getenv("GROQ_API_KEY"))
prompts = PromptBuilder()
savings = SavingsTracker(db)

# ──────────────────────────────────────────────────────────────────
# PYDANTIC REQUEST MODELS
# ──────────────────────────────────────────────────────────────────
class AnalysisRequest(BaseModel):
    window_days:  int            = 30
    instance_ids: list[str]      = []
    question:     Optional[str]  = None
    focus:        list[str]      = ["rightsizing", "risk_warnings", "full_report"]


class AutoSelectRequest(BaseModel):
    window_days: int = 30
    prompt:      Optional[str] = None


class EvalRequest(BaseModel):
    scenario_id: str
    metrics:     list[dict]
    window_days: int = 30


class SavingsEntry(BaseModel):
    instance_id:                  str
    instance_name:                Optional[str]   = None
    current_type:                 Optional[str]   = None
    recommended_type:             Optional[str]   = None
    recommendation:               str
    current_monthly_cost_usd:     Optional[float] = None
    recommended_monthly_cost_usd: Optional[float] = None
    estimated_monthly_saving_usd: Optional[float] = None
    window_days:                  Optional[int]   = None


class BulkSavingsRequest(BaseModel):
    markdown_text: str
    window_days:   int = 30
    instances:     list[dict] = []


class CompareCostRequest(BaseModel):
    current_type: str
    recommended_type: str
    region: Optional[str] = None


class CompareCostByInstanceRequest(BaseModel):
    instance_id: str
    recommended_type: str
    region: Optional[str] = None

# ──────────────────────────────────────────────────────────────────
# STARTUP
# ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    await db.ensure_schema()
    asyncio.create_task(cache.start_cleanup_loop())

# ──────────────────────────────────────────────────────────────────
# ROUTES
# ──────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "model": llm.model}


@app.get("/instances")
async def list_instances():
    try:
        instances = await db.fetch_available_instances()
        return {"instances": instances}
    except Exception as e:
        log.error(f"Failed to fetch instances: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fleet-summary")
async def fleet_summary(window_days: int = 30):
    try:
        rows = await db.fetch_metrics(window_days, [])
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
    log.info(f"Analysis request: window={req.window_days}d instances={req.instance_ids or 'ALL'}")

    # 0. Check cache
    cache_key = cache.build_key(req.instance_ids, req.window_days, req.focus, req.question)
    cached = await cache.get(cache_key)
    if cached:
        log.info(f"Cache HIT for key={cache_key[:12]}…")

        async def stream_cached():
            chunk_size = 80
            for i in range(0, len(cached), chunk_size):
                yield f"data: {json.dumps({'token': cached[i:i+chunk_size]})}\n\n"
                await asyncio.sleep(0.005)
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
        rows = await db.fetch_metrics(req.window_days, req.instance_ids)
    except Exception as e:
        log.error(f"DB fetch failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for window={req.window_days}d. "
                   "Check that the ETL has run and data exists in ec2_metrics_latest."
        )

    # 2. Fetch pricing + catalog (non-blocking, 3s timeout)
    fleet_types = list({r.get("instance_type") for r in rows if r.get("instance_type")})
    pricing_md = ""
    try:
        pricing_md = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, format_pricing_for_prompt, fleet_types),
            timeout=3.0
        )
    except (asyncio.TimeoutError, Exception) as e:
        log.warning(f"Pricing fetch skipped: {e}")

    catalog_md = ""
    try:
        catalog_md = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, format_catalog_for_prompt, fleet_types),
            timeout=3.0
        )
    except (asyncio.TimeoutError, Exception) as e:
        log.warning(f"Catalog fetch skipped: {e}")

    # 3. Build prompts
    formatted_data = prompts.format_metrics(rows, req.window_days, pricing_md + catalog_md)
    system_prompt  = prompts.build_system_prompt(req.focus)
    user_prompt    = prompts.build_user_prompt(formatted_data, req.window_days, req.question)

    log.info(f"Prompt built: {len(user_prompt)} chars | {len(rows)} instances")

    # 4. Stream response + cache
    collected_tokens = []

    async def streaming_with_cache():
        async for event in llm.stream_response(system_prompt, user_prompt):
            if event.startswith("data: ") and "[DONE]" not in event:
                try:
                    payload = json.loads(event[6:].strip())
                    collected_tokens.append(payload.get("token", ""))
                except (json.JSONDecodeError, KeyError):
                    pass
            yield event

        full_text = "".join(collected_tokens)
        if full_text and "**Error:**" not in full_text:
            try:
                await cache.save(cache_key, full_text)
                log.info(f"Cached response for key={cache_key[:12]}… ({len(full_text)} chars)")
            except Exception as e:
                log.warning(f"Failed to cache response: {e}")

    return StreamingResponse(
        streaming_with_cache(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                 "Access-Control-Allow-Origin": "*", "X-Cache": "MISS"},
    )


@app.post("/auto-select")
async def auto_select(req: AutoSelectRequest):
    condition = "cpu_avg_pct < 15 OR cpu_p95_pct > 80 OR mem_p95_pct > 80"

    if req.prompt and req.prompt.strip().lower() not in ["", "default"]:
        try:
            generated_sql = await llm.translate_prompt_to_sql(
                req.prompt, prompts.AUTO_SELECT_SYSTEM_PROMPT
            )
            if generated_sql:
                condition = generated_sql
        except Exception as e:
            log.warning(f"Failed to generate SQL from prompt: {e}. Falling back to default.")

    try:
        instance_ids = await db.auto_select_instances(req.window_days, condition)
        return {"instance_ids": instance_ids}
    except Exception as e:
        log.error(f"Auto-select query failed: {e} with condition: {condition}")
        raise HTTPException(status_code=500, detail=f"Database error during auto-select: {e}")


@app.post("/analyse-eval")
async def analyse_eval(req: EvalRequest):
    log.info(f"Eval request: scenario={req.scenario_id} instances={len(req.metrics)}")

    formatted_data = prompts.format_metrics(req.metrics, req.window_days)
    system_prompt  = prompts.build_eval_system_prompt()
    user_prompt    = (
        f"Analyse the following EC2 metrics (scenario: {req.scenario_id}):\n\n"
        f"{formatted_data}"
    )

    try:
        result = await llm.call_json(system_prompt, user_prompt)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=422, detail=f"LLM returned invalid JSON: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Groq error: {e}")

    return {"scenario_id": req.scenario_id, "llm_response": result}


@app.get("/timeseries")
async def get_timeseries(instance_id: str, window_days: int = 30):
    rows = await db.fetch_timeseries(instance_id, window_days)
    return {"instance_id": instance_id, "timeseries": rows}


@app.get("/instance-metrics")
async def instance_metrics(instance_id: str, window_days: int = 30):
    try:
        rows = await db.fetch_metrics(window_days, [instance_id])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    return {"instance_id": instance_id, "window_days": window_days, "metrics": rows}


@app.get("/pricing")
async def pricing_endpoint(instance_types: str = ""):
    types = [t.strip() for t in instance_types.split(",") if t.strip()] if instance_types else []
    try:
        table = get_pricing_table(types)
        return {"region": "us-east-1", "pricing": table}
    except Exception as e:
        log.error(f"Pricing fetch failed: {e}")
        raise HTTPException(status_code=500, detail=f"Pricing API error: {e}")


@app.post("/pricing/compare")
async def compare_pricing_endpoint(req: CompareCostRequest):
    """
    Compare the original instance cost with the recommended instance cost.
    Uses AWS MCP pricing when enabled; falls back to local pricing module.
    """
    current_type = req.current_type.strip()
    recommended_type = req.recommended_type.strip()
    if not current_type or not recommended_type:
        raise HTTPException(status_code=400, detail="Both current_type and recommended_type are required.")

    region = normalize_region(req.region or os.getenv("AWS_REGION"))

    try:
        result = await compare_instance_costs(
            current_type=current_type,
            recommended_type=recommended_type,
            region=region,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log.error(f"Pricing compare failed: {e}")
        raise HTTPException(status_code=500, detail=f"Pricing compare error: {e}")

    return result


async def _get_current_instance_type(instance_id: str) -> Optional[str]:
    """
    Resolve current instance_type for an instance_id from available sources.
    Tries summary-all (demo + real), then summary, then raw metrics table.
    """
    async with db.session_factory() as session:
        sources = [
            (
                "v_ec2_llm_summary",
                """
                SELECT instance_type
                FROM v_ec2_llm_summary
                WHERE instance_id = :iid
                  AND instance_type IS NOT NULL
                  AND instance_type <> ''
                ORDER BY sample_days DESC NULLS LAST, window_days DESC
                LIMIT 1
                """,
            ),
            (
                "v_ec2_llm_summary",
                """
                SELECT instance_type
                FROM v_ec2_llm_summary
                WHERE instance_id = :iid
                  AND instance_type IS NOT NULL
                  AND instance_type <> ''
                ORDER BY sample_days DESC NULLS LAST, window_days DESC
                LIMIT 1
                """,
            ),
            (
                "ec2_metrics_latest",
                """
                SELECT instance_type
                FROM ec2_metrics_latest
                WHERE instance_id = :iid
                  AND instance_type IS NOT NULL
                  AND instance_type <> ''
                ORDER BY day_bucket DESC
                LIMIT 1
                """,
            ),
        ]

        for source_name, sql_str in sources:
            try:
                value = await session.scalar(text(sql_str), {"iid": instance_id})
                if value:
                    return value
            except Exception as e:
                log.debug(f"Current type lookup skipped for {source_name}: {e}")

        return None


def _extract_instance_type(raw: str) -> Optional[str]:
    """
    Extract a valid EC2 instance type token from free text.
    Example: '... recommend t4g.small for this workload' -> 't4g.small'
    """
    if not raw:
        return None
    token = raw.strip().lower()
    # already a type
    if re.fullmatch(r"[a-z][a-z0-9]*\d[a-z0-9]*\.[a-z0-9]+", token):
        return token
    # find first type-like token in longer text
    m = re.search(r"\b([a-z][a-z0-9]*\d[a-z0-9]*\.[a-z0-9]+)\b", token)
    return m.group(1) if m else None


@app.post("/pricing/compare-by-instance")
async def compare_pricing_by_instance_endpoint(req: CompareCostByInstanceRequest):
    """
    Compare monthly cost using current type from Postgres + recommended type from request.
    """
    instance_id = req.instance_id.strip()
    recommended_type = req.recommended_type.strip()
    if not instance_id or not recommended_type:
        raise HTTPException(status_code=400, detail="Both instance_id and recommended_type are required.")

    current_type = await _get_current_instance_type(instance_id)
    if not current_type:
        raise HTTPException(
            status_code=404,
            detail=f"Could not resolve current instance_type for instance_id: {instance_id}",
        )

    normalized_recommended_type = _extract_instance_type(recommended_type)
    if not normalized_recommended_type:
        return {
            "instance_id": instance_id,
            "current_type": current_type,
            "recommended_type": None,
            "skipped": True,
            "skip_reason": "No valid EC2 instance type token found in recommendation text.",
            "raw_recommended_text": recommended_type,
        }

    region = normalize_region(req.region or os.getenv("AWS_REGION"))

    try:
        result = await compare_instance_costs(
            current_type=current_type,
            recommended_type=normalized_recommended_type,
            region=region,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log.error(f"Pricing compare-by-instance failed: {e}")
        raise HTTPException(status_code=500, detail=f"Pricing compare-by-instance error: {e}")

    return {
        "instance_id": instance_id,
        "current_type": current_type,
        "recommended_type": normalized_recommended_type,
        "skipped": False,
        **result,
    }


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
    instance_ids = [i.strip() for i in ids.split(",") if i.strip()]
    if not instance_ids:
        raise HTTPException(status_code=400, detail="At least one instance_id is required.")
    if len(instance_ids) > 6:
        raise HTTPException(status_code=400, detail="Maximum 6 instances allowed for comparison.")

    rows = await db.fetch_timeseries_compare(instance_ids, window_days)

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


# ── Savings Routes ────────────────────────────────────────────────

@app.post("/savings", status_code=201)
async def create_saving(entry: SavingsEntry):
    return await savings.create(
        instance_id=entry.instance_id,
        recommendation=entry.recommendation,
        instance_name=entry.instance_name,
        current_type=entry.current_type,
        recommended_type=entry.recommended_type,
        current_monthly_cost_usd=entry.current_monthly_cost_usd,
        recommended_monthly_cost_usd=entry.recommended_monthly_cost_usd,
        estimated_monthly_saving_usd=entry.estimated_monthly_saving_usd,
        window_days=entry.window_days,
    )


@app.post("/savings/bulk", status_code=201)
async def create_savings_bulk(req: BulkSavingsRequest):
    return await savings.create_bulk(req.markdown_text, req.instances, req.window_days)


@app.get("/savings")
async def list_savings(instance_id: Optional[str] = None, status: Optional[str] = None):
    return await savings.list(instance_id=instance_id, status=status)


@app.patch("/savings/{entry_id}")
async def update_saving_status(entry_id: int, status: str):
    try:
        return await savings.update_status(entry_id, status)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/preview-prompt")
async def preview_prompt(window_days: int = 30):
    rows           = await db.fetch_metrics(window_days, [])
    formatted_data = prompts.format_metrics(rows, window_days)
    system_prompt  = prompts.build_system_prompt(["rightsizing", "risk_warnings", "full_report"])
    user_prompt    = prompts.build_user_prompt(formatted_data, window_days, None)
    return {
        "instance_count": len(rows),
        "prompt_chars":   len(user_prompt),
        "system_prompt":  system_prompt,
        "user_prompt":    user_prompt,
    }
