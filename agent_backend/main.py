"""
╔══════════════════════════════════════════════════════════════════╗
║         EC2 Analysis Agent — FastAPI Backend                     ║
║                                                                  ║
║  • Pulls aggregated metrics from PostgreSQL                      ║
║  • Formats data optimally for LLM consumption                    ║
║  • Streams Groq LLM response (llama-3.3-70b-versatile)           ║
║  • Returns rightsizing, risk warnings, full markdown report      ║
╚══════════════════════════════════════════════════════════════════╝

Install:
    pip install fastapi uvicorn sqlalchemy asyncpg groq python-dotenv

Run:
    uvicorn main:app --reload --port 8000

    source .venv/bin/activate
    uv run uvicorn agent_backend.main:app --reload --port 8000

EC2 Analysis Agent — FastAPI Backend

This module serves as the primary entry point for the EC2 Analysis Agent backend.
It provides a REST API built with FastAPI to:
- Interface with PostgreSQL for metric retrieval.
- Orchestrate LLM-based analysis of EC2 instance usage.
- Manage analysis results, caching, and savings recommendations.
- Provide pricing and catalog data for EC2 instances.

Key Components:
- FastAPI App: Configured with CORS for frontend interaction.
- Database: Async connection to PostgreSQL for metrics and state.
- LLM Service: Interface for streaming responses from Groq (Llama-3).
- Agents: Modular logic for analysis, cost calculation, and savings tracking.
"""

import os
import json
import asyncio
import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from dotenv import load_dotenv

from agent_backend.cache import AnalysisCache
# from agent_backend.agents.analysis.analysis_agent import LLMService, PromptBuilder
from agent_backend.agents.orchestrator.savings import SavingsTracker
from agent_backend.agents.cost.cost_agent import (
    get_pricing_table_async,
    format_pricing_for_prompt,
    get_uptime_pricing_table,
    get_pricing_table,
    compare_instance_costs,
    normalize_region,
)

load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s │ %(levelname)-8s │ %(message)s"
)
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
# Initialize the FastAPI application
app = FastAPI(
    title="EC2 Analysis Agent",
    version="2.0.0",
    description="Backend API for EC2 instance analysis and rightsizing recommendations.",
)

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

# Local storage initialization
cache = AnalysisCache()
savings = SavingsTracker()


# ──────────────────────────────────────────────────────────────────
# PYDANTIC REQUEST MODELS
# ──────────────────────────────────────────────────────────────────

class AnalysisRequest(BaseModel):
    """Request model for performing LLM analysis on a set of EC2 instances."""
    window_days: int = 30  # Number of days of historical data to analyze
    instance_ids: list[str] = []  # List of instance IDs to focus on (empty for all)
    question: Optional[str] = None  # Optional custom user question for the LLM
    focus: list[str] = ["rightsizing", "risk_warnings", "full_report"]  # Areas of analysis


class AutoSelectRequest(BaseModel):
    """Request model for automatically selecting instances based on utilization criteria."""
    window_days: int = 30
    prompt: Optional[str] = None  # Natural language prompt to refine selection


class EvalRequest(BaseModel):
    """Request model for evaluation tasks on specific metric scenarios."""
    scenario_id: str
    metrics: list[dict]
    window_days: int = 30


class SavingsEntry(BaseModel):
    """Model representing a single savings recommendation entry."""
    instance_id: str
    instance_name: Optional[str] = None
    current_type: Optional[str] = None
    current_monthly_price_usd: Optional[float] = None
    recommended_type: Optional[str] = None
    recommended_monthly_price_usd: Optional[float] = None
    recommendation: str
    current_monthly_cost_usd: Optional[float] = None
    recommended_monthly_cost_usd: Optional[float] = None
    estimated_monthly_saving_usd: Optional[float] = None
    window_days: Optional[int] = None


class BulkSavingsRequest(BaseModel):
    """Request model for saving multiple recommendations at once."""
    markdown_text: str  # Markdown containing recommendations to be parsed
    window_days: int = 30
    instances: list[dict] = []


class CompareCostRequest(BaseModel):
    """Request model for comparing costs between two instance types."""
    current_type: str
    recommended_type: str
    region: Optional[str] = None


class CompareCostByInstanceRequest(BaseModel):
    """Request model for comparing costs for a specific instance against a recommended type."""
    instance_id: str
    recommended_type: str
    region: Optional[str] = None


# ──────────────────────────────────────────────────────────────────
# STARTUP
# ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    log.info("Starting up in Local Data Mode (Parquet-based)")
    # No database setup needed
    asyncio.create_task(cache.start_cleanup_loop())


# ──────────────────────────────────────────────────────────────────
# ROUTES
# ──────────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    """Health check endpoint to verify API availability."""
    return {"status": "ok"}


# Helper to load local metrics
def load_local_metrics() -> pd.DataFrame:
    """Loads metrics from the local summary_metrics.parquet file."""
    path = Path(__file__).parent / "data" / "summary_metrics.parquet"
    if not path.exists():
        log.warning(f"Metrics file not found: {path}")
        return pd.DataFrame()
    return pd.read_parquet(path)


async def _get_current_instance_type(instance_id: str) -> Optional[str]:
    """Helper to resolve current instance type from local metrics."""
    df = load_local_metrics()
    if df.empty: return None
    match = df[df["instance_id"] == instance_id]
    if not match.empty:
        return match.iloc[0].get("instance_type")
    return None


@app.get("/instances")
async def list_instances():
    """Returns unique instance IDs and names from local metrics file."""
    df = load_local_metrics()
    if df.empty:
        return {"instances": []}

    # Deduplicate and format
    unique = df.drop_duplicates("instance_id")
    instances = []
    for _, r in unique.iterrows():
        instances.append({
            "instance_id": r["instance_id"],
            "instance_name": r.get("instance_name") or "unnamed",
            "instance_type": r.get("instance_type"),
            "az": r.get("az", "us-east-1a"),
            "platform": r.get("platform", "Linux")
        })
    return {"instances": instances}


@app.get("/fleet-summary")
async def fleet_summary(window_days: int = 30):
    """Summarized view of the EC2 fleet's utilization from local Parquet."""
    df = load_local_metrics()
    if df.empty:
        return {"window_days": window_days, "instances": []}

    # Filter by window (if window_days column existed, for now we just return the summary metrics)
    # The summary_metrics.parquet represents the aggregate already
    instances = []
    for _, r in df.iterrows():
        instances.append({
            "instance_id": r["instance_id"],
            "instance_name": r.get("instance_name") or "unnamed",
            "instance_type": r.get("instance_type"),
            "cpu_avg": round(float(r["cpu_avg_pct"]), 1) if pd.notnull(r.get("cpu_avg_pct")) else None,
            "cpu_max": round(float(r["cpu_peak_pct"]), 1) if pd.notnull(r.get("cpu_peak_pct")) else None,
            "mem_avg": round(float(r["mem_avg_pct"]), 1) if pd.notnull(r.get("mem_avg_pct")) else None,
            "mem_max": round(float(r["mem_p95_pct"]), 1) if pd.notnull(r.get("mem_p95_pct")) else None,
            "uptime_hours": r.get("uptime_hours"),
            "sample_days": r.get("sample_days")
        })
    return {"window_days": window_days, "instances": instances}


@app.post("/analyse")
async def analyse(req: AnalysisRequest):
    """
    Main analysis orchestrator.
    Redirects to the recommendation script for consistent reporting.
    """
    log.info(f"Analysis request (aliased to recommendation script): ids={req.instance_ids}")
    return await _run_script_and_stream(req.instance_ids, req.question)


@app.post("/auto-select")
async def auto_select(req: AutoSelectRequest):
    """
    Identifies candidates for rightsizing from local metrics.
    """
    df = load_local_metrics()
    if df.empty:
        return {"instance_ids": []}

    # Default candidate rules (based on recommendation_agent.py logic)
    mask = (
        (df["cpu_avg_pct"] < 15) |
        (df["cpu_p95_pct"] > 80) |
        (df["mem_p95_pct"] > 80)
    )
    candidates = df[mask]

    # Handle custom prompts if needed (simplified for local mode)
    if req.prompt and req.prompt.strip().lower() not in ["", "default"]:
        log.info(f"Custom auto-select prompt ignored in local mode: {req.prompt}")

    return {"instance_ids": candidates["instance_id"].tolist()}




@app.get("/timeseries")
async def get_timeseries(instance_id: str, window_days: int = 30):
    """Returns local historical daily metrics (currently empty)."""
    return {"instance_id": instance_id, "timeseries": []}


@app.get("/instance-metrics")
async def instance_metrics(instance_id: str, window_days: int = 30):
    """Returns local aggregated metrics for a single instance."""
    df = load_local_metrics()
    if df.empty: return {"instance_id": instance_id, "metrics": []}
    match = df[df["instance_id"] == instance_id]
    return {"instance_id": instance_id, "window_days": window_days, "metrics": match.to_dict(orient="records")}


@app.get("/pricing")
async def pricing_endpoint(instance_types: str = ""):
    """
    Returns on-demand pricing information for one or more instance types.
    Fails over to a local pricing module if live data is unavailable.
    """
    types = (
        [t.strip() for t in instance_types.split(",") if t.strip()]
        if instance_types
        else []
    )
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
        raise HTTPException(
            status_code=400,
            detail="Both current_type and recommended_type are required.",
        )

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


class ParseRecommendationsRequest(BaseModel):
    markdown_text: str
    instances: list[dict]


@app.post("/parse-recommendations")
async def parse_recommendations_endpoint(req: ParseRecommendationsRequest):
    """
    Parse LLM markdown output server-side to extract recommended instances.
    More resilient than relying on client-side JS parsing.
    """
    try:
        rec_map = SavingsTracker.parse_recommendations(req.markdown_text, req.instances)

        # Convert dictionary map to a flat list for the frontend
        results = []
        for iid, data in rec_map.items():
            results.append(
                {
                    "instance_id": iid,
                    "recommended_type": data.get("recommended_type"),
                    "saving": data.get("saving"),
                    "current_price": data.get("current_price"),
                    "recommended_price": data.get("recommended_price"),
                }
            )

        return {"recommendations": results}
    except Exception as e:
        log.error(f"Failed to parse recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pricing/compare-by-instance")
async def compare_pricing_by_instance_endpoint(req: CompareCostByInstanceRequest):
    """
    Compare monthly cost using current type from Postgres + recommended type from request.
    Includes uptime-based cost when uptime data is available.
    """
    instance_id = req.instance_id.strip()
    recommended_type = req.recommended_type.strip()
    if not instance_id or not recommended_type:
        raise HTTPException(
            status_code=400,
            detail="Both instance_id and recommended_type are required.",
        )

    current_type = await _get_current_instance_type(instance_id)
    if not current_type:
        raise HTTPException(
            status_code=404,
            detail=f"Could not resolve current instance_type for instance_id: {instance_id}",
        )

    normalized_recommended_type = _extract_instance_type(recommended_type)
    if not normalized_recommended_type:
        if recommended_type.lower() in ("no change", "keep", "same"):
            normalized_recommended_type = current_type
        else:
            return {
                "instance_id": instance_id,
                "current_type": current_type,
                "recommended_type": None,
                "skipped": True,
                "skip_reason": "No valid EC2 instance type token found in recommendation text.",
                "raw_recommended_text": recommended_type,
            }

    region = normalize_region(req.region or os.getenv("AWS_REGION"))

    # Lookup uptime_hours for this instance from local data
    uptime_hours_val: Optional[float] = None
    df = load_local_metrics()
    if not df.empty:
        match = df[df["instance_id"] == instance_id]
        if not match.empty:
            uptime_hours_val = float(match.iloc[0].get("uptime_hours", 0))

    try:
        result = await compare_instance_costs(
            current_type=current_type,
            recommended_type=normalized_recommended_type,
            region=region,
            uptime_hours=uptime_hours_val,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log.error(f"Pricing compare-by-instance failed: {e}")
        raise HTTPException(
            status_code=500, detail=f"Pricing compare-by-instance error: {e}"
        )

    return {
        "instance_id": instance_id,
        "current_type": current_type,
        "recommended_type": normalized_recommended_type,
        "skipped": False,
        **result,
    }


# ── Uptime ────────────────────────────────────────────────────────


@app.get("/uptime")
async def get_uptime_fleet(window_days: int = 30):
    """Returns uptime data from local metrics."""
    df = load_local_metrics()
    if df.empty:
        return {"window_days": window_days, "uptime": []}

    rows = []
    for _, r in df.iterrows():
        rows.append({
            "instance_id": r["instance_id"],
            "instance_name": r.get("instance_name") or "unnamed",
            "instance_type": r.get("instance_type"),
            "uptime_hours": r.get("uptime_hours", 0),
            "sample_days": r.get("sample_days", 0)
        })

    priced_uptime = get_uptime_pricing_table(rows, os.getenv("AWS_REGION", "us-east-1"))
    return {"window_days": window_days, "uptime": priced_uptime}


@app.get("/uptime/{instance_id}")
async def get_uptime_instance(instance_id: str):
    """Returns uptime data for a single instance from local metrics."""
    df = load_local_metrics()
    if df.empty: return {"instance_id": instance_id, "uptime": []}

    match = df[df["instance_id"] == instance_id]
    if match.empty:
        return {"instance_id": instance_id, "uptime": []}

    r = match.iloc[0]
    row = {
        "instance_id": r["instance_id"],
        "instance_name": r.get("instance_name") or "unnamed",
        "instance_type": r.get("instance_type"),
        "uptime_hours": r.get("uptime_hours", 0),
        "sample_days": r.get("sample_days", 0)
    }

    priced_uptime = get_uptime_pricing_table([row], os.getenv("AWS_REGION", "us-east-1"))
    return {"instance_id": instance_id, "uptime": priced_uptime}


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
    Allows side-by-side time-series comparison for up to 6 instances.
    Returns a pivoted series where each date has CPU/Mem for all requested IDs.
    """
    instance_ids = [i.strip() for i in ids.split(",") if i.strip()]
    if not instance_ids:
        raise HTTPException(
            status_code=400, detail="At least one instance_id is required."
        )
    if len(instance_ids) > 6:
        raise HTTPException(
            status_code=400, detail="Maximum 6 instances allowed for comparison."
        )

    # Timeseries comparison (currently returns empty in local mode)
    rows = []

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
        "window_days": window_days,
        "series": sorted(pivoted.values(), key=lambda x: x["date"]),
    }


async def _run_script_and_stream(instance_ids: Optional[list[str]] = None, question: Optional[str] = None):
    """Internal helper to execute the recommendation script and stream its logs."""
    import sys
    script_path = Path(__file__).parent / "agents" / "analysis" / "recommendation_agent.py"
    
    cmd = [sys.executable, str(script_path)]
    
    # Arg 1: Instance IDs
    cmd.append(",".join(instance_ids) if instance_ids else "")
    
    # Arg 2: Custom Question
    if question:
        cmd.append(question)

    log.info(f"Executing agent script: {cmd}")

    async def stream_process():
        from asyncio import subprocess as aio_sp
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=aio_sp.PIPE,
                stderr=aio_sp.STDOUT,
            )
            
            if process.stdout:
                while True:
                    line = await process.stdout.readline()
                    if not line:
                        break
                    token = line.decode()
                    if token:
                        yield f"data: {json.dumps({'token': token})}\n\n"
            
            await process.wait()
        except Exception as e:
            yield f"data: {json.dumps({'token': f'Error during agent execution: {e}\n'})}\n\n"
        
        yield "data: [DONE]\n\n"

    return StreamingResponse(
        stream_process(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )


@app.post("/run-recommendation-agent")
async def run_recommendation_agent_endpoint(req: AnalysisRequest):
    """Explicitly triggers the structured recommendation agent script."""
    return await _run_script_and_stream(req.instance_ids, req.question)


# ── Savings Routes ────────────────────────────────────────────────


@app.post("/savings", status_code=201)
async def create_saving(entry: SavingsEntry):
    """Persists a specific rightsizing recommendation to the database for tracking."""
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
        current_monthly_price_usd=entry.current_monthly_price_usd,
        recommended_monthly_price_usd=entry.recommended_monthly_price_usd,
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


@app.get("/recommendations")
async def get_recommendations():
    """
    Reads the local recommendations.parquet file produced by scripts/recommendation_agent.py
    and returns all rows as JSON. No database involved.
    """
    rec_file = Path(__file__).parent / "data" / "recommendations.parquet"
    if not rec_file.exists():
        return {"recommendations": [], "summary": {}}

    try:
        df = pd.read_parquet(rec_file)
        # Use to_json then json.loads to robustly handle NaN/NaT values
        records = json.loads(df.to_json(orient="records", date_format="iso"))

        # Build a simple summary from the data
        total_saving = sum(
            float(r["estimated_monthly_saving_usd"] or 0)
            for r in records
        )
        action_counts: dict = {}
        for r in records:
            a = r.get("rightsizing_action", "unknown") or "unknown"
            action_counts[a] = action_counts.get(a, 0) + 1

        return {
            "recommendations": records,
            "summary": {
                "total_candidates": len(records),
                "total_estimated_saving_usd": round(total_saving, 2),
                "action_counts": action_counts,
            }
        }
    except Exception as e:
        log.error(f"Failed to read recommendations.parquet: {e}")
        raise HTTPException(status_code=500, detail=str(e))


