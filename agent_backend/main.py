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

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(levelname)-8s │ %(message)s")
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────
# APP SETUP
# ──────────────────────────────────────────────────────────────────
app = FastAPI(title="EC2 Analysis Agent", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
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
        SELECT
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
def format_metrics_for_llm(rows: list[dict], window_days: int) -> str:
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
- Base cost estimates on AWS us-east-1 on-demand Linux pricing.
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
    """Stream tokens from Groq as Server-Sent Events."""
    stream = await groq_client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system",  "content": system_prompt},
            {"role": "user",    "content": user_prompt},
        ],
        temperature=0.3,      # lower = more consistent/factual output
        max_tokens=4096,
        stream=True,
    )

    async for chunk in stream:
        delta = chunk.choices[0].delta.content
        if delta:
            # SSE format
            yield f"data: {json.dumps({'token': delta})}\n\n"

    yield "data: [DONE]\n\n"


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


@app.post("/analyse")
async def analyse(req: AnalysisRequest):
    """
    Main endpoint — fetches metrics, formats them, streams Groq response.
    Returns a Server-Sent Event stream of tokens.
    """
    log.info(f"Analysis request: window={req.window_days}d instances={req.instance_ids or 'ALL'}")

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

    # 2. Format for LLM
    formatted_data = format_metrics_for_llm(rows, req.window_days)
    system_prompt  = build_system_prompt(req.focus)
    user_prompt    = build_user_prompt(formatted_data, req.window_days, req.question)

    log.info(f"Prompt built: {len(user_prompt)} chars | {len(rows)} instances")

    # 3. Stream Groq response
    return StreamingResponse(
        stream_groq_response(system_prompt, user_prompt),
        media_type="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*",
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

Required JSON structure:
{
  "instances": [
    {
      "instance_id": "i-xxxxx",
      "instance_name": "...",
      "current_type": "m5.large",
      "recommended_type": "t3.medium",
      "rightsizing_action": "downsize | upsize | change_family | keep | terminate | insufficient_data",
      "rightsizing_reason": "one sentence explanation",
      "risk_flags": [
        {
          "flag": "cpu_high | memory_pressure | io_throttle | status_check_failures | zombie | low_sample_days",
          "severity": "CRITICAL | HIGH | MEDIUM | LOW",
          "detail": "brief explanation"
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

Rules:
- rightsizing_action must be exactly one of the enum values listed above.
- Use "insufficient_data" if sample_days < 7.
- Use "terminate" only if CPU avg < 1% AND network is near zero.
- estimated_monthly_saving_usd should be positive for downsizes, negative for upsizes (cost increase), 0 for keep.
- Base pricing on AWS us-east-1 on-demand Linux rates.
- confidence = "low" if sample_days < 7, "medium" if 7-14 days, "high" if >= 15 days.
- Return ONLY the JSON object. No markdown. No explanation. No preamble."""


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