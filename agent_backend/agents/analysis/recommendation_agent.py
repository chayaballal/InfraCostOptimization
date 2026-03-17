"""
recommendation_agent.py — Local-file-based EC2 Rightsizing Agent

Pipeline:
  1. Read  → agent_backend/data/summary_metrics.parquet
  2. Filter → Select candidate instances (under/over-utilised)
  3. Prompt → Format metrics as markdown table for LLM
  4. Analyse → Send to Groq LLM, get structured JSON recommendations
  5. Store  → Save results to agent_backend/data/recommendations.parquet

Run:
    cd /home/surana/ec2-cloudwatch-metrics-extract
    source .venv/bin/activate
    python scripts/recommendation_agent.py

Requirements (already in pyproject.toml):
    groq, pandas, pyarrow, python-dotenv
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from groq import AsyncGroq

# ──────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Paths — all local
BASE_DIR         = Path(__file__).parent.parent.parent / "data"
SUMMARY_FILE     = BASE_DIR / "summary_metrics.parquet"
RECOMMENDATIONS_FILE = BASE_DIR / "recommendations.parquet"
PRICING_FILE     = BASE_DIR / "pricing_cache.csv"

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL   = "llama-3.3-70b-versatile"

# ──────────────────────────────────────────────────────────────────
# STEP 1: READ SUMMARY METRICS
# ──────────────────────────────────────────────────────────────────
def read_summary_metrics() -> pd.DataFrame:
    """Load the local summary_metrics.parquet file."""
    if not SUMMARY_FILE.exists():
        raise FileNotFoundError(
            f"Summary file not found: {SUMMARY_FILE}\n"
            "Run: python scripts/generate_summary_metrics.py first."
        )
    df = pd.read_parquet(SUMMARY_FILE)
    log.info(f"Loaded {len(df)} instances from {SUMMARY_FILE}")
    return df


# ──────────────────────────────────────────────────────────────────
# STEP 2: FILTER CANDIDATES
# ──────────────────────────────────────────────────────────────────
CANDIDATE_RULES = {
    "zombie":          "cpu_avg_pct < 1",
    "under_utilised":  "cpu_avg_pct < 15",
    "cpu_stressed":    "cpu_p95_pct > 80",
    "mem_pressured":   "mem_p95_pct > 80",
}

def filter_candidates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only instances that match at least one candidate rule.
    Healthy instances (none of the rules triggered) are excluded
    so we don't waste LLM tokens on them.
    """
    mask = (
        (df["cpu_avg_pct"] < 15) |
        (df["cpu_p95_pct"] > 80) |
        (df["mem_p95_pct"] > 80)
    )
    candidates = df[mask].copy()

    # Tag each candidate with which rule triggered it
    def tag_reason(row):
        reasons = []
        if row["cpu_avg_pct"] < 1:
            reasons.append("zombie")
        elif row["cpu_avg_pct"] < 15:
            reasons.append("under_utilised")
        if row["cpu_p95_pct"] > 80:
            reasons.append("cpu_stressed")
        if row["mem_p95_pct"] > 80:
            reasons.append("mem_pressured")
        return ", ".join(reasons)

    candidates["candidate_reason"] = candidates.apply(tag_reason, axis=1)

    log.info(
        f"Candidate filter: {len(candidates)}/{len(df)} instances selected for analysis"
    )
    for _, row in candidates.iterrows():
        log.info(
            f"  -> {row['instance_id']} ({row['instance_type']}) "
            f"cpu_avg={row['cpu_avg_pct']}% cpu_p95={row['cpu_p95_pct']}% "
            f"mem_p95={row['mem_p95_pct']}% | Reason: {row['candidate_reason']}"
        )

    return candidates


# ──────────────────────────────────────────────────────────────────
# STEP 3: FORMAT METRICS AS LLM PROMPT
# ──────────────────────────────────────────────────────────────────
def build_system_prompt() -> str:
    return """You are an expert AWS Cloud Architect and FinOps analyst specialising in EC2 rightsizing.

You will receive a table of EC2 instance utilisation metrics. 

Your JSON response MUST include a "narrative" field (2-3 sentence executive summary) and an "instances" list.

Analyse each instance and respond ONLY with a valid JSON object. Do NOT include markdown fences, prose, or any text outside the JSON.

## MANDATORY RULES (apply in this exact order):

### 1. INSUFFICIENT DATA
If sample_days < 7:
  - rightsizing_action MUST be "insufficient_data"
  - confidence MUST be "low"

### 2. ZOMBIE CHECK
If cpu_avg_pct < 1.0:
  - rightsizing_action MUST be "terminate"
  - Add flag: zombie with severity HIGH

### 3. RIGHTSIZING ACTION — use EXACTLY one value:
  "downsize"          → same family, smaller size
  "upsize"            → same family, larger size
  "change_family"     → different instance family
  "keep"              → currently optimal
  "terminate"         → idle/unused instance
  "insufficient_data" → sample_days < 7

### 4. FAMILY CHANGE RULES:
  - mem_p95_pct > 85% AND cpu_avg_pct < 40% → change_family to r5/r6i (memory-optimised)
  - cpu_p95_pct > 80% AND mem_avg_pct < 60% → upsize within c5/c6i (compute-optimised)

### 5. RISK FLAGS (use exact flag names):
  "cpu_high"         → cpu_p95_pct > 80%  (CRITICAL if > 90%, else HIGH)
  "zombie"           → cpu_avg_pct < 1.0  (HIGH)
  "memory_pressure"  → mem_p95_pct > 85%  (CRITICAL if > 95%, else HIGH)
  "low_sample_days"  → sample_days < 7    (MEDIUM)

### 6. CONFIDENCE:
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
      "rightsizing_action": "downsize",
      "rightsizing_reason": "one sentence with exact metric values",
      "estimated_monthly_saving_usd": 12.50, # Use NEGATIVE values if the recommended type costs MORE than current.
      "confidence": "high | medium | low",
      "risk_flags": [
        {
          "flag": "zombie | cpu_high | memory_pressure | low_sample_days",
          "severity": "CRITICAL | HIGH | MEDIUM | LOW",
          "detail": "short explanation with metric values"
        }
      ]
    }
  ],
  "summary": {
    "total_instances": 0,
    "instances_to_downsize": 0,
    "instances_to_upsize": 0,
    "instances_to_change_family": 0,
    "instances_to_terminate": 0,
    "instances_healthy": 0,
    "instances_insufficient_data": 0,
    "total_estimated_saving_usd": 0.0,
    "critical_risks": 0
  },
  "narrative": "A 2-3 sentence professional executive summary of the findings, mentioning key trends or major savings opportunities."
}

Return ONLY the JSON object. No markdown. No explanation."""


def build_user_prompt(df: pd.DataFrame, custom_prompt: str = None) -> str:
    """Convert candidate DataFrame rows and pricing info into a prompt for the LLM."""
    
    # 1. Build metrics table
    lines = [
        f"Analyse the following {len(df)} EC2 candidate instances:\n",
        "| Instance ID | Name | Type | CPU Avg% | CPU P95% | Mem Avg% | Mem P95% | Uptime (h) | Sample Days |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for _, r in df.iterrows():
        lines.append(
            f"| {r['instance_id']} | {r['instance_name']} | {r['instance_type']} "
            f"| {r['cpu_avg_pct']} | {r['cpu_p95_pct']} "
            f"| {r['mem_avg_pct']} | {r['mem_p95_pct']} "
            f"| {r['uptime_hours']} | {r['sample_days']} |"
        )
    
    metrics_block = "\n".join(lines)

    # 2. Build pricing reference table from local CSV
    pricing_lines = [
        "\n### EC2 Price Reference (Linux On-Demand)\n",
        "| Instance Type | $/hr | $/mo (730h) |",
        "|---|---|---|",
    ]
    if PRICING_FILE.exists():
        try:
            pdf = pd.read_csv(PRICING_FILE)
            for _, r in pdf.iterrows():
                h = float(r["hourly_usd"])
                pricing_lines.append(f"| {r['instance_type']} | {h:.4f} | {h*730:.2f} |")
        except Exception:
            pricing_lines.append("| (Data load error) | | |")
    else:
        pricing_lines.append("| (No pricing file found) | | |")
    
    pricing_block = "\n".join(pricing_lines)

    # 3. Combine
    prompt = f"{metrics_block}\n\n{pricing_block}"
    if custom_prompt:
        prompt += f"\n\n---\n**Additional user question/context:** {custom_prompt}"
    
    return prompt


# ──────────────────────────────────────────────────────────────────
# STEP 4: CALL GROQ LLM
# ──────────────────────────────────────────────────────────────────
async def call_llm(system_prompt: str, user_prompt: str) -> dict:
    """Send prompt to Groq and parse the JSON response."""
    if not GROQ_API_KEY:
        raise EnvironmentError("GROQ_API_KEY not set in .env")

    client = AsyncGroq(api_key=GROQ_API_KEY)

    log.info(f"Sending {len(user_prompt)} chars to Groq ({GROQ_MODEL})...")

    response = await client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        temperature=0.001,
        max_tokens=4096,
        stream=False,
    )

    raw = response.choices[0].message.content.strip()

    # Strip markdown fences if the LLM adds them anyway
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
        raw = raw.rsplit("```", 1)[0].strip()

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as e:
        log.error(f"LLM returned invalid JSON: {e}\nRaw output:\n{raw}")
        raise

    log.info(
        f"LLM response received: {result['summary']['total_instances']} instances analysed"
    )
    return result


# ──────────────────────────────────────────────────────────────────
# STEP 5: STORE RECOMMENDATIONS
# ──────────────────────────────────────────────────────────────────
def save_recommendations(llm_result: dict, candidates_df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten the LLM JSON response into a DataFrame and save as
    agent_backend/data/recommendations.parquet.
    """
    now = datetime.now(timezone.utc).isoformat()
    rows = []

    # Build a lookup for candidate metadata
    meta = candidates_df.set_index("instance_id").to_dict("index")

    # Load pricing lookup for validation
    prices = {}
    if PRICING_FILE.exists():
        try:
            pdf = pd.read_csv(PRICING_FILE)
            for _, r in pdf.iterrows():
                prices[r["instance_type"]] = float(r["hourly_usd"])
        except Exception as e:
            log.warning(f"Could not load pricing for validation: {e}")

    for inst in llm_result.get("instances", []):
        iid = inst.get("instance_id", "")
        m = meta.get(iid, {})

        # Flatten risk flags into a JSON string
        risk_flags_str = json.dumps(inst.get("risk_flags", []))

        row = {
            "instance_id":                 iid,
            "instance_name":               inst.get("instance_name", m.get("instance_name")),
            "current_type":                inst.get("current_type", m.get("instance_type")),
            "recommended_type":            inst.get("recommended_type"),
            "rightsizing_action":          inst.get("rightsizing_action"),
            "rightsizing_reason":          inst.get("rightsizing_reason"),
            "estimated_monthly_saving_usd": inst.get("estimated_monthly_saving_usd"),
            "confidence":                  inst.get("confidence"),
            "risk_flags":                  risk_flags_str,
            "candidate_reason":            m.get("candidate_reason", ""),
            "cpu_avg_pct":                 m.get("cpu_avg_pct"),
            "cpu_p95_pct":                 m.get("cpu_p95_pct"),
            "mem_p95_pct":                 m.get("mem_p95_pct"),
            "uptime_hours":                m.get("uptime_hours"),
            "sample_days":                 m.get("sample_days"),
            "status":                      "Proposed",
            "analysed_at":                 now,
        }

        # --- PRICE VALIDATION OVERRIDE ---
        ctype = row["current_type"]
        rtype = row["recommended_type"]
        action = (row["rightsizing_action"] or "").lower()

        c_hr = prices.get(ctype)
        r_hr = prices.get(rtype) if rtype and rtype != ctype else None
        
        # If terminate, recommended price is 0
        if action == "terminate":
            r_hr = 0.0
        
        if c_hr is not None and r_hr is not None:
            # Recalculate saving to ensure accuracy
            calc_saving = round(float((c_hr - r_hr) * 730), 2)
            
            # Ensure incoming saving is a float if it exists
            incoming_saving = row.get("estimated_monthly_saving_usd")
            try:
                incoming_saving = float(incoming_saving) if incoming_saving is not None else 0.0
            except (ValueError, TypeError):
                incoming_saving = 0.0

            # If agent provided 0 but it's clearly a cost or saving, override
            if incoming_saving == 0 and calc_saving != 0:
                row["estimated_monthly_saving_usd"] = calc_saving
            # If agent provided a value but it's wildly different (>10% or >$10), override
            elif incoming_saving != 0:
                if abs(incoming_saving - calc_saving) > max(10.0, float(abs(calc_saving) * 0.1)):
                    row["estimated_monthly_saving_usd"] = calc_saving

        rows.append(row)

    # Add LLM summary as a separate attribute on the DataFrame
    df = pd.DataFrame(rows)

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(RECOMMENDATIONS_FILE, index=False, engine="pyarrow", compression="snappy")

    log.info(f"Saved {len(df)} recommendations -> {RECOMMENDATIONS_FILE}")

    # Also save a human-readable CSV alongside
    csv_path = RECOMMENDATIONS_FILE.with_suffix(".csv")
    df.to_csv(csv_path, index=False)
    log.info(f"Saved CSV copy -> {csv_path}")

    return df


def print_summary(llm_result: dict, rec_df: pd.DataFrame):
    """Print a clean summary table to the console in Markdown format."""
    s = llm_result.get("summary", {})
    narrative = llm_result.get("narrative", "")

    print("```") # Close the log code block
    print()
    print("---") # Visual separator
    print()

    if narrative:
        print("### Executive Summary")
        print(f"> {narrative}")
        print()

    print("## EC2 Rightsizing Recommendation Summary")
    print()
    print("| Metric | Value |")
    print("| :--- | :--- |")
    print(f"| **Total candidates analysed** | {s.get('total_instances', 0)} |")
    print(f"| **Downsize** | {s.get('instances_to_downsize', 0)} |")
    print(f"| **Upsize** | {s.get('instances_to_upsize', 0)} |")
    print(f"| **Change family** | {s.get('instances_to_change_family', 0)} |")
    print(f"| **Terminate (zombie)** | {s.get('instances_to_terminate', 0)} |")
    print(f"| **Keep (healthy)** | {s.get('instances_healthy', 0)} |")
    print(f"| **Insufficient data** | {s.get('instances_insufficient_data', 0)} |")
    print(f"| **Critical risks** | {s.get('critical_risks', 0)} |")
    # print(f"| **Est. Total Monthly Saving** | **${s.get('total_estimated_saving_usd', 0):.2f}** |")
    print()
    print("### Detailed Recommendations")
    print()

    if not rec_df.empty:
        # Convert to markdown table
        headers = ["Instance ID", "Name", "Current", "Recommended", "Action", "Saving/mo", "Confidence"]
        print("| " + " | ".join(headers) + " |")
        print("| " + " | ".join(["---"] * len(headers)) + " |")
        for _, row in rec_df.iterrows():
            def clean_val(v):
                if pd.isna(v) or v is None or str(v).lower() == "nan":
                    return "—"
                return str(v)

            vals = [
                clean_val(row["instance_id"]),
                clean_val(row["instance_name"]),
                clean_val(row["current_type"]),
                clean_val(row["recommended_type"]),
                clean_val(row["rightsizing_action"]).upper(),
                f"${row['estimated_monthly_saving_usd']:.2f}" if not pd.isna(row['estimated_monthly_saving_usd']) else "—",
                clean_val(row["confidence"]).capitalize()
            ]
            print("| " + " | ".join(vals) + " |")
    print()
    print("**Process complete.** View the full list in the **Recommendations** tab.")


# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────
async def main():
    print("### System Logs")
    print("---")
    print("```text") # Open log block
    log.info("  EC2 Recommendation Agent — Local File Mode")

    # 1. Read
    all_df = read_summary_metrics()

    # 2. Filter candidates and handle custom prompt
    import sys
    custom_prompt = None
    if len(sys.argv) > 1 and sys.argv[1].strip():
        # Support comma-separated list of IDs from command line
        specified_ids = [i.strip() for i in sys.argv[1].split(",") if i.strip()]
        candidates_df = all_df[all_df["instance_id"].isin(specified_ids)].copy()
        log.info(f"Using {len(candidates_df)} specified instance IDs from command line.")
    else:
        candidates_df = filter_candidates(all_df)

    if len(sys.argv) > 2 and sys.argv[2].strip():
        custom_prompt = sys.argv[2].strip()
        log.info(f"Applying custom user prompt: {custom_prompt[:50]}...")

    if candidates_df.empty:
        log.info("No candidates found — all instances appear healthy. Saving empty recommendations.")
        # Create a mock result for a healthy fleet
        llm_result = {
            "instances": [],
            "summary": {
                "total_instances": len(all_df),
                "instances_to_downsize": 0,
                "instances_to_upsize": 0,
                "instances_to_change_family": 0,
                "instances_to_terminate": 0,
                "instances_healthy": len(all_df),
                "instances_insufficient_data": 0,
                "total_estimated_saving_usd": 0.0,
                "critical_risks": 0
            },
            "narrative": "The entire fleet appears healthy and within optimal utilization ranges based on current metrics. No rightsizing actions required."
        }
        rec_df = save_recommendations(llm_result, candidates_df)
        print_summary(llm_result, rec_df)
        return

    # 3. Build prompts
    system_prompt = build_system_prompt()
    user_prompt   = build_user_prompt(candidates_df, custom_prompt)

    # 4. Call LLM
    llm_result = await call_llm(system_prompt, user_prompt)

    # 5. Save recommendations
    rec_df = save_recommendations(llm_result, candidates_df)

    # 6. Print summary
    log.info("Done.")
    print_summary(llm_result, rec_df)

if __name__ == "__main__":
    asyncio.run(main())
