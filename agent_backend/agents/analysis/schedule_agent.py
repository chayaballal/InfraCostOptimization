"""
schedule_agent.py — Schedule-Aware EC2 Rightsizing Agent

Pipeline:
  1. Read  → agent_backend/data/schedule_instances.json  (8 pre-classified instances)
  2. Load  → agent_backend/data/pricing_cache.csv         (per-hour pricing)
  3. Prompt → Build per-instance prompt with pattern + metrics
  4. Analyse → Send to Groq LLM, get structured JSON recommendation
  5. Override → Recalculate all costs from pricing_cache.csv (LLM only picks instance types)
  6. Store  → Save to schedule_recommendations.parquet + schedule_recommendations.json

Run:
    cd /home/surana/ec2-cloudwatch-metrics-extract
    source .venv/bin/activate
    python -m agent_backend.agents.analysis.schedule_agent
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timezone, date
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

BASE_DIR = Path(__file__).parent.parent.parent / "data"
INSTANCES_FILE = BASE_DIR / "schedule_instances.json"
PRICING_FILE = BASE_DIR / "pricing_cache.csv"
RECOMMENDATIONS_PARQUET = BASE_DIR / "schedule_recommendations.parquet"
RECOMMENDATIONS_JSON = BASE_DIR / "schedule_recommendations.json"

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = "llama-3.3-70b-versatile"

HOURS_PER_MONTH = 730
WEEKS_PER_MONTH = 4.33

# ──────────────────────────────────────────────────────────────────
# FALLBACK DATA (For Rate Limits / Demos)
# ──────────────────────────────────────────────────────────────────
FALLBACK_RECOMMENDATIONS = {
    "i-test-cat1": {
        "instance_id": "i-test-cat1", "category": "CAT-1", "schedule_type": "single",
        "single_recommendation": { "instance_type": "m5.large", "reason": "Stable workload fits current type perfectly." },
        "notes": "Maintaining current size as CPU is stable around 50%."
    },
    "i-test-cat2": {
        "instance_id": "i-test-cat2", "category": "CAT-2", "schedule_type": "day_of_week",
        "day_of_week_schedule": {
            "Monday": {"instance_type": "c5.2xlarge"}, "Tuesday": {"instance_type": "t3.small"},
            "Wednesday": {"instance_type": "c5.2xlarge"}, "Thursday": {"instance_type": "t3.small"},
            "Friday": {"instance_type": "c5.2xlarge"}, "Saturday": {"instance_type": "t3.nano"}, "Sunday": {"instance_type": "t3.nano"}
        },
        "notes": "Highly patterned weekday/weekend usage detected."
    },
    "i-test-cat3": {
        "instance_id": "i-test-cat3", "category": "CAT-3", "schedule_type": "time_slot",
        "time_slot_schedule": [{
            "day": "Monday", "scale_up_time": "20:45", "scale_down_time": "03:30",
            "peak_instance_type": "m5.xlarge", "idle_instance_type": "t3.nano", "crosses_midnight": True
        }, {
            "day": "Wednesday", "scale_up_time": "20:45", "scale_down_time": "03:30",
            "peak_instance_type": "m5.xlarge", "idle_instance_type": "t3.nano", "crosses_midnight": True
        }],
        "notes": "Batch processing burst pattern. Scaling down to nano during idle periods."
    },
    "i-test-cat3-xmidnight": {
        "instance_id": "i-test-cat3-xmidnight", "category": "CAT-3", "schedule_type": "time_slot",
        "time_slot_schedule": [{
            "day": "Monday", "scale_up_time": "20:45", "scale_down_time": "08:30",
            "peak_instance_type": "m5.xlarge", "idle_instance_type": "t3.nano", "crosses_midnight": True
        }],
        "notes": "Overnight pipeline detected. Scaling down after 8:30 AM."
    },
    "i-test-cat4": {
        "instance_id": "i-test-cat4", "category": "CAT-4", "schedule_type": "autoscaling",
        "autoscaling_config": {
            "base_instance_type": "t3.medium", "target_cpu_threshold_pct": 60, "scale_out_type": "m5.large",
            "reason": "Unpredictable spikes require elastic scaling rather than a fixed schedule."
        },
        "autoscaling_recommended": True
    },
    "i-test-cat5": {
        "instance_id": "i-test-cat5", "category": "CAT-5", "schedule_type": "terminate",
        "terminate_recommended": True, "notes": "No significant activity in 30 days. Candidate for termination."
    },
    "i-test-cat6": {
        "instance_id": "i-test-cat6", "category": "CAT-6", "schedule_type": "single",
        "single_recommendation": { "instance_type": "c5.2xlarge", "reason": "Workload trending up significantly. Sizing for future peak." },
        "trend_alert": True, "projected_cpu_in_4_weeks_pct": 72
    },
    "i-test-cat3-payroll": {
        "instance_id": "i-test-cat3-payroll", "category": "CAT-3", "schedule_type": "time_slot",
        "time_slot_schedule": [{
            "day": "Friday", "scale_up_time": "07:45", "scale_down_time": "18:30",
            "peak_instance_type": "m5.2xlarge", "idle_instance_type": "t3.nano", "crosses_midnight": False
        }],
        "notes": "Monthly payroll burst. Only active on Fridays."
    }
}


# ──────────────────────────────────────────────────────────────────
# PRICING LOOKUP
# ──────────────────────────────────────────────────────────────────
def load_pricing() -> dict[str, float]:
    """Load pricing_cache.csv into a dict of {instance_type: hourly_usd}."""
    if not PRICING_FILE.exists():
        log.warning(f"Pricing file not found: {PRICING_FILE}")
        return {}
    df = pd.read_csv(PRICING_FILE)
    prices = {}
    for _, row in df.iterrows():
        prices[row["instance_type"].strip()] = float(row["hourly_usd"])
    log.info(f"Loaded {len(prices)} instance type prices from {PRICING_FILE}")
    return prices


def hourly_cost(instance_type: str, prices: dict) -> float:
    """Get hourly cost from pricing_cache.csv. Returns 0 if not found."""
    cost = prices.get(instance_type, 0.0)
    if cost == 0 and instance_type:
        log.warning(f"Price not found for instance type: {instance_type}")
    return cost


def monthly_cost(instance_type: str, prices: dict) -> float:
    """Monthly cost = hourly × 730 hours."""
    return round(hourly_cost(instance_type, prices) * HOURS_PER_MONTH, 2)


# ──────────────────────────────────────────────────────────────────
# STEP 1: READ INSTANCES
# ──────────────────────────────────────────────────────────────────
def read_instances() -> list[dict]:
    """Load the mock schedule instances JSON."""
    if not INSTANCES_FILE.exists():
        raise FileNotFoundError(f"Schedule instances file not found: {INSTANCES_FILE}")
    with open(INSTANCES_FILE) as f:
        instances = json.load(f)
    log.info(f"Loaded {len(instances)} schedule instances from {INSTANCES_FILE}")
    return instances


# ──────────────────────────────────────────────────────────────────
# STEP 2: BUILD PROMPTS
# ──────────────────────────────────────────────────────────────────
def build_system_prompt(prices: dict) -> str:
    """Build the system prompt with pricing table from pricing_cache.csv."""
    # Build pricing reference from actual CSV data
    pricing_lines = []
    for itype, hr in sorted(prices.items()):
        mo = round(hr * HOURS_PER_MONTH, 2)
        pricing_lines.append(f"  {itype:20s} = ${hr}/hr  (~${mo}/mo)")
    pricing_table = "\n".join(pricing_lines)

    return f"""You are an AWS EC2 right-sizing expert. You receive a pre-classified EC2 instance record
with its usage pattern category and metrics. Your job is to recommend the most
cost-efficient EC2 configuration.

Output ONLY a valid JSON object. No explanation, no markdown, no preamble.

Rules by category:
- CAT-1 (Stable):
    Recommend a single instance type for all 7 days.
    Output: schedule_type = "single", populate single_recommendation only.

- CAT-2 (Day-Patterned):
    Recommend a different instance type per day of week based on that day's CPU avg.
    Output: schedule_type = "day_of_week", populate day_of_week_schedule with all 7 days.
    Each day entry must include: instance_type.

- CAT-3 (Periodic Burst):
    Recommend a time-slot schedule per active day.
    For each active day: idle type for off-hours, peak type during job window.
    Use scale_up_time = job start - 15 min (pre-warm).
    Use scale_down_time = p95_scale_down_time from the instance record.
    Output: schedule_type = "time_slot", populate time_slot_schedule array.
    Each entry must include: day, scale_up_time, scale_down_time, peak_instance_type,
    idle_instance_type, crosses_midnight.

- CAT-4 (Unpredictable Spiky):
    Recommend a peak-sized type to handle worst-case CPU.
    Set autoscaling_recommended = true with target_cpu_threshold_pct = 60.
    Output: schedule_type = "autoscaling", populate autoscaling_config.

- CAT-5 (Zombie):
    Set terminate_recommended = true.
    Output: schedule_type = "terminate".

- CAT-6 (Trending):
    Calculate Projected CPU = overall_cpu_avg + (trend_slope_pct_per_week × 4).
    IMPORTANT: For trending workloads, NEVER recommend burstable types (like t3/t4g) because sustained growth will exhaust CPU credits. ALWAYS prefer a compute-optimised (c5) or general (m5) equivalent (e.g., c5.xlarge instead of t3.xlarge).
    CONSERVATIVE RULE: If the workload is trending, choose a size that ensures the PROJECTED CPU on the NEW type stays below 60% with headroom. If downsizing would push projected CPU > 60% on the smaller type, DO NOT downsize; stay at the current type or keep a larger size.
    Set trend_alert = true.
    Output: schedule_type = "single", populate single_recommendation.

CPU → instance type sizing guide (relative to NEW type capacity):
  < 10%  → t3.nano or t3.micro
  10-25% → t3.small or t3.medium
  25-45% → t3.large or t3.medium (If Trending: stick to c5.large or stay at current)
  45-65% → m5.large or c5.large
  65-80% → m5.xlarge or c5.xlarge
  80%+   → m5.2xlarge or c5.2xlarge (compute-heavy)

NOTE: When calculating sizing, always translate the current CPU% to the target type's capacity. Example: If current i3.xlarge (4 vCPU) is @30% CPU, moving to a large (2 vCPU) means it will be @60% CPU before any trend growth. For trending loads, avoid downsizing if it leads to >60% projected load.

Instance type pricing reference (from pricing_cache.csv):
{pricing_table}

IMPORTANT: Only recommend instance types that appear in the pricing reference above.
Do NOT invent costs — just pick the instance type. Costs will be calculated externally.

Required JSON structure:
{{
  "instance_id": "string",
  "category": "CAT-1",
  "schedule_type": "single | day_of_week | time_slot | autoscaling | terminate",

  "single_recommendation": {{
    "instance_type": "string or null",
    "reason": "string"
  }},

  "day_of_week_schedule": {{
    "Monday":    {{ "instance_type": "string" }},
    "Tuesday":   {{ "instance_type": "string" }},
    "Wednesday": {{ "instance_type": "string" }},
    "Thursday":  {{ "instance_type": "string" }},
    "Friday":    {{ "instance_type": "string" }},
    "Saturday":  {{ "instance_type": "string" }},
    "Sunday":    {{ "instance_type": "string" }}
  }},

  "time_slot_schedule": [
    {{
      "day": "Monday",
      "scale_up_time": "20:45",
      "scale_down_time": "03:30",
      "peak_instance_type": "m5.xlarge",
      "idle_instance_type": "t3.nano",
      "crosses_midnight": true
    }}
  ],

  "autoscaling_config": {{
    "base_instance_type": "string or null",
    "target_cpu_threshold_pct": 60,
    "scale_out_type": "string or null",
    "reason": "string"
  }},

  "autoscaling_recommended": false,
  "terminate_recommended": false,
  "trend_alert": false,
  "projected_cpu_in_4_weeks_pct": null,

  "risk_flags": [],
  "notes": "string"
}}

Return ONLY the JSON object. No markdown. No explanation."""


def build_user_prompt(instance: dict) -> str:
    """Build the per-instance user prompt."""
    dow_lines = []
    for day, v in instance["day_of_week_profile"].items():
        line = f"  {day}: cpu_avg={v['cpu_avg']}%, cpu_max={v['cpu_max']}%, active={v['active']}"
        if "note" in v:
            line += f"  # {v['note']}"
        dow_lines.append(line)
    dow = "\n".join(dow_lines)

    windows = json.dumps(instance["job_windows"], indent=4) if instance["job_windows"] else "  None"
    p95 = json.dumps(instance["p95_scale_down_times"], indent=4) if instance["p95_scale_down_times"] else "  None"

    metrics = instance["metrics"]
    extra_metrics = ""
    if "trend_weekly_snapshots" in metrics:
        extra_metrics += f"\n  trend_weekly_snapshots: {metrics['trend_weekly_snapshots']}"
    if "burst_pattern_note" in metrics:
        extra_metrics += f"\n  burst_pattern_note: {metrics['burst_pattern_note']}"

    return f"""Instance: {instance['instance_id']} ({instance['instance_name']})
Current type: {instance['current_type']}
Region: {instance['region']}

Pattern: {instance['category']} — {instance['category_label']}
Expected schedule type: {instance['schedule_type']}

Metrics:
  active_days_per_week:      {metrics['active_days_per_week']}
  cpu_cov:                   {metrics['cpu_cov']}
  peak_to_avg_ratio:         {metrics['peak_to_avg_ratio']}
  trend_slope_pct_per_week:  {metrics['trend_slope_pct_per_week']}
  overall_cpu_avg:           {metrics['overall_cpu_avg']}%
  overall_cpu_max:           {metrics['overall_cpu_max']}%{extra_metrics}

Day-of-week CPU profile:
{dow}

Job windows detected:
{windows}

P95 scale-down times (use these as scale_down_time):
{p95}

Return only the JSON recommendation object."""


# ──────────────────────────────────────────────────────────────────
# STEP 3: CALL GROQ LLM
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
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.001,
        max_tokens=2048,
        stream=False,
    )

    raw = response.choices[0].message.content.strip()

    # Strip markdown fences if LLM adds them
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
        raw = raw.rsplit("```", 1)[0].strip()

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as e:
        log.error(f"LLM returned invalid JSON: {e}\nRaw output:\n{raw}")
        raise

    return result


# ──────────────────────────────────────────────────────────────────
# STEP 4: CALCULATE COSTS FROM PRICING_CACHE.CSV
# ──────────────────────────────────────────────────────────────────
def calculate_costs(rec: dict, instance: dict, prices: dict) -> dict:
    """
    Override all cost fields using pricing_cache.csv.
    The LLM only picks instance types — we compute all dollar values.
    """
    current_type = instance["current_type"]
    current_monthly = monthly_cost(current_type, prices)
    rec["current_monthly_cost_usd"] = current_monthly

    schedule_type = rec.get("schedule_type", "")

    if schedule_type == "single":
        rec_type = (rec.get("single_recommendation") or {}).get("instance_type")
        if rec_type:
            new_monthly = monthly_cost(rec_type, prices)
            rec["new_monthly_cost_usd"] = new_monthly
        else:
            rec["new_monthly_cost_usd"] = current_monthly

    elif schedule_type == "day_of_week":
        schedule = rec.get("day_of_week_schedule") or {}
        weekly_cost = 0.0
        for day_name, day_data in schedule.items():
            itype = day_data.get("instance_type", current_type)
            daily_cost = round(hourly_cost(itype, prices) * 24, 2)
            day_data["estimated_daily_cost_usd"] = daily_cost
            weekly_cost += daily_cost
        rec["new_monthly_cost_usd"] = round(weekly_cost * WEEKS_PER_MONTH, 2)

    elif schedule_type == "time_slot":
        slots = rec.get("time_slot_schedule") or []
        total_weekly_cost = 0.0
        for slot in slots:
            peak_type = slot.get("peak_instance_type", current_type)
            idle_type = slot.get("idle_instance_type", "t3.nano")

            # Calculate hours from time strings
            scale_up = slot.get("scale_up_time", "00:00")
            scale_down = slot.get("scale_down_time", "24:00")
            crosses = slot.get("crosses_midnight", False)

            up_h, up_m = map(int, scale_up.split(":"))
            up_decimal = up_h + up_m / 60.0

            down_h, down_m = map(int, scale_down.split(":"))
            down_decimal = down_h + down_m / 60.0

            if crosses:
                peak_hours = (24 - up_decimal) + down_decimal
            else:
                peak_hours = down_decimal - up_decimal

            idle_hours = 24 - peak_hours
            if idle_hours < 0:
                idle_hours = 0

            slot["peak_hours_per_day"] = round(peak_hours, 2)
            slot["idle_hours_per_day"] = round(idle_hours, 2)

            peak_cost_day = round(hourly_cost(peak_type, prices) * peak_hours, 2)
            idle_cost_day = round(hourly_cost(idle_type, prices) * idle_hours, 2)
            slot["peak_cost_per_day_usd"] = peak_cost_day
            slot["idle_cost_per_day_usd"] = idle_cost_day
            slot["total_cost_per_day_usd"] = round(peak_cost_day + idle_cost_day, 2)

            total_weekly_cost += peak_cost_day + idle_cost_day

        # For non-active days (7 - len(slots)), assume idle type for full 24h
        active_days = len(slots)
        idle_days = 7 - active_days
        if idle_days > 0 and slots:
            idle_type = slots[0].get("idle_instance_type", "t3.nano")
            idle_day_cost = hourly_cost(idle_type, prices) * 24
            total_weekly_cost += idle_day_cost * idle_days

        rec["new_monthly_cost_usd"] = round(total_weekly_cost * WEEKS_PER_MONTH, 2)

    elif schedule_type == "autoscaling":
        config = rec.get("autoscaling_config") or {}
        base_type = config.get("base_instance_type", current_type)
        new_monthly = monthly_cost(base_type, prices)
        rec["new_monthly_cost_usd"] = new_monthly

    elif schedule_type == "terminate":
        rec["new_monthly_cost_usd"] = 0.0

    else:
        rec["new_monthly_cost_usd"] = current_monthly

    # Compute savings
    new_monthly = rec.get("new_monthly_cost_usd", current_monthly)
    saving = round(current_monthly - new_monthly, 2)
    rec["estimated_monthly_saving_usd"] = saving
    rec["saving_pct"] = round((saving / current_monthly) * 100, 1) if current_monthly > 0 else 0.0

    return rec


# ──────────────────────────────────────────────────────────────────
# STEP 5: SAVE RECOMMENDATIONS
# ──────────────────────────────────────────────────────────────────
def save_recommendations(results: list[dict], instances: list[dict]) -> pd.DataFrame:
    """Save recommendations to Parquet + JSON."""
    now = datetime.now(timezone.utc).isoformat()
    today = date.today().isoformat()

    # Enrich results with instance metadata
    inst_map = {i["instance_id"]: i for i in instances}
    for rec in results:
        iid = rec.get("instance_id", "")
        inst = inst_map.get(iid, {})
        rec["instance_name"] = inst.get("instance_name", "")
        rec["current_type"] = inst.get("current_type", "")
        rec["region"] = inst.get("region", "us-east-1")
        rec["category_label"] = inst.get("category_label", "")
        rec["recommendation_date"] = today
        rec["analysed_at"] = now

    # Save full JSON (preserves nested structures for frontend)
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    with open(RECOMMENDATIONS_JSON, "w") as f:
        json.dump(results, f, indent=2)
    log.info(f"Saved {len(results)} recommendations → {RECOMMENDATIONS_JSON}")

    # Build flat rows for Parquet (summary view)
    rows = []
    for rec in results:
        row = {
            "instance_id": rec.get("instance_id"),
            "instance_name": rec.get("instance_name"),
            "current_type": rec.get("current_type"),
            "category": rec.get("category"),
            "category_label": rec.get("category_label"),
            "schedule_type": rec.get("schedule_type"),
            "current_monthly_cost_usd": rec.get("current_monthly_cost_usd"),
            "new_monthly_cost_usd": rec.get("new_monthly_cost_usd"),
            "estimated_monthly_saving_usd": rec.get("estimated_monthly_saving_usd"),
            "saving_pct": rec.get("saving_pct"),
            "autoscaling_recommended": rec.get("autoscaling_recommended", False),
            "terminate_recommended": rec.get("terminate_recommended", False),
            "trend_alert": rec.get("trend_alert", False),
            "projected_cpu_in_4_weeks_pct": rec.get("projected_cpu_in_4_weeks_pct"),
            "notes": rec.get("notes", ""),
            "recommendation_date": rec.get("recommendation_date"),
            "analysed_at": rec.get("analysed_at"),
        }

        # Extract recommended type for summary
        st = rec.get("schedule_type", "")
        if st == "single":
            row["recommended_type"] = (rec.get("single_recommendation") or {}).get("instance_type")
        elif st == "autoscaling":
            row["recommended_type"] = (rec.get("autoscaling_config") or {}).get("base_instance_type")
        elif st == "terminate":
            row["recommended_type"] = "TERMINATE"
        elif st == "day_of_week":
            row["recommended_type"] = "7-day schedule"
        elif st == "time_slot":
            row["recommended_type"] = "time-slot schedule"
        else:
            row["recommended_type"] = None

        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_parquet(RECOMMENDATIONS_PARQUET, index=False, engine="pyarrow", compression="snappy")
    log.info(f"Saved {len(df)} rows → {RECOMMENDATIONS_PARQUET}")

    # Also save CSV for easy inspection
    csv_path = RECOMMENDATIONS_PARQUET.with_suffix(".csv")
    df.to_csv(csv_path, index=False)
    log.info(f"Saved CSV → {csv_path}")

    return df


def print_summary(results: list[dict]):
    """Print a summary table to console."""
    print("```")  # close log block
    print()
    print("---")
    print()
    print("## Schedule-Aware Recommendation Summary")
    print()

    total_current = sum(r.get("current_monthly_cost_usd", 0) for r in results)
    total_new = sum(r.get("new_monthly_cost_usd", 0) for r in results)
    total_saving = sum(r.get("estimated_monthly_saving_usd", 0) for r in results)

    print("| Metric | Value |")
    print("| :--- | :--- |")
    print(f"| **Total instances** | {len(results)} |")
    print(f"| **Current monthly cost** | ${total_current:,.2f} |")
    print(f"| **Projected monthly cost** | ${total_new:,.2f} |")
    print(f"| **Est. Monthly Saving** | **${total_saving:,.2f}** |")
    print(f"| **Est. Annual Saving** | **${total_saving * 12:,.2f}** |")
    print()
    print("### Per-Instance Breakdown")
    print()
    headers = ["Instance", "Name", "Category", "Current", "Schedule", "Current $/mo", "New $/mo", "Saving"]
    print("| " + " | ".join(headers) + " |")
    print("| " + " | ".join(["---"] * len(headers)) + " |")

    for r in results:
        vals = [
            r.get("instance_id", ""),
            r.get("instance_name", ""),
            r.get("category", ""),
            r.get("current_type", ""),
            r.get("schedule_type", ""),
            f"${r.get('current_monthly_cost_usd', 0):,.2f}",
            f"${r.get('new_monthly_cost_usd', 0):,.2f}",
            f"${r.get('estimated_monthly_saving_usd', 0):,.2f} ({r.get('saving_pct', 0):.0f}%)",
        ]
        print("| " + " | ".join(vals) + " |")

    print()
    print("**Process complete.** View the full results in the **Schedule** tab.")


# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────
async def main():
    print("### System Logs")
    print("---")
    print("```text")  # Open log block
    log.info("  Schedule-Aware Recommendation Agent — Starting")

    # 1. Load data
    instances = read_instances()
    prices = load_pricing()

    if not prices:
        log.error("No pricing data loaded. Cannot calculate costs.")
        return

    # 2. Build system prompt (once, shared across all calls)
    system_prompt = build_system_prompt(prices)

    # 3. Process each instance
    results = []
    for instance in instances:
        iid = instance["instance_id"]
        cat = instance["category"]
        log.info(f"Processing {iid} ({cat} — {instance['category_label']})...")

        user_prompt = build_user_prompt(instance)

        try:
            rec = await call_llm(system_prompt, user_prompt)
        except Exception as e:
            log.warning(f"  LLM call failed for {iid}: {e}")
            if iid in FALLBACK_RECOMMENDATIONS:
                log.info(f"  Using hardcoded fallback for {iid} (LLM Rate Limit / Error).")
                rec = FALLBACK_RECOMMENDATIONS[iid].copy()
            else:
                log.info(f"  Retrying {iid}...")
                try:
                    rec = await call_llm(system_prompt, user_prompt)
                except Exception as e2:
                    log.error(f"  Retry also failed for {iid}: {e2}. Skipping.")
                    continue

        # 4. Override costs from pricing_cache.csv
        rec = calculate_costs(rec, instance, prices)

        results.append(rec)
        log.info(
            f"  → {rec['schedule_type']} | "
            f"${rec.get('current_monthly_cost_usd', 0):.2f} → ${rec.get('new_monthly_cost_usd', 0):.2f} | "
            f"saving ${rec.get('estimated_monthly_saving_usd', 0):.2f}/mo"
        )

    # 5. Save
    save_recommendations(results, instances)

    # 6. Print summary
    log.info("Done.")
    print_summary(results)


if __name__ == "__main__":
    asyncio.run(main())
