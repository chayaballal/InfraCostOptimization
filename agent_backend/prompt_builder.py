"""
prompt_builder.py — LLM prompt construction for the EC2 Analysis Agent.

Builds system/user prompts and formats metric data for LLM consumption.
"""

import json
from typing import Optional


class PromptBuilder:
    """Stateless helper that constructs all LLM prompts."""

    # ── Auto-select system prompt (text-to-SQL) ───────────────────

    AUTO_SELECT_SYSTEM_PROMPT = """You are an expert AWS PostgreSQL Database Administrator.
Your task is to convert a user's natural language request into a valid PostgreSQL WHERE clause filtering the view `v_ec2_llm_summary`.
The view `v_ec2_llm_summary` has the following columns:
- instance_id (varchar)
- instance_name (varchar)
- instance_type (varchar)
- az (varchar)
- platform (varchar)
- window_days (int)
- sample_days (int)
- cpu_avg_pct (numeric)
- cpu_peak_pct (numeric)
- cpu_p95_pct (numeric)
- cpu_p99_pct (numeric)
- mem_avg_pct (numeric)
- mem_peak_pct (numeric)
- mem_p95_pct (numeric)
- net_in_gb (numeric)
- net_out_gb (numeric)
- net_in_avg_mbps (numeric)
- net_out_avg_mbps (numeric)
- disk_read_gb (numeric)
- disk_write_gb (numeric)
- ebs_read_gb (numeric)
- ebs_write_gb (numeric)
- ebs_io_balance_pct (numeric)
- status_check_failures (int)

ONLY output the PostgreSQL WHERE clause condition. Do not output the SELECT statement, the word WHERE, markdown formatting, or any explanation.

Example Input: "Instances with high cpu or memory"
Example Output: cpu_p95_pct > 80 OR mem_p95_pct > 80

Example Input: "m5.large instances"
Example Output: instance_type = 'm5.large'
"""

    # ── Metric formatting ─────────────────────────────────────────

    @staticmethod
    def format_metrics(
        rows: list[dict],
        window_days: int,
        pricing_markdown: str = "",
    ) -> str:
        """Convert metric rows into a compact, LLM-readable markdown table."""
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

        lines += [
            "\n### Raw JSON (full precision)\n",
            "```json",
            json.dumps(rows, indent=2, default=str),
            "```",
        ]

        if pricing_markdown:
            lines.append(pricing_markdown)

        return "\n".join(lines)

    # ── System prompts ────────────────────────────────────────────

    @staticmethod
    def build_system_prompt(focus: list[str]) -> str:
        """Build a focused system prompt based on the selected focus areas."""
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

    # ── User prompt ───────────────────────────────────────────────

    @staticmethod
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

    # ── Eval system prompt ────────────────────────────────────────

    @staticmethod
    def build_eval_system_prompt() -> str:
        """Eval-specific system prompt — forces structured JSON output."""
        return """You are an expert AWS Cloud Architect evaluating EC2 instance efficiency.

Analyse the provided EC2 metrics and respond ONLY with a valid JSON object.
Do NOT include markdown fences, prose, or any text outside the JSON.

## MANDATORY RULES — apply in this exact order:


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
