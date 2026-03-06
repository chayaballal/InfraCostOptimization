"""
╔══════════════════════════════════════════════════════════════════╗
║   EC2 Analysis Agent — Live Instance Report                      ║
║                                                                  ║
║  Fetches real EC2 instances from the database, runs the LLM      ║
║  analysis, and produces a factor-verification report showing     ║
║  exactly which metrics drove each recommendation.                ║
║                                                                  ║
║  Run:                                                            ║
║      uv run python live_report.py                                ║
║      uv run python live_report.py --window 60                    ║
║      uv run python live_report.py --instance i-0abc123           ║
╚══════════════════════════════════════════════════════════════════╝
"""

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.rule import Rule
from rich import box
from rich.text import Text

API_BASE = "http://localhost:8000"
console  = Console()

# ── Thresholds (mirrors main.py eval system prompt) ───────────────
THRESHOLDS = {
    "cpu_avg_pct":              {"low": 5,   "high": None, "unit": "%"},
    "cpu_p95_pct":              {"low": None,"high": 80,   "unit": "%"},
    "mem_p95_pct":              {"low": None,"high": 85,   "unit": "%"},
    "ebs_io_balance_pct":       {"low": 20,  "high": None, "unit": "%"},
    "status_check_failures":    {"low": None,"high": 0,    "unit": "count"},
    "sample_days":              {"low": 7,   "high": None, "unit": "days"},
}


# ── Derive expected factors automatically from real metric values ─
def derive_expected_factors(metrics: dict) -> dict:
    """
    Given real metric values, derive what the correct primary factor
    should be using the same threshold logic as the eval system prompt.
    Returns a dict matching the deciding_factors schema in eval_scenarios.json.
    """
    cpu_avg   = metrics.get("cpu_avg_pct") or 0
    cpu_p95   = metrics.get("cpu_p95_pct") or 0
    mem_p95   = metrics.get("mem_p95_pct") or 0
    ebs_io    = metrics.get("ebs_io_balance_pct") or 100
    failures  = metrics.get("status_check_failures") or 0
    sample    = metrics.get("sample_days") or 30
    net_in    = metrics.get("net_in_gb") or 0
    net_out   = metrics.get("net_out_gb") or 0

    # Priority order matches the eval system prompt rule ordering
    if sample < 7:
        return {
            "primary_metric":        "sample_days",
            "primary_direction":     "below_threshold",
            "secondary_metrics":     [],
            "must_not_cite_as_primary": ["cpu_avg_pct", "mem_avg_pct", "cpu_p95_pct"],
            "expected_action":       "insufficient_data",
        }

    if cpu_avg < 1.0 and net_in < 0.1 and net_out < 0.1:
        return {
            "primary_metric":        "cpu_avg_pct",
            "primary_direction":     "below_threshold",
            "secondary_metrics":     ["net_in_gb", "net_out_gb"],
            "must_not_cite_as_primary": ["ebs_io_balance_pct", "mem_p95_pct"],
            "expected_action":       "terminate",
        }

    if failures > 0:
        return {
            "primary_metric":        "status_check_failures",
            "primary_direction":     "above_threshold",
            "secondary_metrics":     ["cpu_avg_pct"],
            "must_not_cite_as_primary": ["mem_avg_pct", "ebs_io_balance_pct"],
            "expected_action":       "keep_or_minor_change",
        }

    if ebs_io < 20:
        return {
            "primary_metric":        "ebs_io_balance_pct",
            "primary_direction":     "below_threshold",
            "secondary_metrics":     ["disk_read_gb", "disk_write_gb"],
            "must_not_cite_as_primary": ["cpu_avg_pct", "mem_avg_pct"],
            "expected_action":       "keep_or_upsize",
        }

    if mem_p95 > 85 and cpu_avg < 40:
        return {
            "primary_metric":        "mem_p95_pct",
            "primary_direction":     "above_threshold",
            "secondary_metrics":     ["mem_avg_pct", "mem_peak_pct"],
            "must_not_cite_as_primary": ["cpu_avg_pct", "cpu_p95_pct"],
            "expected_action":       "change_family",
        }

    if cpu_p95 > 80:
        return {
            "primary_metric":        "cpu_p95_pct",
            "primary_direction":     "above_threshold",
            "secondary_metrics":     ["cpu_avg_pct", "cpu_peak_pct"],
            "must_not_cite_as_primary": ["mem_avg_pct", "ebs_io_balance_pct"],
            "expected_action":       "upsize",
        }

    if cpu_avg < 5:
        return {
            "primary_metric":        "cpu_avg_pct",
            "primary_direction":     "below_threshold",
            "secondary_metrics":     ["mem_avg_pct", "cpu_p95_pct"],
            "must_not_cite_as_primary": ["ebs_io_balance_pct", "status_check_failures"],
            "expected_action":       "downsize",
        }

    # Healthy / well-sized
    return {
        "primary_metric":        "cpu_avg_pct",
        "primary_direction":     "below_threshold",
        "secondary_metrics":     ["mem_avg_pct"],
        "must_not_cite_as_primary": ["status_check_failures", "net_in_gb"],
        "expected_action":       "keep",
    }


# ── Score deciding factors (same logic as eval_runner.py) ─────────
def score_factors(llm_instance: dict, expected_factors: dict) -> list[dict]:
    checks  = []
    factors = llm_instance.get("deciding_factors", [])

    if not factors:
        checks.append({
            "check": "deciding_factors_present",
            "passed": False,
            "detail": "LLM returned no deciding_factors"
        })
        return checks

    checks.append({
        "check": "deciding_factors_present",
        "passed": True,
        "detail": f"{len(factors)} factor(s) returned"
    })

    primary_factors   = [f for f in factors if f.get("impact", "").lower() == "primary"]
    primary_names     = [f.get("metric", "").lower() for f in primary_factors]
    exp_primary       = expected_factors.get("primary_metric", "").lower()
    exp_direction     = expected_factors.get("primary_direction", "").lower()
    must_not_primary  = [m.lower() for m in expected_factors.get("must_not_cite_as_primary", [])]

    # 1. Correct primary metric
    hit = any(exp_primary in n or n in exp_primary for n in primary_names)
    checks.append({
        "check": f"primary_factor:{exp_primary}",
        "passed": hit,
        "detail": f"LLM cited {primary_names} as primary — expected '{exp_primary}'"
                  if not hit else f"correctly identified '{exp_primary}' as primary driver"
    })

    # 2. Direction correct
    if hit and exp_direction:
        pf = next(
            (f for f in primary_factors
             if exp_primary in f.get("metric","").lower()
             or f.get("metric","").lower() in exp_primary),
            None
        )
        if pf:
            actual_dir = pf.get("direction", "").lower()
            dir_ok     = exp_direction in actual_dir or actual_dir in exp_direction
            checks.append({
                "check": f"direction:{exp_primary}",
                "passed": dir_ok,
                "detail": f"direction='{actual_dir}' expected='{exp_direction}'"
            })

    # 3. Must-not-be-primary checks
    for bad in must_not_primary:
        wrongly = any(bad in n or n in bad for n in primary_names)
        if wrongly:
            checks.append({
                "check": f"wrong_primary:{bad}",
                "passed": False,
                "detail": f"'{bad}' incorrectly cited as primary driver"
            })

    return checks


# ── Metric health assessment ───────────────────────────────────────
def assess_metric_health(metrics: dict) -> list[dict]:
    """Return a list of metric health assessments with status and thresholds."""
    assessments = []
    checks = [
        ("cpu_avg_pct",           metrics.get("cpu_avg_pct"),           "low",  5,   "CPU avg < 5% → underutilised"),
        ("cpu_p95_pct",           metrics.get("cpu_p95_pct"),           "high", 80,  "CPU P95 > 80% → performance risk"),
        ("mem_p95_pct",           metrics.get("mem_p95_pct"),           "high", 85,  "Mem P95 > 85% → memory pressure"),
        ("ebs_io_balance_pct",    metrics.get("ebs_io_balance_pct"),    "low",  20,  "EBS IO balance < 20% → throttling risk"),
        ("status_check_failures", metrics.get("status_check_failures"), "high", 0,   "Any failures → reliability concern"),
        ("sample_days",           metrics.get("sample_days"),           "low",  7,   "< 7 days → insufficient data"),
    ]
    for name, value, direction, threshold, note in checks:
        if value is None:
            status = "no_data"
        elif direction == "low" and value < threshold:
            status = "alert"
        elif direction == "high" and value > threshold:
            status = "alert"
        else:
            status = "ok"
        assessments.append({
            "metric":    name,
            "value":     value,
            "threshold": threshold,
            "direction": direction,
            "status":    status,
            "note":      note,
        })
    return assessments


# ── API helpers ───────────────────────────────────────────────────
async def fetch_instances(client: httpx.AsyncClient) -> list[dict]:
    resp = await client.get(f"{API_BASE}/instances", timeout=10)
    resp.raise_for_status()
    return resp.json().get("instances", [])


async def fetch_and_analyse(
    client: httpx.AsyncClient,
    instance: dict,
    window_days: int,
) -> dict:
    """Fetch metrics for one instance via /analyse-eval and return scored result."""
    payload = {
        "scenario_id": instance["instance_id"],
        "metrics":     [],          # will be filled by the endpoint from DB — see note below
        "window_days": window_days,
    }

    # /analyse-eval accepts pre-built metrics OR we can use the dedicated
    # /analyse-eval-live endpoint. Since we don't have that, we call
    # /instances-metrics (a new lightweight endpoint we add below).
    # For now: fetch metrics directly from /preview-prompt equivalent
    # using the existing fetch logic via a metrics endpoint.
    metrics_resp = await client.get(
        f"{API_BASE}/instance-metrics",
        params={"instance_id": instance["instance_id"], "window_days": window_days},
        timeout=30,
    )
    metrics_resp.raise_for_status()
    metrics_rows = metrics_resp.json().get("metrics", [])

    if not metrics_rows:
        return {
            "instance":    instance,
            "metrics":     [],
            "llm":         None,
            "factors":     [],
            "health":      [],
            "expected_df": {},
            "error":       f"No metrics found for {instance['instance_id']} in {window_days}d window",
        }

    # Call /analyse-eval with the real metrics
    eval_resp = await client.post(
        f"{API_BASE}/analyse-eval",
        json={"scenario_id": instance["instance_id"], "metrics": metrics_rows, "window_days": window_days},
        timeout=120,
    )
    eval_resp.raise_for_status()
    llm_response = eval_resp.json().get("llm_response", {})
    llm_instance = next(
        (i for i in llm_response.get("instances", [])
         if i.get("instance_id") == instance["instance_id"]),
        None
    )

    metrics      = metrics_rows[0] if metrics_rows else {}
    expected_df  = derive_expected_factors(metrics)
    factor_checks = score_factors(llm_instance, expected_df) if llm_instance else []
    health        = assess_metric_health(metrics)

    return {
        "instance":    instance,
        "metrics":     metrics,
        "llm":         llm_instance,
        "factors":     factor_checks,
        "health":      health,
        "expected_df": expected_df,
        "error":       None,
    }


# ── Terminal report ───────────────────────────────────────────────
def print_instance_report(result: dict, idx: int, total: int):
    inst    = result["instance"]
    metrics = result["metrics"]
    llm     = result["llm"]
    iid     = inst["instance_id"]
    name    = inst.get("instance_name") or "unnamed"

    console.print(Rule(
        f"[bold cyan][{idx}/{total}] {iid}[/bold cyan]  [dim]{name}  {inst.get('instance_type','')}  {inst.get('az','')}[/dim]"
    ))

    if result.get("error"):
        console.print(f"  [red]Error: {result['error']}[/red]")
        return

    # ── Metric health table ──
    ht = Table(box=box.SIMPLE, show_header=True, header_style="bold dim",
               title="[dim]Metric Health[/dim]", title_style="")
    ht.add_column("Metric",    min_width=26)
    ht.add_column("Observed",  justify="right", min_width=10)
    ht.add_column("Threshold", justify="right", min_width=10)
    ht.add_column("Status",    min_width=8)
    ht.add_column("Note",      style="dim")
    for a in result["health"]:
        if a["status"] == "alert":
            status_txt = "[bold red]ALERT[/bold red]"
            val_style  = "bold red"
        else:
            status_txt = "[green]OK[/green]"
            val_style  = "green"
        val = a["value"]
        ht.add_row(
            a["metric"],
            f"[{val_style}]{val}[/{val_style}]" if val is not None else "[dim]—[/dim]",
            str(a["threshold"]),
            status_txt,
            a["note"],
        )
    console.print(ht)

    # ── LLM recommendation ──
    if llm:
        action  = llm.get("rightsizing_action", "—")
        current = llm.get("current_type", inst.get("instance_type", "—"))
        rec     = llm.get("recommended_type", "—")
        reason  = llm.get("rightsizing_reason", "—")
        saving  = llm.get("estimated_monthly_saving_usd", 0) or 0
        conf    = llm.get("confidence", "—")

        action_color = {
            "downsize": "green", "upsize": "yellow", "change_family": "cyan",
            "keep": "dim", "terminate": "red", "insufficient_data": "dim"
        }.get(action, "white")

        saving_str = (
            f"[green]+${saving:.2f}/mo saved[/green]" if saving > 0
            else f"[yellow]-${abs(saving):.2f}/mo cost increase[/yellow]" if saving < 0
            else "[dim]no change[/dim]"
        )

        console.print(f"  Recommendation:  [{action_color}]{action.upper()}[/{action_color}]  "
                      f"[dim]{current}[/dim] → [bold]{rec}[/bold]  "
                      f"{saving_str}  [dim]confidence: {conf}[/dim]")
        # Use Text() for LLM-generated reason to prevent markup parsing
        reason_text = Text("  Reason: ", style="default")
        reason_text.append(reason, style="dim")
        console.print(reason_text)

        # ── Deciding factors table ──
        factors = llm.get("deciding_factors", [])
        if factors:
            ft = Table(box=box.SIMPLE, show_header=True, header_style="bold yellow",
                       title="[yellow]Deciding Factors (LLM reasoning)[/yellow]")
            ft.add_column("Metric",      min_width=24)
            ft.add_column("Observed",    justify="right", min_width=9)
            ft.add_column("Threshold",   justify="right", min_width=9)
            ft.add_column("Direction",   min_width=18)
            ft.add_column("Impact",      min_width=11)
            ft.add_column("Explanation", style="dim")
            for f in sorted(factors, key=lambda x: {"primary":0,"secondary":1,"irrelevant":2}.get(x.get("impact",""),3)):
                impact = f.get("impact", "")
                imp_style = "bold yellow" if impact == "primary" else ("" if impact == "secondary" else "dim")
                # Use Text.from_markup=False to prevent Rich parsing LLM content as markup
                explanation = Text(f.get("explanation", ""), style="dim", no_wrap=False)
                ft.add_row(
                    f.get("metric", ""),
                    str(f.get("observed_value", "—")),
                    str(f.get("threshold", "—")),
                    f.get("direction", ""),
                    Text(impact, style=imp_style),
                    explanation,
                )
            console.print(ft)

        # ── Factor accuracy checks ──
        exp_df = result.get("expected_df", {})
        checks = result.get("factors", [])
        if checks:
            ct = Table(box=box.SIMPLE, show_header=True, header_style="bold dim",
                       title="[dim]Factor Accuracy Checks[/dim]")
            ct.add_column("Check",  min_width=38)
            ct.add_column("Result", min_width=8)
            ct.add_column("Detail")
            for c in checks:
                icon = "[green]PASS[/green]" if c["passed"] else "[red]FAIL[/red]"
                ct.add_row(c["check"], icon, Text(c["detail"], style="dim", no_wrap=False))
            console.print(ct)

            passed = sum(1 for c in checks if c["passed"])
            total_ = len(checks)
            pct    = round(passed / total_ * 100, 1) if total_ else 0
            color  = "green" if pct == 100 else "yellow" if pct >= 70 else "red"
            console.print(
                f"  Factor accuracy: [{color}]{pct}% ({passed}/{total_})[/{color}]  "
                f"[dim]expected primary driver: {exp_df.get('primary_metric','?')} "
                f"({exp_df.get('primary_direction','?')})[/dim]"
            )

        flags = llm.get("risk_flags", [])
        if flags:
            console.print()
            for flag in flags:
                sev = flag.get("severity", "")
                sev_color = {"CRITICAL":"red","HIGH":"yellow","MEDIUM":"cyan","LOW":"dim"}.get(sev,"white")
                flag_line = Text(f"  ")
                flag_line.append(sev, style=f"bold {sev_color}")
                flag_line.append(f"  {flag.get('flag','')}  ", style="bold")
                flag_line.append(flag.get("detail",""), style="dim")
                console.print(flag_line)


# ── HTML report generator ─────────────────────────────────────────
def generate_html_report(results: list[dict], window_days: int) -> str:
    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    rows_html = ""

    for r in results:
        inst    = r["instance"]
        metrics = r["metrics"]
        llm     = r.get("llm") or {}
        iid     = inst["instance_id"]
        name    = inst.get("instance_name") or "unnamed"
        itype   = inst.get("instance_type", "—")
        az      = inst.get("az", "—")

        action  = llm.get("rightsizing_action", "—")
        rec     = llm.get("recommended_type", "—")
        reason  = llm.get("rightsizing_reason", "—")
        saving  = llm.get("estimated_monthly_saving_usd") or 0
        conf    = llm.get("confidence", "—")

        action_cls = {
            "downsize":"tag-green","upsize":"tag-yellow","change_family":"tag-blue",
            "keep":"tag-gray","terminate":"tag-red","insufficient_data":"tag-gray"
        }.get(action, "tag-gray")

        saving_html = (
            f'<span class="saving-pos">+${saving:.2f}/mo saved</span>' if saving > 0
            else f'<span class="saving-neg">-${abs(saving):.2f}/mo</span>' if saving < 0
            else '<span class="saving-zero">No change</span>'
        )

        # Health pills
        health_html = ""
        for a in r.get("health", []):
            val     = a["value"]
            cls     = "pill-alert" if a["status"] == "alert" else "pill-ok"
            label   = a["metric"].replace("_pct","").replace("_"," ")
            note    = a["note"]
            health_html += f'<span class="pill {cls}" title="{note}">{label}: {val}</span>'

        # Deciding factors rows
        factors_html = ""
        for f in sorted(llm.get("deciding_factors", []),
                        key=lambda x: {"primary":0,"secondary":1,"irrelevant":2}.get(x.get("impact",""),3)):
            impact   = f.get("impact", "")
            imp_cls  = {"primary":"impact-primary","secondary":"impact-secondary","irrelevant":"impact-irrelevant"}.get(impact, "")
            factors_html += f"""
            <tr>
              <td class="mono">{f.get('metric','')}</td>
              <td class="mono right">{f.get('observed_value','—')}</td>
              <td class="mono right">{f.get('threshold','—')}</td>
              <td>{f.get('direction','')}</td>
              <td><span class="impact-badge {imp_cls}">{impact}</span></td>
              <td class="dim">{f.get('explanation','')}</td>
            </tr>"""

        # Factor accuracy checks
        checks       = r.get("factors", [])
        passed       = sum(1 for c in checks if c["passed"])
        total_checks = len(checks)
        pct          = round(passed / total_checks * 100, 1) if total_checks else 0
        acc_cls      = "acc-pass" if pct == 100 else "acc-partial" if pct >= 70 else "acc-fail"
        exp_primary  = r.get("expected_df", {}).get("primary_metric", "—")

        checks_html = ""
        for c in checks:
            icon = "✓" if c["passed"] else "✗"
            cls_ = "check-pass" if c["passed"] else "check-fail"
            checks_html += f'<div class="check-row {cls_}"><span class="check-icon">{icon}</span><span class="check-name">{c["check"]}</span><span class="check-detail">{c["detail"]}</span></div>'

        # Risk flags
        flags_html = ""
        for flag in llm.get("risk_flags", []):
            sev     = flag.get("severity", "")
            sev_cls = {"CRITICAL":"sev-critical","HIGH":"sev-high","MEDIUM":"sev-medium","LOW":"sev-low"}.get(sev,"")
            flags_html += f'<div class="flag-row"><span class="sev-badge {sev_cls}">{sev}</span><span class="flag-name">{flag.get("flag","")}</span><span class="flag-detail">{flag.get("detail","")}</span></div>'

        error_html = f'<div class="error-box">{r["error"]}</div>' if r.get("error") else ""

        rows_html += f"""
      <div class="instance-card" id="{iid}">
        <div class="card-header">
          <div class="card-title">
            <span class="iid mono">{iid}</span>
            <span class="iname">{name}</span>
            <span class="pill pill-gray mono">{itype}</span>
            <span class="pill pill-gray mono">{az}</span>
          </div>
          <div class="card-meta">
            <span class="tag {action_cls}">{action.upper()}</span>
            <span class="rec mono">{itype} → <strong>{rec}</strong></span>
            {saving_html}
            <span class="conf dim">confidence: {conf}</span>
          </div>
        </div>

        {error_html}

        <div class="reason dim">"{reason}"</div>

        <div class="health-pills">{health_html}</div>

        {"<h4>Deciding Factors</h4><table class='factors-table'><thead><tr><th>Metric</th><th>Observed</th><th>Threshold</th><th>Direction</th><th>Impact</th><th>Explanation</th></tr></thead><tbody>" + factors_html + "</tbody></table>" if factors_html else ""}

        <div class="factor-accuracy {acc_cls}">
          Factor accuracy: <strong>{pct}%</strong> ({passed}/{total_checks})
          &nbsp;·&nbsp; Expected primary driver: <span class="mono">{exp_primary}</span>
        </div>

        {checks_html}

        {"<h4>Risk Flags</h4>" + flags_html if flags_html else ""}
      </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>EC2 Live Analysis Report — {run_at}</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    :root {{
      --bg: #f8f7f4; --canvas: #fff; --border: #e5e3de; --border2: #d1cec7;
      --text: #1c1917; --text2: #57534e; --muted: #a8a29e;
      --accent: #2563eb; --green: #16a34a; --amber: #d97706; --red: #dc2626; --blue: #0891b2;
      --mono: 'JetBrains Mono', monospace; --sans: 'DM Sans', sans-serif;
    }}
    body {{ background: var(--bg); color: var(--text); font-family: var(--sans); font-size: 14px; line-height: 1.6; }}
    .topbar {{ background: var(--canvas); border-bottom: 1px solid var(--border); padding: 14px 32px; display: flex; align-items: center; gap: 16px; position: sticky; top: 0; z-index: 10; box-shadow: 0 1px 4px rgba(0,0,0,0.05); }}
    .topbar-logo {{ font-size: 15px; font-weight: 700; letter-spacing: -0.02em; }}
    .topbar-logo span {{ color: var(--accent); }}
    .topbar-meta {{ font-size: 12px; color: var(--muted); font-family: var(--mono); margin-left: auto; }}
    .summary-bar {{ background: var(--canvas); border-bottom: 1px solid var(--border); padding: 10px 32px; display: flex; gap: 24px; font-size: 12px; }}
    .sbar-item {{ display: flex; flex-direction: column; }}
    .sbar-label {{ color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; font-size: 10px; }}
    .sbar-value {{ font-family: var(--mono); font-weight: 600; font-size: 14px; color: var(--text); }}
    .sbar-value.green {{ color: var(--green); }} .sbar-value.red {{ color: var(--red); }} .sbar-value.amber {{ color: var(--amber); }}
    .content {{ max-width: 1100px; margin: 0 auto; padding: 24px 32px; }}
    .instance-card {{ background: var(--canvas); border: 1px solid var(--border); border-radius: 10px; padding: 20px 24px; margin-bottom: 20px; }}
    .card-header {{ display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 12px; gap: 16px; flex-wrap: wrap; }}
    .card-title {{ display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }}
    .iid {{ font-family: var(--mono); font-size: 13px; font-weight: 600; color: var(--blue); }}
    .iname {{ font-size: 14px; font-weight: 600; }}
    .card-meta {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }}
    .rec {{ font-family: var(--mono); font-size: 12px; color: var(--text2); }}
    .conf {{ font-size: 11px; }}
    .reason {{ font-size: 13px; margin-bottom: 12px; padding: 8px 12px; background: var(--bg); border-radius: 6px; border-left: 3px solid var(--border2); }}
    .health-pills {{ display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 14px; }}
    .mono {{ font-family: var(--mono); }} .dim {{ color: var(--muted); }}
    .pill {{ font-size: 11px; padding: 3px 9px; border-radius: 20px; font-family: var(--mono); border: 1px solid var(--border); background: var(--bg); }}
    .pill-ok {{ color: var(--green); border-color: #bbf7d0; background: #f0fdf4; }}
    .pill-alert {{ color: var(--red); border-color: #fecaca; background: #fff1f2; font-weight: 600; }}
    .pill-gray {{ color: var(--text2); }}
    .tag {{ font-size: 11px; font-weight: 700; padding: 3px 10px; border-radius: 4px; letter-spacing: 0.04em; }}
    .tag-green {{ background: #f0fdf4; color: var(--green); border: 1px solid #bbf7d0; }}
    .tag-yellow {{ background: #fffbeb; color: var(--amber); border: 1px solid #fde68a; }}
    .tag-blue {{ background: #e0f2fe; color: var(--blue); border: 1px solid #bae6fd; }}
    .tag-red {{ background: #fff1f2; color: var(--red); border: 1px solid #fecaca; }}
    .tag-gray {{ background: var(--bg); color: var(--muted); border: 1px solid var(--border); }}
    .saving-pos {{ color: var(--green); font-weight: 600; font-size: 13px; }}
    .saving-neg {{ color: var(--amber); font-weight: 600; font-size: 13px; }}
    .saving-zero {{ color: var(--muted); font-size: 13px; }}
    h4 {{ font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); margin: 14px 0 8px; }}
    .factors-table {{ width: 100%; border-collapse: collapse; font-size: 12px; margin-bottom: 12px; }}
    .factors-table th {{ background: var(--bg); padding: 7px 12px; text-align: left; font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); border-bottom: 1px solid var(--border); }}
    .factors-table td {{ padding: 7px 12px; border-bottom: 1px solid var(--border); vertical-align: top; }}
    .factors-table tr:last-child td {{ border-bottom: none; }}
    .factors-table tr:hover td {{ background: var(--bg); }}
    .right {{ text-align: right; }}
    .impact-badge {{ font-size: 10px; font-weight: 700; padding: 2px 7px; border-radius: 4px; text-transform: uppercase; }}
    .impact-primary {{ background: #fffbeb; color: var(--amber); border: 1px solid #fde68a; }}
    .impact-secondary {{ background: #f0f9ff; color: var(--blue); border: 1px solid #bae6fd; }}
    .impact-irrelevant {{ background: var(--bg); color: var(--muted); border: 1px solid var(--border); }}
    .factor-accuracy {{ font-size: 12px; padding: 7px 12px; border-radius: 6px; margin: 10px 0 8px; }}
    .acc-pass {{ background: #f0fdf4; color: var(--green); border: 1px solid #bbf7d0; }}
    .acc-partial {{ background: #fffbeb; color: var(--amber); border: 1px solid #fde68a; }}
    .acc-fail {{ background: #fff1f2; color: var(--red); border: 1px solid #fecaca; }}
    .check-row {{ display: flex; align-items: flex-start; gap: 8px; font-size: 12px; padding: 3px 0; }}
    .check-icon {{ font-weight: 700; min-width: 14px; }}
    .check-pass .check-icon {{ color: var(--green); }} .check-fail .check-icon {{ color: var(--red); }}
    .check-name {{ font-family: var(--mono); color: var(--text2); min-width: 220px; }}
    .check-detail {{ color: var(--muted); }}
    .flag-row {{ display: flex; align-items: flex-start; gap: 10px; padding: 6px 0; border-bottom: 1px solid var(--border); font-size: 13px; }}
    .flag-row:last-child {{ border-bottom: none; }}
    .sev-badge {{ font-size: 10px; font-weight: 700; padding: 2px 8px; border-radius: 4px; min-width: 70px; text-align: center; }}
    .sev-critical {{ background: #fff1f2; color: var(--red); border: 1px solid #fecaca; }}
    .sev-high {{ background: #fffbeb; color: var(--amber); border: 1px solid #fde68a; }}
    .sev-medium {{ background: #e0f2fe; color: var(--blue); border: 1px solid #bae6fd; }}
    .sev-low {{ background: var(--bg); color: var(--muted); border: 1px solid var(--border); }}
    .flag-name {{ font-weight: 600; min-width: 160px; }}
    .flag-detail {{ color: var(--text2); }}
    .error-box {{ background: #fff1f2; border: 1px solid #fecaca; color: var(--red); padding: 10px 14px; border-radius: 6px; font-size: 13px; margin-bottom: 12px; }}
  </style>
</head>
<body>
  <div class="topbar">
    <div class="topbar-logo">EC2 <span>Fleet</span> Report</div>
    <div class="topbar-meta">Generated {run_at} · {window_days}d window · {len(results)} instances · Groq llama-3.3-70b</div>
  </div>
  <div class="summary-bar">
    <div class="sbar-item"><span class="sbar-label">Instances</span><span class="sbar-value">{len(results)}</span></div>
    <div class="sbar-item"><span class="sbar-label">Downsize</span><span class="sbar-value green">{sum(1 for r in results if r.get('llm') and r['llm'].get('rightsizing_action')=='downsize')}</span></div>
    <div class="sbar-item"><span class="sbar-label">Upsize</span><span class="sbar-value amber">{sum(1 for r in results if r.get('llm') and r['llm'].get('rightsizing_action')=='upsize')}</span></div>
    <div class="sbar-item"><span class="sbar-label">Change Family</span><span class="sbar-value amber">{sum(1 for r in results if r.get('llm') and r['llm'].get('rightsizing_action')=='change_family')}</span></div>
    <div class="sbar-item"><span class="sbar-label">Terminate</span><span class="sbar-value red">{sum(1 for r in results if r.get('llm') and r['llm'].get('rightsizing_action')=='terminate')}</span></div>
    <div class="sbar-item"><span class="sbar-label">Keep</span><span class="sbar-value">{sum(1 for r in results if r.get('llm') and r['llm'].get('rightsizing_action')=='keep')}</span></div>
    <div class="sbar-item"><span class="sbar-label">Est. Monthly Savings</span><span class="sbar-value green">${sum(r['llm'].get('estimated_monthly_saving_usd') or 0 for r in results if r.get('llm') and (r['llm'].get('estimated_monthly_saving_usd') or 0) > 0):.2f}</span></div>
  </div>
  <div class="content">
    {rows_html}
  </div>
</body>
</html>"""


# ── Main ──────────────────────────────────────────────────────────
async def main():
    global API_BASE
    parser = argparse.ArgumentParser(description="EC2 Live Instance Report")
    parser.add_argument("--window",   "-w", type=int, default=30,        help="Metric window in days (10/30/60/90)")
    parser.add_argument("--instance", "-i", default=None,                help="Single instance ID to analyse")
    parser.add_argument("--output",   "-o", default="live_report.html",  help="HTML output file path")
    parser.add_argument("--json",           default=None,                help="Also save raw JSON to this path")
    parser.add_argument("--api",            default=API_BASE,            help="Backend base URL")
    args   = parser.parse_args()
    API_BASE = args.api

    console.print(Panel(
        f"[bold cyan]EC2 Live Fleet Report[/bold cyan]\n"
        f"Window: [bold]{args.window}d[/bold]  ·  "
        f"Backend: [bold]{API_BASE}[/bold]  ·  "
        f"Output: [bold]{args.output}[/bold]",
        border_style="cyan"
    ))

    async with httpx.AsyncClient() as client:
        # Fetch instance list
        console.print("[dim]Fetching instances from database...[/dim]")
        try:
            instances = await fetch_instances(client)
        except Exception as e:
            console.print(f"[red]Cannot reach backend: {e}[/red]")
            console.print(f"[yellow]Make sure uvicorn is running: uv run uvicorn main:app --reload --port 8000[/yellow]")
            return

        if args.instance:
            instances = [i for i in instances if i["instance_id"] == args.instance]
            if not instances:
                console.print(f"[red]Instance '{args.instance}' not found in database.[/red]")
                return

        console.print(f"  Found [bold]{len(instances)}[/bold] instance(s)\n")

        results = []
        for idx, inst in enumerate(instances, 1):
            console.print(f"[dim]Analysing {inst['instance_id']} ({inst.get('instance_name','')})...[/dim]")
            t0 = time.monotonic()
            try:
                result = await fetch_and_analyse(client, inst, args.window)
            except httpx.ConnectError:
                console.print(f"[red]Connection refused — is the backend running?[/red]")
                break
            except Exception as e:
                result = {
                    "instance": inst, "metrics": {}, "llm": None,
                    "factors": [], "health": [], "expected_df": {},
                    "error": str(e),
                }
            result["elapsed"] = round(time.monotonic() - t0, 2)
            results.append(result)
            print_instance_report(result, idx, len(instances))

    if not results:
        return

    # Summary
    all_checks = [c for r in results for c in r.get("factors", [])]
    passed     = sum(1 for c in all_checks if c["passed"])
    total      = len(all_checks)
    overall    = round(passed / total * 100, 1) if total else 0

    console.print(Rule())
    console.print(Panel(
        f"[bold]REPORT SUMMARY[/bold]\n\n"
        f"  Instances analysed : {len(results)}\n"
        f"  Factor accuracy    : [bold cyan]{overall}%[/bold cyan] ({passed}/{total} checks)\n"
        f"  Downsizes          : {sum(1 for r in results if r.get('llm') and r['llm'].get('rightsizing_action')=='downsize')}\n"
        f"  Upsizes            : {sum(1 for r in results if r.get('llm') and r['llm'].get('rightsizing_action')=='upsize')}\n"
        f"  Change family      : {sum(1 for r in results if r.get('llm') and r['llm'].get('rightsizing_action')=='change_family')}\n"
        f"  Terminate          : {sum(1 for r in results if r.get('llm') and r['llm'].get('rightsizing_action')=='terminate')}\n"
        f"  Est. monthly saving: ${ sum(r['llm'].get('estimated_monthly_saving_usd') or 0 for r in results if r.get('llm') and (r['llm'].get('estimated_monthly_saving_usd') or 0) > 0):.2f}\n"
        f"  HTML report        : {args.output}",
        border_style="cyan", title="Results",
    ))

    # Save HTML
    html = generate_html_report(results, args.window)
    Path(args.output).write_text(html, encoding="utf-8")
    console.print(f"[green]HTML report saved → {args.output}[/green]")

    # Save JSON
    if args.json:
        Path(args.json).write_text(
            json.dumps({
                "run_at":     datetime.now(timezone.utc).isoformat(),
                "window_days": args.window,
                "results":    [
                    {k: v for k, v in r.items() if k != "llm"}
                    | {"llm": r.get("llm")}
                    for r in results
                ]
            }, indent=2, default=str),
            encoding="utf-8"
        )
        console.print(f"[green]JSON saved → {args.json}[/green]")


if __name__ == "__main__":
    asyncio.run(main())