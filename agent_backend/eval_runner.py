"""
╔══════════════════════════════════════════════════════════════════╗
║         EC2 Analysis Agent — LLM Eval Runner                     ║
║                                                                  ║
║  Runs all scenarios from eval_scenarios.json against the         ║
║  /analyse-eval endpoint and scores the LLM's responses.          ║
║                                                                  ║
║  Run:                                                            ║
║      uv run python eval_runner.py                                ║
║      uv run python eval_runner.py --scenario EVAL-001            ║
║      uv run python eval_runner.py --output report.json           ║
╚══════════════════════════════════════════════════════════════════╝
"""

import argparse
import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from rich.text import Text

API_BASE        = "http://localhost:8000"
SCENARIOS_FILE  = Path(__file__).parent / "eval_scenarios.json"
console         = Console()


# ──────────────────────────────────────────────────────────────────
# SCORER
# Each check returns (passed: bool, detail: str)
# ──────────────────────────────────────────────────────────────────

def score_deciding_factors(
    llm_instance: dict,
    expected: dict,
) -> list[dict]:
    """
    Verifies that the LLM cited the correct metrics as the deciding
    factors for its recommendation, and ignored irrelevant ones.

    This is the core accuracy test: did the model understand WHICH
    metrics actually drove the decision?
    """
    checks = []
    exp_df = expected.get("deciding_factors", {})
    if not exp_df:
        return checks

    factors = llm_instance.get("deciding_factors", [])
    if not factors:
        checks.append({
            "check": "deciding_factors_present",
            "passed": False,
            "detail": "LLM returned no deciding_factors — field missing from response"
        })
        return checks

    checks.append({
        "check": "deciding_factors_present",
        "passed": True,
        "detail": f"{len(factors)} factor(s) returned"
    })

    # Build lookup: metric_name → factor dict
    factor_map = {f.get("metric", "").lower(): f for f in factors}
    primary_factors   = [f for f in factors if f.get("impact", "").lower() == "primary"]
    secondary_factors = [f for f in factors if f.get("impact", "").lower() == "secondary"]

    exp_primary         = exp_df.get("primary_metric", "").lower()
    exp_secondary       = [m.lower() for m in exp_df.get("secondary_metrics", [])]
    exp_irrelevant      = [m.lower() for m in exp_df.get("irrelevant_metrics", [])]
    must_not_primary    = [m.lower() for m in exp_df.get("must_not_cite_as_primary", [])]
    exp_direction       = exp_df.get("primary_direction", "").lower()

    # 1. Correct primary metric identified
    primary_names = [f.get("metric", "").lower() for f in primary_factors]
    primary_hit   = any(exp_primary in n or n in exp_primary for n in primary_names)
    checks.append({
        "check": f"primary_factor_correct:{exp_primary}",
        "passed": primary_hit,
        "detail": f"LLM primary factors={primary_names} — expected '{exp_primary}' as primary"
                  if not primary_hit else
                  f"correctly identified '{exp_primary}' as primary driver"
    })

    # 2. Primary metric direction correct (above/below threshold)
    if primary_hit and exp_direction:
        primary_factor = next(
            (f for f in primary_factors
             if exp_primary in f.get("metric", "").lower()
             or f.get("metric", "").lower() in exp_primary),
            None
        )
        if primary_factor:
            actual_dir = primary_factor.get("direction", "").lower()
            dir_ok     = exp_direction in actual_dir or actual_dir in exp_direction
            checks.append({
                "check": f"primary_direction_correct:{exp_primary}",
                "passed": dir_ok,
                "detail": f"direction='{actual_dir}' expected='{exp_direction}'"
            })

    # 3. Wrong metrics must NOT be cited as primary
    for bad in must_not_primary:
        wrongly_primary = any(bad in n or n in bad for n in primary_names)
        checks.append({
            "check": f"must_not_be_primary:{bad}",
            "passed": not wrongly_primary,
            "detail": f"'{bad}' incorrectly cited as primary driver"
                      if wrongly_primary else f"'{bad}' correctly not primary"
        })

    # 4. At least one secondary metric correctly identified (if any expected)
    if exp_secondary:
        all_non_primary = [f.get("metric", "").lower() for f in factors
                           if f.get("impact", "").lower() != "primary"]
        secondary_hit = any(
            any(s in n or n in s for n in all_non_primary)
            for s in exp_secondary
        )
        checks.append({
            "check": "secondary_factors_identified",
            "passed": secondary_hit,
            "detail": f"expected one of {exp_secondary} as secondary, LLM has {all_non_primary}"
                      if not secondary_hit else
                      f"correctly identified secondary metric(s) from {exp_secondary}"
        })

    # 5. Truly irrelevant metrics not cited as primary or secondary
    irrelevant_wrongly_cited = []
    for irr in exp_irrelevant:
        f = factor_map.get(irr)
        if f and f.get("impact", "").lower() in ("primary", "secondary"):
            irrelevant_wrongly_cited.append(irr)
    checks.append({
        "check": "irrelevant_metrics_not_cited",
        "passed": len(irrelevant_wrongly_cited) == 0,
        "detail": f"irrelevant metrics wrongly cited as drivers: {irrelevant_wrongly_cited}"
                  if irrelevant_wrongly_cited else
                  f"correctly excluded irrelevant metrics {exp_irrelevant}"
    })

    return checks

def score_rightsizing(
    llm_instance: dict,
    expected: dict,
) -> list[dict]:
    """Score rightsizing recommendation for one instance."""
    checks = []
    exp_rs = expected.get("rightsizing", {})
    if not exp_rs:
        return checks

    action       = llm_instance.get("rightsizing_action", "").lower()
    recommended  = llm_instance.get("recommended_type", "").lower()
    reason       = llm_instance.get("rightsizing_reason", "").lower()
    exp_action   = exp_rs.get("action", "")
    acc_targets  = [t.lower() for t in exp_rs.get("acceptable_targets", [])]
    bad_targets  = [t.lower() for t in exp_rs.get("must_not_recommend", [])]
    reason_kwds  = [k.lower() for k in exp_rs.get("reason_keywords", [])]

    # 1. Action check — support compound expected actions like "terminate_or_downsize"
    if exp_action == "any":
        checks.append({"check": "rightsizing_action", "passed": True,
                        "detail": "any action accepted"})
    else:
        # Split "terminate_or_downsize" → ["terminate", "downsize"]
        acceptable_actions = [a.strip() for a in exp_action.replace("_or_", "|").split("|")]
        passed = any(a in action or action in a for a in acceptable_actions)
        checks.append({"check": "rightsizing_action", "passed": passed,
                        "detail": f"action='{action}' matches expected='{exp_action}'"
                                  if passed else
                                  f"action='{action}' expected='{exp_action}'"})

    # 2. Recommended type in acceptable list
    if acc_targets and "terminate" not in acc_targets:
        hit = any(t in recommended or recommended in t for t in acc_targets)
        checks.append({
            "check": "recommended_type_acceptable",
            "passed": hit,
            "detail": f"recommended='{recommended}' acceptable={acc_targets}"
        })

    # 3. Must NOT recommend bad types
    if bad_targets:
        bad_hit = any(t in recommended for t in bad_targets)
        checks.append({
            "check": "must_not_recommend",
            "passed": not bad_hit,
            "detail": f"recommended='{recommended}' wrongly matches must_not={bad_targets}"
                      if bad_hit else "OK"
        })

    # 4. Reason keywords — ADVISORY only (warns but doesn't fail the scenario)
    #    The LLM may use synonyms; we don't want to penalise correct reasoning
    if reason_kwds:
        kwd_hit = any(k in reason for k in reason_kwds)
        checks.append({
            "check": "reason_keywords (advisory)",
            "passed": kwd_hit,
            "detail": f"found: {[k for k in reason_kwds if k in reason]}"
                      if kwd_hit else
                      f"none of {reason_kwds} found — may be using synonyms"
        })

    return checks


def score_risk_warnings(
    llm_instance: dict,
    expected: dict,
) -> list[dict]:
    """Score risk warning flags for one instance."""
    checks = []
    exp_rw = expected.get("risk_warnings", {})
    if not exp_rw:
        return checks

    flags         = llm_instance.get("risk_flags", [])
    flag_names    = [f.get("flag", "").lower() for f in flags]
    flag_sevs     = {f.get("flag", "").lower(): f.get("severity", "").upper() for f in flags}
    all_detail    = " ".join(f.get("detail", "").lower() for f in flags)

    exp_flags     = [f.lower() for f in exp_rw.get("expected_flags", [])]
    no_flags      = [f.lower() for f in exp_rw.get("must_not_flag", [])]
    exp_severity  = exp_rw.get("severity", "").upper()
    must_mention  = [m.lower() for m in exp_rw.get("must_mention", [])]

    # 1. Expected flags are present
    for ef in exp_flags:
        hit = any(ef in fn or fn in ef for fn in flag_names)
        checks.append({
            "check": f"flag_present:{ef}",
            "passed": hit,
            "detail": f"flag '{ef}' {'found' if hit else 'NOT found'} in {flag_names}"
        })

    # 2. Flags that must NOT appear — only fail if severity is not LOW
    #    (LLM sometimes raises informational LOW flags; we allow that)
    for nf in no_flags:
        hit_flags = [f for f in flags if nf in f.get("flag", "").lower()]
        if hit_flags:
            # Allow if all hits are LOW severity
            all_low = all(f.get("severity", "").upper() == "LOW" for f in hit_flags)
            passed  = all_low
            detail  = (f"flag '{nf}' present but severity=LOW (allowed)"
                       if all_low else
                       f"flag '{nf}' present with severity > LOW — should not be raised")
        else:
            passed = True
            detail = f"flag '{nf}' correctly absent"
        checks.append({"check": f"flag_absent:{nf}", "passed": passed, "detail": detail})

    # 3. Severity check for primary flag
    if exp_flags and exp_severity:
        primary = exp_flags[0]
        # Find actual severity with partial match
        actual_sev = None
        for fn, sev in flag_sevs.items():
            if primary in fn or fn in primary:
                actual_sev = sev
                break
        if actual_sev:
            sev_ok = actual_sev == exp_severity
            checks.append({
                "check": f"severity:{primary}",
                "passed": sev_ok,
                "detail": f"severity='{actual_sev}' expected='{exp_severity}'"
            })
        else:
            checks.append({
                "check": f"severity:{primary}",
                "passed": False,
                "detail": f"flag '{primary}' not found — cannot check severity"
            })

    # 4. Must-mention keywords anywhere in flag details
    for kw in must_mention:
        hit = kw in all_detail
        checks.append({
            "check": f"mention:{kw}",
            "passed": hit,
            "detail": f"keyword '{kw}' {'found' if hit else 'NOT found'} in flag details"
        })

    return checks


def score_scenario(scenario: dict, llm_response: dict) -> dict:
    """
    Score a full scenario. Returns a result dict with:
      - checks:    list of individual check results
      - passed:    number passing
      - total:     total checks
      - score_pct: percentage
      - grade:     PASS / PARTIAL / FAIL
    """
    expected   = scenario.get("expected", {})
    instances  = llm_response.get("instances", [])
    checks     = []

    for exp_inst_meta in scenario["metrics"]:
        iid      = exp_inst_meta["instance_id"]
        llm_inst = next((i for i in instances if i.get("instance_id") == iid), None)

        if not llm_inst:
            checks.append({
                "check": f"instance_present:{iid}",
                "passed": False,
                "detail": f"Instance {iid} missing from LLM response"
            })
            continue

        checks.append({"check": f"instance_present:{iid}", "passed": True, "detail": "found"})

        # Group checks by category for readable output
        rs_checks = score_rightsizing(llm_inst, expected)
        df_checks = score_deciding_factors(llm_inst, expected)
        rw_checks = score_risk_warnings(llm_inst, expected)

        # Tag each check with its category
        for c in rs_checks: c["category"] = "rightsizing"
        for c in df_checks: c["category"] = "deciding_factors"
        for c in rw_checks: c["category"] = "risk_warnings"

        checks.extend(rs_checks)
        checks.extend(df_checks)
        checks.extend(rw_checks)

    # Advisory checks don't count toward score
    scored_checks = [c for c in checks if "(advisory)" not in c.get("check", "")]
    passed = sum(1 for c in scored_checks if c["passed"])
    total  = len(scored_checks)
    pct    = round(passed / total * 100, 1) if total else 0

    if pct == 100:
        grade = "PASS"
    elif pct >= 70:
        grade = "PARTIAL"
    else:
        grade = "FAIL"

    return {
        "scenario_id":   scenario["id"],
        "scenario_name": scenario["name"],
        "checks":        checks,
        "passed":        passed,
        "total":         total,
        "score_pct":     pct,
        "grade":         grade,
    }


# ──────────────────────────────────────────────────────────────────
# RUNNER
# ──────────────────────────────────────────────────────────────────

async def run_scenario(
    client: httpx.AsyncClient,
    scenario: dict,
) -> dict:
    """Call /analyse-eval for one scenario and return scored result."""
    payload = {
        "scenario_id": scenario["id"],
        "metrics":     scenario["metrics"],
        "window_days": scenario["metrics"][0].get("window_days", 30),
    }

    t0 = time.monotonic()
    try:
        console.print(f"    [dim]POST {API_BASE}/analyse-eval ...[/dim]")
        resp = await client.post(
            f"{API_BASE}/analyse-eval",
            json=payload,
            timeout=120.0,      # 2 min — Groq can be slow on large payloads
        )
        resp.raise_for_status()
        data         = resp.json()
        elapsed      = round(time.monotonic() - t0, 2)
        llm_response = data.get("llm_response", {})
        scored       = score_scenario(scenario, llm_response)
        scored["elapsed_sec"]  = elapsed
        scored["llm_response"] = llm_response
        scored["api_error"]    = None
    except httpx.ConnectError:
        elapsed = round(time.monotonic() - t0, 2)
        scored  = _error_result(scenario, elapsed,
                    f"Connection refused — is uvicorn running on {API_BASE}?")
    except httpx.TimeoutException:
        elapsed = round(time.monotonic() - t0, 2)
        scored  = _error_result(scenario, elapsed,
                    "Request timed out after 120s — Groq may be overloaded, retry later.")
    except httpx.HTTPStatusError as e:
        elapsed = round(time.monotonic() - t0, 2)
        scored  = _error_result(scenario, elapsed,
                    f"HTTP {e.response.status_code}: {e.response.text[:300]}")
    except Exception as e:
        elapsed = round(time.monotonic() - t0, 2)
        scored  = _error_result(scenario, elapsed, str(e))

    return scored


def _error_result(scenario: dict, elapsed: float, error: str) -> dict:
    return {
        "scenario_id":   scenario["id"],
        "scenario_name": scenario["name"],
        "checks":        [],
        "passed":        0,
        "total":         0,
        "score_pct":     0,
        "grade":         "ERROR",
        "elapsed_sec":   elapsed,
        "llm_response":  {},
        "api_error":     error,
    }


async def run_all(
    filter_id: Optional[str] = None,
) -> list[dict]:
    scenarios = json.loads(SCENARIOS_FILE.read_text())
    if filter_id:
        scenarios = [s for s in scenarios if s["id"] == filter_id]
        if not scenarios:
            console.print(f"[red]Scenario '{filter_id}' not found.[/red]")
            return []

    console.print(Panel(
        f"[bold cyan]EC2 Analysis Agent — LLM Eval Runner[/bold cyan]\n"
        f"Running [bold]{len(scenarios)}[/bold] scenario(s) against [bold]{API_BASE}[/bold]",
        border_style="cyan"
    ))

    results = []
    async with httpx.AsyncClient() as client:
        for i, scenario in enumerate(scenarios, 1):
            console.print(f"\n[dim][{i}/{len(scenarios)}][/dim] [bold]{scenario['id']}[/bold] — {scenario['name']}")
            result = await run_scenario(client, scenario)
            results.append(result)

            # Print checks grouped by category
            categories = ["rightsizing", "deciding_factors", "risk_warnings"]
            cat_labels = {
                "rightsizing":      "Rightsizing",
                "deciding_factors": "Deciding Factors",
                "risk_warnings":    "Risk Warnings",
            }
            cat_styles = {
                "rightsizing":      "cyan",
                "deciding_factors": "yellow",
                "risk_warnings":    "magenta",
            }

            for cat in categories:
                cat_checks = [c for c in result["checks"] if c.get("category") == cat]
                if not cat_checks:
                    continue

                t = Table(
                    box=box.SIMPLE,
                    show_header=True,
                    header_style=f"bold {cat_styles[cat]}",
                    title=f"[{cat_styles[cat]}]{cat_labels[cat]}[/{cat_styles[cat]}]",
                    title_style="bold",
                )
                t.add_column("Check", min_width=38)
                t.add_column("Result", min_width=8)
                t.add_column("Detail")

                for c in cat_checks:
                    advisory = "(advisory)" in c.get("check", "")
                    if c["passed"]:
                        icon = "[green]PASS[/green]"
                    elif advisory:
                        icon = "[dim]WARN[/dim]"
                    else:
                        icon = "[red]FAIL[/red]"
                    detail = Text(c["detail"], style="dim", overflow="fold")
                    t.add_row(c["check"], icon, detail)

                console.print(t)

            # Show what factors the LLM actually returned
            llm_resp  = result.get("llm_response", {})
            instances = llm_resp.get("instances", [])
            for inst in instances:
                factors = inst.get("deciding_factors", [])
                if factors:
                    ft = Table(box=box.MINIMAL, show_header=True, header_style="bold dim",
                               title=f"[dim]LLM deciding_factors for {inst['instance_id']}[/dim]")
                    ft.add_column("Metric",    style="cyan",  min_width=22)
                    ft.add_column("Value",     style="bold",  min_width=8)
                    ft.add_column("Threshold", min_width=8)
                    ft.add_column("Direction", min_width=18)
                    ft.add_column("Impact",    min_width=10)
                    ft.add_column("Explanation")
                    for f in factors:
                        impact_style = (
                            "bold yellow" if f.get("impact") == "primary"
                            else "dim"     if f.get("impact") == "irrelevant"
                            else ""
                        )
                        impact_text = Text(str(f.get("impact", "")), style=impact_style)
                        ft.add_row(
                            f.get("metric", ""),
                            str(f.get("observed_value", "")),
                            str(f.get("threshold", "")),
                            f.get("direction", ""),
                            impact_text,
                            Text(f.get("explanation", ""), overflow="fold", style="dim"),
                        )
                    console.print(ft)

            grade_color = {"PASS": "green", "PARTIAL": "yellow", "FAIL": "red", "ERROR": "red"}.get(result["grade"], "white")
            console.print(
                f"  Score: [{grade_color}]{result['score_pct']}% ({result['passed']}/{result['total']}) "
                f"— {result['grade']}[/{grade_color}]  "
                f"[dim]{result['elapsed_sec']}s[/dim]"
            )
            if result.get("api_error"):
                console.print(f"  [red]API Error: {result['api_error']}[/red]")

    return results


# ──────────────────────────────────────────────────────────────────
# SUMMARY REPORT
# ──────────────────────────────────────────────────────────────────

def print_summary(results: list[dict]):
    if not results:
        return

    total_scenarios = len(results)
    passed    = sum(1 for r in results if r["grade"] == "PASS")
    partial   = sum(1 for r in results if r["grade"] == "PARTIAL")
    failed    = sum(1 for r in results if r["grade"] == "FAIL")
    errors    = sum(1 for r in results if r["grade"] == "ERROR")
    avg_score = round(sum(r["score_pct"] for r in results) / total_scenarios, 1)
    avg_time  = round(sum(r["elapsed_sec"] for r in results) / total_scenarios, 2)

    console.print("\n")
    console.print(Panel(
        f"[bold]EVAL SUMMARY[/bold]\n\n"
        f"  Scenarios run : {total_scenarios}\n"
        f"  PASS          : [green]{passed}[/green]\n"
        f"  PARTIAL       : [yellow]{partial}[/yellow]\n"
        f"  FAIL          : [red]{failed}[/red]\n"
        f"  ERROR         : [red]{errors}[/red]\n"
        f"  Avg score     : [bold cyan]{avg_score}%[/bold cyan]\n"
        f"  Avg latency   : {avg_time}s / scenario",
        border_style="cyan",
        title="Results",
    ))

    # Per-scenario summary table
    t = Table(box=box.ROUNDED, title="Per-Scenario Scores", title_style="bold")
    t.add_column("ID",      style="bold cyan", min_width=10)
    t.add_column("Name",    min_width=40)
    t.add_column("Score",   justify="right")
    t.add_column("Checks",  justify="right")
    t.add_column("Grade",   justify="center")
    t.add_column("Time",    justify="right", style="dim")

    for r in results:
        gc = {"PASS": "green", "PARTIAL": "yellow", "FAIL": "red", "ERROR": "red"}.get(r["grade"], "white")
        t.add_row(
            r["scenario_id"],
            r["scenario_name"][:45],
            f"{r['score_pct']}%",
            f"{r['passed']}/{r['total']}",
            f"[{gc}]{r['grade']}[/{gc}]",
            f"{r['elapsed_sec']}s",
        )

    console.print(t)

    # Highlight failures
    failures = [r for r in results if r["grade"] in ("FAIL", "PARTIAL", "ERROR")]
    if failures:
        console.print("\n[bold yellow]Failed/Partial checks:[/bold yellow]")
        for r in failures:
            bad = [c for c in r.get("checks", []) if not c["passed"]]
            for c in bad:
                console.print(f"  [red]✗[/red] [{r['scenario_id']}] {c['check']}: {c['detail']}")


# ──────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────

async def main():
    global API_BASE
    parser = argparse.ArgumentParser(description="EC2 Analysis Agent — LLM Eval Runner")
    parser.add_argument("--scenario", "-s", help="Run a single scenario by ID (e.g. EVAL-001)")
    parser.add_argument("--output",   "-o", help="Save full results JSON to this file")
    parser.add_argument("--api",            help="Backend base URL", default=API_BASE)
    args = parser.parse_args()

    API_BASE = args.api

    results = await run_all(filter_id=args.scenario)

    if results:
        print_summary(results)

    if args.output and results:
        out = {
            "run_at":    datetime.utcnow().isoformat() + "Z",
            "api_base":  API_BASE,
            "scenarios": len(results),
            "results":   results,
        }
        Path(args.output).write_text(json.dumps(out, indent=2, default=str))
        console.print(f"\n[dim]Full results saved to: {args.output}[/dim]")


if __name__ == "__main__":
    asyncio.run(main())
