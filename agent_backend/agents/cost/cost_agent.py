"""
EC2 On-Demand Pricing — MCP Server with DB + TTL Cache
═══════════════════════════════════════════════════════
Fetches real-time on-demand Linux pricing via the AWS Pricing MCP Server,
caches results for 24 hours in memory and permanently in PostgreSQL,
and provides fast single/batch lookups with a hardcoded fallback.

Key Responsibilities:
1. MCP Integration: Spawning and communicating with the AWS Pricing MCP server.
2. Robust Parsing: Deep-searching nested JSON responses for hourly rates.
3. Concurrency Control: Preventing MCP server crashes with a global semaphore.
4. Tiered Caching: Memory (fast) -> Database (persistent) -> Fallback (safety).
"""

from __future__ import annotations

import os
import json
import time
import csv
import logging
import asyncio
from pathlib import Path
from typing import Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from agent_backend.data.database import Database

from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

# Local storage for pricing
PRICING_CSV = Path(__file__).parent.parent.parent / "data" / "pricing_cache.csv"

# ──────────────────────────────────────────────────────────────────
# MODULE-LEVEL CACHE  (instance_type → hourly_usd)
# ──────────────────────────────────────────────────────────────────
# Pre-populated with hardcoded fallback prices so lookups are instant
# even when the AWS Pricing MCP server is unavailable or slow.
_FALLBACK_PRICES_USD: dict[str, float] = {
    # "t2.micro": 0.0116,
    # "t2.small": 0.023,
    # "t2.medium": 0.0464,
    # "t2.large": 0.0928,
    # "t3.nano": 0.0052,
    # "t3.micro": 0.0104,
    # "t3.small": 0.0208,
    # "t3.medium": 0.0416,
    # "t3.large": 0.0832,
    # "t3.xlarge": 0.1664,
    # "t3.2xlarge": 0.3328,
    # "t4g.nano": 0.0042,
    # "t4g.micro": 0.0084,
    # "t4g.small": 0.0168,
    # "t4g.medium": 0.0336,
    # "t4g.large": 0.0672,
    # "t4g.xlarge": 0.1344,
    # "t4g.2xlarge": 0.2688,
    # "m5.large": 0.096,
    # "m5.xlarge": 0.192,
    # "m5.2xlarge": 0.384,
    # "m5.4xlarge": 0.768,
    # "m5.8xlarge": 1.536,
    # "m7g.medium": 0.0408,
    # "m7g.large": 0.0816,
    # "m7g.xlarge": 0.1632,
    # "m7g.2xlarge": 0.3264,
    # "m7g.4xlarge": 0.6528,
    # "m7g.8xlarge": 1.3056,
    # "m7i.large": 0.1008,
    # "m7i.xlarge": 0.2016,
    # "m7i.2xlarge": 0.4032,
    # "m7i.4xlarge": 0.8064,
    # "m7i.8xlarge": 1.6128,
    # "c5.large": 0.085,
    # "c5.xlarge": 0.170,
    # "c5.2xlarge": 0.340,
    # "c5.4xlarge": 0.680,
    # "c7g.medium": 0.0346,
    # "c7g.large": 0.0692,
    # "c7g.xlarge": 0.1384,
    # "c7g.2xlarge": 0.2768,
    # "c7g.4xlarge": 0.5536,
    # "c7i.large": 0.085,
    # "c7i.xlarge": 0.170,
    # "c7i.2xlarge": 0.340,
    # "c7i.4xlarge": 0.680,
    # "r5.large": 0.126,
    # "r5.xlarge": 0.252,
    # "r5.2xlarge": 0.504,
    # "r5.4xlarge": 1.008,
    # "r7g.large": 0.1064,
    # "r7g.xlarge": 0.2128,
    # "r7g.2xlarge": 0.4256,
    # "r7g.4xlarge": 0.8512,
    # "r7i.large": 0.133,
    # "r7i.xlarge": 0.266,
    # "r7i.2xlarge": 0.532,
    # "r7i.4xlarge": 1.064,
}
# Fallback values are used as a last resort if both DB and MCP fail.

# The active memory cache. Seeded with fallbacks but refreshed from DB/MCP.
_price_cache: dict[str, float] = dict(_FALLBACK_PRICES_USD)
_cache_ts: float = 0.0
_CACHE_TTL_SECONDS = 86_400  # Refresh memory cache every 24 hours

DEFAULT_REGION = "us-east-1"
VALID_REGIONS = {
    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
    "ap-south-1", "ap-southeast-1", "eu-west-1", "eu-central-1",
}

# These instances are always fetched during a 'sync' operation.
# They provide the pool of 'Right-sizing targets' for the LLM to choose from.
_RIGHTSIZING_TARGETS = [
    # Burstable
    "t3.nano", "t3.micro", "t3.small", "t3.medium", "t3.large", "t3.xlarge", "t3.2xlarge",
    "t4g.nano", "t4g.micro", "t4g.small", "t4g.medium", "t4g.large", "t4g.xlarge",
    # General purpose
    "m5.large", "m5.xlarge", "m5.2xlarge", "m5.4xlarge",
    "m7i.large", "m7i.xlarge", "m7i.2xlarge", "m7i.4xlarge", "m7i.8xlarge",
    "m7g.medium", "m7g.large", "m7g.xlarge", "m7g.2xlarge", "m7g.4xlarge", "m7g.8xlarge",
    # Compute optimised
    "c5.large", "c5.xlarge", "c5.2xlarge", "c5.4xlarge",
    "c7i.large", "c7i.xlarge", "c7i.2xlarge", "c7i.4xlarge",
    "c7g.medium", "c7g.large", "c7g.xlarge", "c7g.2xlarge", "c7g.4xlarge",
    # Memory optimised
    "r5.large", "r5.xlarge", "r5.2xlarge", "r5.4xlarge",
    "r7i.large", "r7i.xlarge", "r7i.2xlarge", "r7i.4xlarge",
    "r7g.large", "r7g.xlarge", "r7g.2xlarge", "r7g.4xlarge",
    # Other common
    "t2.micro", "t2.small", "t2.medium", "t2.large",
]

_logged_tool_list: bool = False
_logged_tool_schema: bool = False
_logged_price_samples: int = 0


def normalize_region(raw_region: Optional[str]) -> str:
    """
    Standardizes AWS region strings.
    Example: Converts 'us-east-1a' (Availability Zone) to 'us-east-1' (Region).
    """
    region = (raw_region or "").strip().lower()
    if not region:
        return DEFAULT_REGION
    if region in VALID_REGIONS:
        return region
    if len(region) > 2 and region[:-1] in VALID_REGIONS:
        return region[:-1]
    return DEFAULT_REGION


def _extract_price_from_tool_content(payload: Any) -> Optional[float]:
    """
    Robust recursive parser for finding numerical prices in varying MCP responses.
    
    It searches for common keys like 'hourly_usd' or 'pricePerUnit' and handles
    nested dictionaries/lists found in the AWS Pricing API output.
    """
    if payload is None:
        return None
    
    # Handle MCP TextContent objects
    text_attr = getattr(payload, "text", None)
    if isinstance(text_attr, str) and text_attr.strip():
        payload = text_attr
        
    if isinstance(payload, (int, float)):
        return float(payload)
        
    if isinstance(payload, dict):
        # 1. Direct key search
        for key in ("hourly_usd", "price_per_hour_usd", "usd_per_hour", "price"):
            if key in payload:
                try:
                    return float(payload[key])
                except (TypeError, ValueError):
                    pass
        # 2. AWS Pricing structure search (pricePerUnit -> USD)
        if "pricePerUnit" in payload and isinstance(payload["pricePerUnit"], dict):
            usd_val = payload["pricePerUnit"].get("USD")
            try:
                return float(usd_val)
            except (TypeError, ValueError):
                pass
        # 3. Recursive search in values
        for value in payload.values():
            parsed = _extract_price_from_tool_content(value)
            if parsed is not None:
                return parsed
        return None
        
    if isinstance(payload, list):
        for item in payload:
            parsed = _extract_price_from_tool_content(item)
            if parsed is not None:
                return parsed
        return None
        
    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            return None
        # ONLY attempt JSON parsing if it looks like a JSON object/array.
        # This prevents accidental parsing of standalone numbers like "2" (vCPUs) 
        # being misinterpreted as a hourly rate.
        if text.startswith("{") or text.startswith("["):
            try:
                parsed_json = json.loads(text)
                return _extract_price_from_tool_content(parsed_json)
            except Exception:
                pass
        return None

def _format_exception_group(exc: BaseException) -> str:
    """Helper to flatten asyncio ExceptionGroups into a readable string."""
    if hasattr(exc, "exceptions"):  # ExceptionGroup
        parts = []
        for i, sub in enumerate(exc.exceptions):
            parts.append(f"[{i}] {type(sub).__name__}: {sub}")
        return " | ".join(parts)
    return f"{type(exc).__name__}: {exc}"


async def _try_price_from_mcp(instance_type: str, region: str) -> Optional[float]:
    """
    Connects to the AWS Pricing MCP server and calls a pricing tool.
    
    It auto-detects which tool is available (e.g., 'get_pricing' or 'get_ec2_instance_price')
    and provides the necessary filters for On-Demand Linux Shared Tenancy.
    """
    try:
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client
    except ImportError:
        log.warning("mcp package is not installed.")
        return None

    # Configurable command for the MCP server
    mcp_command = os.getenv("AWS_MCP_COMMAND", "awslabs.aws-pricing-mcp-server")
    mcp_args = os.getenv("AWS_PRICING_MCP_ARGS", "").strip()
    args = mcp_args.split() if mcp_args else []

    # Inject region and credentials into the server environment
    env = os.environ.copy()
    env["AWS_REGION"] = region
    env["AWS_DEFAULT_REGION"] = region

    for key in (
        "AWS_PROFILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ):
        val = os.getenv(key)
        if val:
            env[key] = val

    server = StdioServerParameters(command=mcp_command, args=args, env=env)

    try:
        async with stdio_client(server) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                tool_names = [t.name for t in tools.tools]

                global _logged_tool_list
                if not _logged_tool_list:
                    log.info(f"MCP tools available: {tool_names}")
                    _logged_tool_list = True

                # Identify which pricing tool to use
                preferred = [
                    "get_ec2_instance_price",
                    "get_ec2_pricing",
                    "get_pricing",
                    "search_products",
                ]
                chosen = next((n for n in preferred if n in tool_names), None)
                if not chosen:
                    log.warning(
                        f"MCP pricing tool not found for {instance_type}."
                    )
                    return None
                global _logged_tool_schema
                if not _logged_tool_schema:
                    tool_def = next(
                        (t for t in tools.tools if t.name == chosen), None
                    )
                    if tool_def:
                        schema = getattr(tool_def, "inputSchema", None)
                        log.info(f"MCP tool schema for {chosen}: {schema}")
                    _logged_tool_schema = True

                # Construct inputs based on tool requirements
                if chosen == "get_pricing":
                    inputs = {
                        "service_code": "AmazonEC2",
                        "region": region,
                        "filters": [
                            {"Field": "instanceType", "Value": instance_type},
                            {"Field": "operatingSystem", "Value": "Linux"},
                            {"Field": "tenancy", "Value": "Shared"},
                            {"Field": "preInstalledSw", "Value": "NA"},
                            {"Field": "capacitystatus", "Value": "Used"},
                        ],
                        "output_options": {"pricing_terms": ["OnDemand"]},
                        "max_results": 1,
                    }
                else:
                    inputs = {
                        "instance_type": instance_type,
                        "region": region,
                        "operating_system": "Linux",
                        "tenancy": "Shared",
                        "term_type": "OnDemand",
                    }

                result = await session.call_tool(chosen, inputs)
                content = getattr(result, "content", None)
                price = _extract_price_from_tool_content(content)
                if price is None:
                    global _logged_price_samples
                    log.warning(
                        f"MCP returned no price for {instance_type}. "
                        f"Tool={chosen} ContentType={type(content).__name__}"
                    )
                    if _logged_price_samples < 3:
                        sample = repr(content)
                        if len(sample) > 600:
                            sample = sample[:600] + "…"
                        log.warning(
                            f"MCP content sample for {instance_type}: {sample}"
                        )
                        _logged_price_samples += 1
                return price
    except Exception as e:
        log.warning(f"MCP client error for {instance_type}: {_format_exception_group(e)}")
        return None


# ──────────────────────────────────────────────────────────────────
# CONCURRENCY CONTROL
# ──────────────────────────────────────────────────────────────────
# We limit to EXACTLY 1 concurrent MCP subprocess globally.
# This prevents 'BrokenResourceError' caused by multiple instances of the
# AWS Pricing MCP server (which uses stdio) competing or crashing under load.
_mcp_sem = asyncio.Semaphore(1)

async def _refresh_cache_async(
    instance_types: list[str], region: str = "us-east-1"
) -> None:
    """
    Batches lookups for multiple instance types.
    
    Always includes _RIGHTSIZING_TARGETS in the fetch to ensure the LLM
    has up-to-date pricing for potential recommendation tcompare_instance_costsargets.
    """
    global _price_cache, _cache_ts

    all_types = list(set(instance_types + _RIGHTSIZING_TARGETS))
    to_fetch = [t for t in all_types if t not in _price_cache]

    if not to_fetch:
        _cache_ts = time.time()
        return

    fetched = 0
    # errors = 0 # Removed as per new code, not explicitly tracked in log message

    async def fetch_one(itype: str) -> tuple[str, Optional[float]]:
        async with _mcp_sem:  # Enforce global serial execution of MCP calls
            try:
                # Timeout prevents a single hung MCP process from blocking the entire pipeline
                price = await asyncio.wait_for(
                    _try_price_from_mcp(itype, region), timeout=10.0
                )
                return itype, price
            except Exception:
                return itype, None

    # Run all lookups in parallel (wait on the semaphore for actual execution)
    results = await asyncio.gather(*(fetch_one(t) for t in to_fetch))

    for itype, price in results:
        if price is not None:
            _price_cache[itype] = price
            fetched += 1
            # log.debug(f"Fetched {itype} from MCP Server: {price}/hr") # Removed as per new code
        # else: # Removed as per new code
            # errors += 1 # Removed as per new code

    _cache_ts = time.time()
    log.info(f"Pricing MCP fetched: {fetched} successful. Total cache size: {len(_price_cache)}")


# ──────────────────────────────────────────────────────────────────
# DATABASE INTEGRATION
# ──────────────────────────────────────────────────────────────────
_db: Optional[Database] = None


def _save_to_csv():
    """Saves the current memory cache to the local CSV file."""
    try:
        PRICING_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(PRICING_CSV, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["instance_type", "region", "hourly_usd"])
            for itype, price in _price_cache.items():
                writer.writerow([itype, "us-east-1", price])
        log.info(f"Saved {len(_price_cache)} prices to {PRICING_CSV}")
    except Exception as e:
        log.error(f"Failed to save prices to CSV: {e}")


def _load_from_csv() -> None:
    """Warms the memory cache by loading prices from the local CSV file."""
    if not PRICING_CSV.exists():
        log.info("No pricing CSV found — starting fresh.")
        return
    try:
        count = 0
        with open(PRICING_CSV, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    _price_cache[row["instance_type"]] = float(row["hourly_usd"])
                    count += 1
                except (ValueError, KeyError):
                    continue
        log.info(f"Loaded {count} prices from local CSV cache.")
    except Exception as e:
        log.warning(f"Failed to load prices from CSV: {e}")


async def sync_prices(instance_types: list[str], region: str = "us-east-1") -> None:
    """
    Hard-syncs memory cache, CSV, and MCP API.
    Typically called during scheduled ETL runs or manual refreshes.
    """
    log.info(f"Starting pricing sync for {len(instance_types)} types...")
    # 1. Fetch from MCP API
    await _refresh_cache_async(instance_types, region)

    # 2. Save to CSV
    _save_to_csv()


async def get_pricing_table_async(
    instance_types: list[str], region: str = "us-east-1"
) -> dict[str, dict]:
    """
    Asynchronously retrieves a pricing table for the requested instance types.
    This is the primary entry point for fetching pricing data with full caching logic.
    """
    # 1. Ensure memory cache has something or is fresh
    if not _price_cache or len(_price_cache) <= len(_FALLBACK_PRICES_USD):
        _load_from_csv()

    # 2. Check if anything is missing after CSV load
    missing = [t for t in instance_types if t not in _price_cache]
    if missing:
        # Fetch from MCP for missing ones
        await _refresh_cache_async(missing, region)
        # Proactively save discovered prices to the CSV
        _save_to_csv()

    # 3. Build result
    # Build and format the final result dictionary
    result = {}
    for itype in sorted(set(instance_types)):
        hourly = _price_cache.get(itype, _FALLBACK_PRICES_USD.get(itype))
        if hourly is not None:
            result[itype] = {
                "hourly_usd": round(hourly, 4),
                "monthly_usd": round(hourly * 730, 2), # Standard AWS 730h month
            }
    return result


# ──────────────────────────────────────────────────────────────────
# PUBLIC ACCESSORS
# ──────────────────────────────────────────────────────────────────


def get_price(instance_type: str, region: str = "us-east-1") -> Optional[float]:
    """Instant lookup from memory cache. Returns hourly USD or None."""
    return _price_cache.get(instance_type, _FALLBACK_PRICES_USD.get(instance_type))


def get_pricing_table(
    instance_types: list[str], region: str = "us-east-1"
) -> dict[str, dict]:
    """Synchronous version. Relies on memory cache; does not trigger new API fetches."""
    result = {}
    for itype in sorted(set(instance_types)):
        hourly = get_price(itype, region)
        if hourly is not None:
            result[itype] = {
                "hourly_usd": round(hourly, 4),
                "monthly_usd": round(hourly * 730, 2),
            }
    return result


async def compare_instance_costs(
    current_type: str,
    recommended_type: str,
    region: Optional[str] = None,
    uptime_hours: Optional[float] = None,
    **kwargs,
) -> dict[str, Any]:
    """
    Calculates cost differences and percentage savings between two instance types.
    
    If uptime_hours is provided, it calculates savings based on actual observed usage
    rather than a theoretical 730h month.
    """
    norm_region = normalize_region(region or os.getenv("AWS_REGION"))
    table = await get_pricing_table_async([current_type, recommended_type], norm_region)

    current = table.get(current_type)
    recommended = table.get(recommended_type)
    if not current or not recommended:
        raise ValueError(f"Pricing unavailable for {current_type} or {recommended_type}")

    # current = table.get(current_type) # Removed as per new code
    # if not current: # Removed as per new code
    #     raise ValueError( # Removed as per new code
    #         f"Unable to find pricing for current instance type: {current_type}" # Removed as per new code
    #     ) # Removed as per new code

    # recommended = table.get(recommended_type) # Removed as per new code
    # if not recommended: # Removed as per new code
    #     raise ValueError( # Removed as per new code
    #         f"Unable to find pricing for recommended instance type: {recommended_type}" # Removed as per new code
    #     ) # Removed as per new code

    # current["instance_type"] = current_type # Removed as per new code
    # current["region"] = norm_region # Removed as per new code
    # recommended["instance_type"] = recommended_type # Removed as per new code
    # recommended["region"] = norm_region # Removed as per new code

    monthly_saving = round(current["monthly_usd"] - recommended["monthly_usd"], 2)
    hourly_saving = round(current["hourly_usd"] - recommended["hourly_usd"], 4)
    savings_pct = round((monthly_saving / current["monthly_usd"]) * 100, 2) if current["monthly_usd"] > 0 else 0.0
    # if current["monthly_usd"] > 0: # Removed as per new code
    #     savings_pct = round((monthly_saving / current["monthly_usd"]) * 100, 2) # Removed as per new code

    res = {
        "region": norm_region,
        "current": {**current, "instance_type": current_type},
        "recommended": {**recommended, "instance_type": recommended_type},
        "hourly_difference_usd": hourly_saving,
        "monthly_difference_usd": monthly_saving,
        "savings_percent": savings_pct,
    }

    if uptime_hours is not None:
        # usage_saving = round(hourly_saving * uptime_hours, 2) # Removed as per new code
        res["uptime_hours"] = uptime_hours
        res["usage_saving_usd"] = round(hourly_saving * uptime_hours, 2)

    return res


def format_pricing_for_prompt(
    instance_types: list[str], region: str = "us-east-1"
) -> str:
    """
    Formats the pricing data into a Markdown table for use in LLM prompts.
    Provides $/hr and monthly estimates for all requested types.
    """
    table = get_pricing_table(instance_types, region)

    if not table:
        return "*(Pricing data unavailable)*" # Changed from "*(Pricing data unavailable — estimates may be approximate.)*"

    lines = [
        "\n### EC2 On-Demand Pricing Reference (Linux, Shared)\n", # Changed from "### EC2 On-Demand Pricing Reference (us-east-1, Linux, Shared Tenancy)\n"
        "| Instance Type | $/hr | $/month (730h) |",
        "|---|---|---|",
    ]

    for itype, p in sorted(table.items()): # Changed from itype, prices
        lines.append(
            f"| {itype} | {p['hourly_usd']:.4f} | {p['monthly_usd']:.2f} |" # Changed from prices
        )

    # lines.append( # Removed as per new code
    #     "\n*Use the prices above for all cost calculations. Do NOT use memorized or estimated prices.*\n" # Removed as per new code
    # ) # Removed as per new code
    return "\n".join(lines)


def get_uptime_pricing_table(
    instance_uptime: list[dict],
    region: str = "us-east-1",
) -> list[dict]:
    """Joins uptime data with pricing to calculate actual usage-based costs."""
    types = list(
        {d["instance_type"] for d in instance_uptime if d.get("instance_type")}
    )
    price_table = get_pricing_table(types, region)

    results = []
    for item in instance_uptime:
        # itype = item.get("instance_type") # Removed as per new code
        # uptime_hours = item.get("uptime_hours", 0) # Removed as per new code
        pricing = price_table.get(item.get("instance_type"), {}) # Changed from itype
        hourly = pricing.get("hourly_usd", 0)

        results.append(
            {
                **item,
                "hourly_usd": hourly,
                "uptime_cost_usd": round(hourly * item.get("uptime_hours", 0), 2), # Changed from uptime_hours
                "monthly_cost_730h_usd": pricing.get("monthly_usd", 0),
            }
        )
    return results


def format_uptime_pricing_for_prompt(
    instance_uptime: list[dict],
    region: str = "us-east-1",
) -> str:
    """Builds a usage-based cost table for LLM analysis."""
    priced = get_uptime_pricing_table(instance_uptime, region)

    if not priced:
        return "*(Uptime pricing unavailable)*" # Changed from "*(Uptime pricing data unavailable.)*"

    lines = [
        "\n### EC2 Uptime-Based Cost (Actual Usage)\n",
        "| Instance ID | Type | Uptime Days | Uptime Hours | $/hr | Uptime Cost $ | Monthly 730h $ |",
        "|---|---|---|---|---|---|---|",
    ]

    for p in priced:
        lines.append(
            f"| {p.get('instance_id', '—')} "
            f"| {p.get('instance_type', '—')} "
            f"| {p.get('uptime_days', 0)} "
            f"| {p.get('uptime_hours', 0)} "
            f"| {p['hourly_usd']:.4f} "
            f"| {p['uptime_cost_usd']:.2f} "
            f"| {p['monthly_cost_730h_usd']:.2f} |"
        )

    lines.append(
        "\n*Uptime cost = hourly rate × actual uptime hours. "
        "Use uptime cost (not 730h monthly) for all savings calculations.*\n"
    )
    return "\n".join(lines)
