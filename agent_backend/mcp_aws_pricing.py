"""
AWS MCP pricing adapter for EC2 instance cost comparison.

Primary path:
  - Uses AWS Pricing MCP server via stdio transport.
Fallback path:
  - Uses local boto3-based pricing module if MCP is unavailable.
"""

from __future__ import annotations

import os
import json
import asyncio
import logging
from typing import Any, Optional

from agent_backend.pricing import get_pricing_table

log = logging.getLogger(__name__)

DEFAULT_REGION = "us-east-1"
VALID_REGIONS = {
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "ap-south-1",
    "ap-southeast-1",
    "eu-west-1",
    "eu-central-1",
}


def normalize_region(raw_region: Optional[str]) -> str:
    """
    Normalize AWS region.
    If an AZ is passed (for example us-east-1d), convert to region (us-east-1).
    """
    region = (raw_region or "").strip().lower()
    if not region:
        return DEFAULT_REGION
    if region in VALID_REGIONS:
        return region
    if len(region) > 2 and region[:-1] in VALID_REGIONS:
        return region[:-1]
    return DEFAULT_REGION


def _monthly(hourly: float) -> float:
    return round(hourly * 730, 2)


def _extract_price_from_tool_content(payload: Any) -> Optional[float]:
    """
    Best-effort parse across MCP tool response shapes.
    """
    if payload is None:
        return None

    if isinstance(payload, (int, float)):
        return float(payload)

    if isinstance(payload, dict):
        for key in ("hourly_usd", "price_per_hour_usd", "usd_per_hour", "price"):
            if key in payload:
                try:
                    return float(payload[key])
                except (TypeError, ValueError):
                    pass
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
        try:
            parsed_json = json.loads(text)
            return _extract_price_from_tool_content(parsed_json)
        except Exception:
            return None

    return None


async def _try_price_from_mcp(instance_type: str, region: str) -> Optional[float]:
    """
    Query AWS Pricing MCP server for EC2 On-Demand Linux shared tenancy price.
    Returns hourly USD if successful, else None.
    """
    try:
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client
    except Exception:
        return None

    # Most environments use uvx for MCP servers.
    # Keep command overridable for portability.
    mcp_command = os.getenv("AWS_MCP_COMMAND", "uvx")
    mcp_args = os.getenv("AWS_PRICING_MCP_ARGS", "").strip()
    if mcp_args:
        args = mcp_args.split()
    else:
        args = ["awslabs.aws-pricing-mcp-server@latest"]

    env = {
        "AWS_REGION": region,
        "AWS_DEFAULT_REGION": region,
    }
    if os.getenv("AWS_PROFILE"):
        env["AWS_PROFILE"] = os.getenv("AWS_PROFILE")
    if os.getenv("AWS_ACCESS_KEY_ID"):
        env["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID")
    if os.getenv("AWS_SECRET_ACCESS_KEY"):
        env["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY")
    if os.getenv("AWS_SESSION_TOKEN"):
        env["AWS_SESSION_TOKEN"] = os.getenv("AWS_SESSION_TOKEN")

    server = StdioServerParameters(command=mcp_command, args=args, env=env)

    async with stdio_client(server) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            tool_names = [t.name for t in tools.tools]

            # Prefer explicit EC2 pricing tools if present.
            preferred = [
                "get_ec2_instance_price",
                "get_ec2_pricing",
                "get_pricing",
                "search_products",
            ]
            chosen = next((n for n in preferred if n in tool_names), None)
            if not chosen:
                return None

            inputs = {
                "instance_type": instance_type,
                "region": region,
                "operating_system": "Linux",
                "tenancy": "Shared",
                "term_type": "OnDemand",
            }

            result = await session.call_tool(chosen, inputs)
            content = getattr(result, "content", None)
            return _extract_price_from_tool_content(content)


async def get_price_with_mcp_fallback(instance_type: str, region: str) -> dict[str, Any]:
    """
    Returns price payload:
      {
        "instance_type": "m5.large",
        "hourly_usd": 0.096,
        "monthly_usd": 70.08,
        "source": "mcp|fallback_pricing_module"
      }
    """
    normalized_region = normalize_region(region)
    hourly: Optional[float] = None
    source = "fallback_pricing_module"

    if os.getenv("ENABLE_AWS_MCP", "true").strip().lower() in {"1", "true", "yes"}:
        try:
            hourly = await asyncio.wait_for(
                _try_price_from_mcp(instance_type=instance_type, region=normalized_region),
                timeout=8.0,
            )
            if hourly is not None:
                source = "mcp"
        except Exception as e:
            log.warning(f"MCP pricing lookup failed for {instance_type}: {e}")

    if hourly is None:
        table = await asyncio.to_thread(get_pricing_table, [instance_type], normalized_region)
        entry = table.get(instance_type)
        if not entry:
            raise ValueError(f"Unable to find pricing for instance type: {instance_type}")
        hourly = float(entry["hourly_usd"])
        source = "fallback_pricing_module"

    return {
        "instance_type": instance_type,
        "hourly_usd": round(float(hourly), 4),
        "monthly_usd": _monthly(float(hourly)),
        "source": source,
        "region": normalized_region,
    }


async def compare_instance_costs(
    current_type: str,
    recommended_type: str,
    region: Optional[str] = None,
) -> dict[str, Any]:
    """
    Compare EC2 current vs recommended instance prices.
    """
    normalized_region = normalize_region(region or os.getenv("AWS_REGION"))
    current = await get_price_with_mcp_fallback(current_type, normalized_region)
    recommended = await get_price_with_mcp_fallback(recommended_type, normalized_region)

    monthly_saving = round(current["monthly_usd"] - recommended["monthly_usd"], 2)
    hourly_saving = round(current["hourly_usd"] - recommended["hourly_usd"], 4)
    savings_pct = 0.0
    if current["monthly_usd"] > 0:
        savings_pct = round((monthly_saving / current["monthly_usd"]) * 100, 2)

    return {
        "region": normalized_region,
        "current": current,
        "recommended": recommended,
        "hourly_difference_usd": hourly_saving,
        "monthly_difference_usd": monthly_saving,
        "savings_percent": savings_pct,
    }
