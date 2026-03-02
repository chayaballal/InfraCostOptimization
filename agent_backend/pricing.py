"""
EC2 On-Demand Pricing — AWS Pricing API with TTL Cache
═══════════════════════════════════════════════════════
Fetches real-time on-demand Linux pricing via the AWS Bulk Pricing API,
caches results for 24 hours, and provides single/batch lookups.

The Pricing API is only available in us-east-1 and ap-south-1.
"""

import os
import json
import time
import logging
from typing import Optional

import boto3
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────
# MODULE-LEVEL CACHE  (instance_type → hourly_usd)
# ──────────────────────────────────────────────────────────────────
_price_cache: dict[str, float] = {}
_cache_ts: float = 0.0
_CACHE_TTL_SECONDS = 86_400  # 24 hours

# Common rightsizing target types to always pre-fetch
_RIGHTSIZING_TARGETS = [
    # Burstable
    "t3.nano", "t3.micro", "t3.small", "t3.medium", "t3.large", "t3.xlarge", "t3.2xlarge",
    "t4g.nano", "t4g.micro", "t4g.small", "t4g.medium", "t4g.large", "t4g.xlarge",
    # General purpose
    "m5.large", "m5.xlarge", "m5.2xlarge", "m5.4xlarge",
    "m7i.large", "m7i.xlarge", "m7i.2xlarge", "m7i.4xlarge",
    "m7g.medium", "m7g.large", "m7g.xlarge", "m7g.2xlarge",
    # Compute optimised
    "c5.large", "c5.xlarge", "c5.2xlarge",
    "c7i.large", "c7i.xlarge", "c7i.2xlarge",
    "c7g.medium", "c7g.large", "c7g.xlarge", "c7g.2xlarge",
    # Memory optimised
    "r5.large", "r5.xlarge", "r5.2xlarge",
    "r7i.large", "r7i.xlarge", "r7i.2xlarge",
    "r7g.large", "r7g.xlarge", "r7g.2xlarge",
    # Other common
    "t2.micro", "t2.small", "t2.medium", "t2.large",
]


from botocore.config import Config

def _create_pricing_client():
    """Create a boto3 Pricing client with zero retries for instant fallback on AccessDenied."""
    return boto3.client(
        "pricing",
        region_name="us-east-1",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        config=Config(
            retries={"max_attempts": 0},
            connect_timeout=1,
            read_timeout=1
        )
    )



def _fetch_price_for_type(client, instance_type: str, region: str = "us-east-1") -> Optional[float]:
    """
    Query the AWS Pricing API for a single instance type.
    Returns the hourly USD price or None if not found.
    """
    # The Pricing API uses long region names like "US East (N. Virginia)"
    region_map = {
        "us-east-1": "US East (N. Virginia)",
        "us-east-2": "US East (Ohio)",
        "us-west-1": "US West (N. California)",
        "us-west-2": "US West (Oregon)",
        "ap-south-1": "Asia Pacific (Mumbai)",
        "ap-southeast-1": "Asia Pacific (Singapore)",
        "eu-west-1": "Europe (Ireland)",
        "eu-central-1": "Europe (Frankfurt)",
    }
    location = region_map.get(region, "US East (N. Virginia)")

    try:
        response = client.get_products(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
                {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
            ],
            MaxResults=1,
        )

        if not response.get("PriceList"):
            return None

        product = json.loads(response["PriceList"][0])
        terms = product.get("terms", {}).get("OnDemand", {})

        for term in terms.values():
            for dimension in term.get("priceDimensions", {}).values():
                price_str = dimension.get("pricePerUnit", {}).get("USD", "0")
                price = float(price_str)
                if price > 0:
                    return price

    except Exception as e:
        # Fallback dictionary for common instances if IAM permission is missing
        fallback_prices_usd = {
            "t2.micro": 0.0116, "t2.small": 0.023, "t2.medium": 0.0464, "t2.large": 0.0928,
            "t3.nano": 0.0052, "t3.micro": 0.0104, "t3.small": 0.0208, "t3.medium": 0.0416, "t3.large": 0.0832, "t3.xlarge": 0.1664, "t3.2xlarge": 0.3328,
            "t4g.nano": 0.0042, "t4g.micro": 0.0084, "t4g.small": 0.0168, "t4g.medium": 0.0336, "t4g.large": 0.0672, "t4g.xlarge": 0.1344,
            "m5.large": 0.096, "m5.xlarge": 0.192, "m5.2xlarge": 0.384, "m5.4xlarge": 0.768,
            "m7g.medium": 0.0408, "m7g.large": 0.0816, "m7g.xlarge": 0.1632, "m7g.2xlarge": 0.3264,
            "m7i.large": 0.1008, "m7i.xlarge": 0.2016, "m7i.2xlarge": 0.4032,
            "c5.large": 0.085, "c5.xlarge": 0.170, "c5.2xlarge": 0.340,
            "c7g.medium": 0.0346, "c7g.large": 0.0692, "c7g.xlarge": 0.1384, "c7g.2xlarge": 0.2768,
            "c7i.large": 0.085, "c7i.xlarge": 0.170, "c7i.2xlarge": 0.340,
            "r5.large": 0.126, "r5.xlarge": 0.252, "r5.2xlarge": 0.504,
            "r7g.large": 0.1064, "r7g.xlarge": 0.2128, "r7g.2xlarge": 0.4256,
            "r7i.large": 0.133, "r7i.xlarge": 0.266, "r7i.2xlarge": 0.532,
        }
        if "AccessDenied" in str(e) and instance_type in fallback_prices_usd:
            return fallback_prices_usd[instance_type]
            
        log.warning(f"Pricing API error for {instance_type}: {e}")

    return None

def _fetch_wrapper(itype: str, region: str):
    client = _create_pricing_client()
    return itype, _fetch_price_for_type(client, itype, region)

def _refresh_cache(instance_types: list[str], region: str = "us-east-1") -> None:
    """Fetch prices for all requested types and populate the cache in parallel."""
    global _price_cache, _cache_ts

    # Deduplicate and merge with rightsizing targets
    all_types = list(set(instance_types + _RIGHTSIZING_TARGETS))
    to_fetch = [t for t in all_types if t not in _price_cache]
    
    if not to_fetch:
        _cache_ts = time.time()
        return

    fetched = 0
    errors = 0

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_fetch_wrapper, t, region): t for t in to_fetch}
        for future in concurrent.futures.as_completed(futures):
            try:
                itype, price = future.result()
                if price is not None:
                    _price_cache[itype] = price
                    fetched += 1
                else:
                    errors += 1
            except Exception as e:
                errors += 1
                log.warning(f"Error fetching price for {futures[future]}: {e}")

    _cache_ts = time.time()
    log.info(f"Pricing cache refreshed: {fetched} new, {errors} not found, {len(_price_cache)} total cached.")


def _is_cache_valid() -> bool:
    return _cache_ts > 0 and (time.time() - _cache_ts) < _CACHE_TTL_SECONDS


# ──────────────────────────────────────────────────────────────────
# PUBLIC API
# ──────────────────────────────────────────────────────────────────

def get_price(instance_type: str, region: str = "us-east-1") -> Optional[float]:
    """
    Get the hourly on-demand price for a single instance type.
    Uses the cache, refreshes if stale.
    """
    if not _is_cache_valid() or instance_type not in _price_cache:
        _refresh_cache([instance_type], region)
    return _price_cache.get(instance_type)


def get_pricing_table(instance_types: list[str], region: str = "us-east-1") -> dict[str, dict]:
    """
    Get a pricing lookup table for a list of instance types.
    Returns { instance_type: { hourly_usd, monthly_usd } }
    """
    if not _is_cache_valid():
        _refresh_cache(instance_types, region)
    else:
        # Ensure all requested types are in the cache
        missing = [t for t in instance_types if t not in _price_cache]
        if missing:
            _refresh_cache(missing, region)

    result = {}
    for itype in sorted(set(instance_types + list(_price_cache.keys()))):
        hourly = _price_cache.get(itype)
        if hourly is not None:
            result[itype] = {
                "hourly_usd": round(hourly, 4),
                "monthly_usd": round(hourly * 730, 2),  # 730 hours/month standard
            }
    return result


def format_pricing_for_prompt(instance_types: list[str], region: str = "us-east-1") -> str:
    """
    Build a markdown pricing table suitable for LLM prompt injection.
    """
    table = get_pricing_table(instance_types, region)

    if not table:
        return "*(Pricing data unavailable — estimates may be approximate.)*"

    lines = [
        "\n### EC2 On-Demand Pricing Reference (us-east-1, Linux, Shared Tenancy)\n",
        "| Instance Type | $/hr | $/month (730h) |",
        "|---|---|---|",
    ]

    for itype, prices in sorted(table.items()):
        lines.append(f"| {itype} | {prices['hourly_usd']:.4f} | {prices['monthly_usd']:.2f} |")

    lines.append(
        "\n*Use the prices above for all cost calculations. Do NOT use memorized or estimated prices.*\n"
    )
    return "\n".join(lines)
