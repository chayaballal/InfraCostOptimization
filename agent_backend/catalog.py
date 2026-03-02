"""
instance_catalog.py
─────────────────────────────────────────────────────────────────────────────
Fetches and caches all current-generation EC2 instance types from the
AWS EC2 DescribeInstanceTypes API.

Cache behavior:
  • Results are stored in memory for CATALOG_TTL_HOURS (24h by default).
  • If the boto3 call fails (network, IAM, etc.) the cache returns a short
    fallback table so the system degrades gracefully.

IAM permission required:   ec2:DescribeInstanceTypes   (read-only)
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

log = logging.getLogger(__name__)

CATALOG_TTL_HOURS = 24

# ── in-memory cache ────────────────────────────────────────────────
_catalog_cache: dict[str, dict] = {}   # instance_type → spec dict
_cache_loaded_at: float          = 0.0

# ── minimal fallback so we never return nothing ────────────────────
_FALLBACK_CATALOG: list[dict] = [
    {"instance_type": "t3.micro",   "vcpus": 2,  "memory_gb": 1.0,  "architecture": "x86_64", "network_gbps": 0.064},
    {"instance_type": "t3.small",   "vcpus": 2,  "memory_gb": 2.0,  "architecture": "x86_64", "network_gbps": 0.064},
    {"instance_type": "t3.medium",  "vcpus": 2,  "memory_gb": 4.0,  "architecture": "x86_64", "network_gbps": 0.256},
    {"instance_type": "t3.large",   "vcpus": 2,  "memory_gb": 8.0,  "architecture": "x86_64", "network_gbps": 0.512},
    {"instance_type": "t4g.micro",  "vcpus": 2,  "memory_gb": 1.0,  "architecture": "arm64",  "network_gbps": 0.064},
    {"instance_type": "t4g.small",  "vcpus": 2,  "memory_gb": 2.0,  "architecture": "arm64",  "network_gbps": 0.064},
    {"instance_type": "t4g.medium", "vcpus": 2,  "memory_gb": 4.0,  "architecture": "arm64",  "network_gbps": 0.256},
    {"instance_type": "m5.large",   "vcpus": 2,  "memory_gb": 8.0,  "architecture": "x86_64", "network_gbps": 0.75},
    {"instance_type": "m5.xlarge",  "vcpus": 4,  "memory_gb": 16.0, "architecture": "x86_64", "network_gbps": 1.25},
    {"instance_type": "m5.2xlarge", "vcpus": 8,  "memory_gb": 32.0, "architecture": "x86_64", "network_gbps": 2.5},
    {"instance_type": "m7i.large",  "vcpus": 2,  "memory_gb": 8.0,  "architecture": "x86_64", "network_gbps": 1.04},
    {"instance_type": "m7i.xlarge", "vcpus": 4,  "memory_gb": 16.0, "architecture": "x86_64", "network_gbps": 2.08},
    {"instance_type": "m7g.large",  "vcpus": 2,  "memory_gb": 8.0,  "architecture": "arm64",  "network_gbps": 1.04},
    {"instance_type": "m7g.xlarge", "vcpus": 4,  "memory_gb": 16.0, "architecture": "arm64",  "network_gbps": 2.08},
    {"instance_type": "c5.large",   "vcpus": 2,  "memory_gb": 4.0,  "architecture": "x86_64", "network_gbps": 0.75},
    {"instance_type": "c5.xlarge",  "vcpus": 4,  "memory_gb": 8.0,  "architecture": "x86_64", "network_gbps": 1.25},
    {"instance_type": "c7i.large",  "vcpus": 2,  "memory_gb": 4.0,  "architecture": "x86_64", "network_gbps": 1.04},
    {"instance_type": "c7g.large",  "vcpus": 2,  "memory_gb": 4.0,  "architecture": "arm64",  "network_gbps": 1.04},
    {"instance_type": "r5.large",   "vcpus": 2,  "memory_gb": 16.0, "architecture": "x86_64", "network_gbps": 0.75},
    {"instance_type": "r5.xlarge",  "vcpus": 4,  "memory_gb": 32.0, "architecture": "x86_64", "network_gbps": 1.25},
    {"instance_type": "r7i.large",  "vcpus": 2,  "memory_gb": 16.0, "architecture": "x86_64", "network_gbps": 1.04},
    {"instance_type": "r7g.large",  "vcpus": 2,  "memory_gb": 16.0, "architecture": "arm64",  "network_gbps": 1.04},
]


def _is_cache_fresh() -> bool:
    return bool(_catalog_cache) and (time.time() - _cache_loaded_at) < CATALOG_TTL_HOURS * 3600


def _load_catalog_from_aws(region: str = "us-east-1") -> None:
    """Populate _catalog_cache by paginating DescribeInstanceTypes."""
    global _cache_loaded_at
    ec2 = boto3.client("ec2", region_name=region)
    paginator = ec2.get_paginator("describe_instance_types")
    page_iter = paginator.paginate(
        Filters=[{"Name": "current-generation", "Values": ["true"]}]
    )
    loaded = 0
    for page in page_iter:
        for it in page.get("InstanceTypes", []):
            itype = it.get("InstanceType", "")
            vcpus = it.get("VCpuInfo", {}).get("DefaultVCpus", 0)
            mem_mb = it.get("MemoryInfo", {}).get("SizeInMiB", 0)
            mem_gb = round(mem_mb / 1024, 1)
            archs = it.get("ProcessorInfo", {}).get("SupportedArchitectures", ["x86_64"])
            arch = "arm64" if "arm64" in archs else "x86_64"

            # Network — use maximum card bandwidth if available
            net_info = it.get("NetworkInfo", {})
            net_gbps: Optional[float] = net_info.get("NetworkCards", [{}])[0].get("BaselineBandwidthInGbps")
            if net_gbps is None:
                net_perf = net_info.get("NetworkPerformance", "")
                # e.g. "10 Gigabit" → 10.0
                try:
                    net_gbps = float(net_perf.split()[0])
                except (ValueError, IndexError):
                    net_gbps = None

            _catalog_cache[itype] = {
                "instance_type": itype,
                "vcpus":         vcpus,
                "memory_gb":     mem_gb,
                "architecture":  arch,
                "network_gbps":  net_gbps,
            }
            loaded += 1

    _cache_loaded_at = time.time()
    log.info(f"Instance catalog loaded from AWS: {loaded} types cached.")


def get_catalog(region: str = "us-east-1") -> list[dict]:
    """
    Return the full sorted instance catalog.
    Loads from AWS on first call; uses cache thereafter.
    Falls back to a hand-curated list if AWS API is unavailable.
    """
    global _catalog_cache
    if not _is_cache_fresh():
        try:
            _load_catalog_from_aws(region)
        except (BotoCoreError, ClientError, Exception) as e:
            log.warning(f"Instance catalog AWS fetch failed: {e}. Using fallback catalog.")
            if not _catalog_cache:
                _catalog_cache = {d["instance_type"]: d for d in _FALLBACK_CATALOG}

    return sorted(_catalog_cache.values(), key=lambda x: x["instance_type"])


def get_specs(instance_type: str, region: str = "us-east-1") -> Optional[dict]:
    """Return specs dict for a specific instance type, or None if not found."""
    get_catalog(region)
    return _catalog_cache.get(instance_type)


def format_catalog_for_prompt(instance_types: list[str], region: str = "us-east-1") -> str:
    """
    Build a compact markdown table of specs for the given types
    plus common rightsizing targets, for injection into the LLM prompt.
    """
    catalog = get_catalog(region)
    catalog_map = {d["instance_type"]: d for d in catalog}

    # Combine requested types + common targets
    targets = set(instance_types) | {
        "t3.medium", "t3.large", "t4g.medium", "t4g.large",
        "m5.large", "m7i.large", "m7g.large",
        "c5.large", "c7i.large", "c7g.large",
        "r5.large", "r7i.large", "r7g.large",
    }

    rows = [catalog_map[t] for t in sorted(targets) if t in catalog_map]
    if not rows:
        return ""

    lines = ["| Instance Type | vCPU | RAM (GB) | Architecture | Network (Gbps) |",
             "|---|---|---|---|---|"]
    for r in rows:
        net = f"{r['network_gbps']:.2f}" if r["network_gbps"] is not None else "—"
        lines.append(f"| {r['instance_type']} | {r['vcpus']} | {r['memory_gb']} | {r['architecture']} | {net} |")

    return "\n## Instance Specification Catalog\n" + "\n".join(lines) + "\n"
