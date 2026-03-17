"""
savings.py — Savings Tracker CRUD + LLM markdown parser.

Handles all savings_tracker table operations and the
recommendation-parsing logic for bulk saves.
"""

from __future__ import annotations

import re
import json
import logging
from typing import Optional
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from agent_backend.agents.cost.cost_agent import get_pricing_table_async

log = logging.getLogger(__name__)


class SavingsTracker:
    """
    Manages the lifecycle of cost-saving recommendations.
    Provides CRUD operations for the `savings_tracker` table and
    implements logic to extract structured recommendations from LLM markdown.
    """

    VALID_STATUSES = {"Proposed", "Investigating", "Implemented", "Rejected"}

    def __init__(self, db=None) -> None:
        self._db = db
        self.file_path = Path(__file__).parent.parent.parent / "data" / "savings_tracker.parquet"
        self.csv_path = self.file_path.with_suffix(".csv")

    def _load_data(self) -> pd.DataFrame:
        if not self.file_path.exists():
            return pd.DataFrame(columns=[
                "id", "instance_id", "instance_name", "current_type", "recommended_type",
                "recommendation", "current_monthly_cost_usd", "recommended_monthly_cost_usd",
                "estimated_monthly_saving_usd", "status",
                "current_monthly_price_usd", "recommended_monthly_price_usd",
                "window_days", "created_at", "updated_at"
            ])
        try:
            return pd.read_parquet(self.file_path)
        except Exception as e:
            log.error(f"Failed to read savings file: {e}")
            return pd.DataFrame()

    def _save_data(self, df: pd.DataFrame):
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(self.file_path, index=False)
        df.to_csv(self.csv_path, index=False)

    # ── Single create / upsert ────────────────────────────────────

    async def create(
        self,
        instance_id: str,
        recommendation: str,
        instance_name: Optional[str] = None,
        current_type: Optional[str] = None,
        recommended_type: Optional[str] = None,
        current_monthly_cost_usd: Optional[float] = None,
        recommended_monthly_cost_usd: Optional[float] = None,
        estimated_monthly_saving_usd: Optional[float] = None,
        window_days: Optional[int] = None,
        current_monthly_price_usd: Optional[float] = None,
        recommended_monthly_price_usd: Optional[float] = None,
    ) -> dict:
        """
        Creates or updates a single savings record in a local parquet file.
        """
        df = self._load_data()
        now = datetime.now(timezone.utc).isoformat()

        # Check if exists
        mask = df["instance_id"] == instance_id
        if mask.any():
            idx = df[mask].index[0]
            # Update
            if instance_name is not None: df.at[idx, "instance_name"] = instance_name
            if current_type is not None: df.at[idx, "current_type"] = current_type
            if recommended_type is not None: df.at[idx, "recommended_type"] = recommended_type
            if recommendation is not None: df.at[idx, "recommendation"] = recommendation
            if current_monthly_cost_usd is not None: df.at[idx, "current_monthly_cost_usd"] = current_monthly_cost_usd
            if recommended_monthly_cost_usd is not None: df.at[idx, "recommended_monthly_cost_usd"] = recommended_monthly_cost_usd
            if estimated_monthly_saving_usd is not None: df.at[idx, "estimated_monthly_saving_usd"] = estimated_monthly_saving_usd
            if current_monthly_price_usd is not None: df.at[idx, "current_monthly_price_usd"] = current_monthly_price_usd
            if recommended_monthly_price_usd is not None: df.at[idx, "recommended_monthly_price_usd"] = recommended_monthly_price_usd
            if window_days is not None: df.at[idx, "window_days"] = window_days
            df.at[idx, "updated_at"] = now
            entry_id = int(df.at[idx, "id"])
            status = df.at[idx, "status"]
        else:
            # Create
            entry_id = int(df["id"].max()) + 1 if not df.empty and "id" in df.columns else 1
            status = "Proposed"
            new_row = {
                "id": entry_id,
                "instance_id": instance_id,
                "instance_name": instance_name,
                "current_type": current_type,
                "recommended_type": recommended_type,
                "recommendation": recommendation,
                "current_monthly_cost_usd": current_monthly_cost_usd,
                "recommended_monthly_cost_usd": recommended_monthly_cost_usd,
                "estimated_monthly_saving_usd": estimated_monthly_saving_usd,
                "status": status,
                "current_monthly_price_usd": current_monthly_price_usd,
                "recommended_monthly_price_usd": recommended_monthly_price_usd,
                "window_days": window_days,
                "created_at": now,
                "updated_at": now
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        self._save_data(df)
        return {
            "id": entry_id,
            "created_at": now,
            "status": status,
        }

    # ── Bulk create from LLM markdown ─────────────────────────────

    async def create_bulk(
        self,
        markdown_text: str,
        instances: list[dict],
        window_days: int,
    ) -> dict:
        """
        Processes a full LLM narrative report, parses the embedded recommendation table,
        and synchronizes the local records for all involved instances.
        """
        rec_map = self.parse_recommendations(markdown_text, instances)
        log.info(
            f"Bulk savings parse: found {len(rec_map)} recommendations from {len(instances)} instances"
        )

        df = self._load_data()
        now = datetime.now(timezone.utc).isoformat()
        saved = []

        for inst in instances:
            iid = inst.get("instance_id")
            rec = rec_map.get(iid, {})
            
            # Priority 1: Use savings already provided in the input if present
            incoming_saving = inst.get("estimated_monthly_saving_usd")
            if incoming_saving is not None:
                rec["saving"] = float(incoming_saving)
            
            # Priority 2: Calculate from prices if we have them
            else:
                cprice = inst.get("current_monthly_price_usd") or rec.get("current_price")
                rprice = inst.get("recommended_monthly_price_usd") or rec.get("recommended_price")

                # Handle Termination: recommended price is 0
                action = (inst.get("rightsizing_action") or rec.get("action") or "").lower()
                if "terminate" in action:
                    rprice = 0.0

                # Fallback to fetching prices if missing
                if cprice is None and inst.get("instance_type"):
                    try:
                        p_data = await get_pricing_table_async([inst["instance_type"]], "us-east-1")
                        cprice = p_data.get(inst["instance_type"], {}).get("monthly_usd")
                    except Exception as e:
                        log.warning(f"Failed fallback cprice fetch for {iid}: {e}")

                if rprice is None and rec.get("recommended_type"):
                    rtype_raw = rec["recommended_type"]
                    # If recommended_type is an action like "terminate", price is 0
                    if any(x in rtype_raw.lower() for x in ["terminate", "none", "n/a", "stop"]):
                        rprice = 0.0
                    else:
                        m = re.search(r"\b([a-z0-9]+\.[a-z0-9]+)\b", rtype_raw.lower())
                        rtype_norm = m.group(1) if m else None
                        if rtype_norm:
                            try:
                                p_data = await get_pricing_table_async([rtype_norm], "us-east-1")
                                rprice = p_data.get(rtype_norm, {}).get("monthly_usd")
                            except Exception as e:
                                log.warning(f"Failed fallback rprice fetch for {iid}: {e}")

                if cprice is not None and rprice is not None:
                    rec["saving"] = round(float(cprice) - float(rprice), 2)

            # File-based UPSERT
            mask = df["instance_id"] == iid
            if mask.any():
                idx = df[mask].index[0]
                if rec.get("recommended_type"): df.at[idx, "recommended_type"] = rec["recommended_type"]
                if rec.get("saving") is not None: df.at[idx, "estimated_monthly_saving_usd"] = rec["saving"]
                if cprice is not None: df.at[idx, "current_monthly_price_usd"] = cprice
                if rprice is not None: df.at[idx, "recommended_monthly_price_usd"] = rprice
                df.at[idx, "instance_name"] = inst.get("instance_name") or df.at[idx, "instance_name"]
                df.at[idx, "current_type"] = inst.get("instance_type") or df.at[idx, "current_type"]
                df.at[idx, "recommendation"] = f"Full report analysis — {window_days}d window"
                df.at[idx, "window_days"] = window_days
                df.at[idx, "updated_at"] = now
                entry_id = int(df.at[idx, "id"])
                status = df.at[idx, "status"]
            else:
                entry_id = int(df["id"].max()) + 1 if not df.empty and "id" in df.columns else 1
                status = "Proposed"
                new_row = {
                    "id": entry_id,
                    "instance_id": iid,
                    "instance_name": inst.get("instance_name"),
                    "current_type": inst.get("instance_type"),
                    "recommended_type": rec.get("recommended_type"),
                    "recommendation": f"Full report analysis — {window_days}d window",
                    "current_monthly_cost_usd": None,
                    "recommended_monthly_cost_usd": None,
                    "estimated_monthly_saving_usd": rec.get("saving"),
                    "status": status,
                    "current_monthly_price_usd": cprice,
                    "recommended_monthly_price_usd": rprice,
                    "window_days": window_days,
                    "created_at": now,
                    "updated_at": now
                }
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            
            saved.append({
                "id": entry_id,
                "instance_id": iid,
                "recommended_type": rec.get("recommended_type"),
                "status": status
            })

        self._save_data(df)
        return {
            "saved": len(saved),
            "parsed_recommendations": len(rec_map),
            "entries": saved,
        }

    # ── List ──────────────────────────────────────────────────────

    async def list(
        self,
        instance_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> dict:
        df = self._load_data()
        if df.empty:
            return {
                "entries": [],
                "total_entries": 0,
                "total_implemented_saving_usd": 0.0,
            }

        # Apply filters
        if instance_id:
            df = df[df["instance_id"] == instance_id]
        if status:
            df = df[df["status"] == status]

        df = df.sort_values(by="created_at", ascending=False)
        
        # Consistent JSON format
        records = json.loads(df.to_json(orient="records", date_format="iso"))

        total_saving = sum(
            float(r["estimated_monthly_saving_usd"] or 0)
            for r in records
            if r["status"] == "Implemented"
        )
        return {
            "entries": records,
            "total_entries": len(records),
            "total_implemented_saving_usd": round(total_saving, 2),
        }

    # ── Update status ─────────────────────────────────────────────

    async def update_status(self, entry_id: int, status: str) -> dict:
        """
        Updates the workflow status of a recommendation locally.
        """
        if status not in self.VALID_STATUSES:
            raise ValueError(f"Status must be one of {self.VALID_STATUSES}")

        df = self._load_data()
        mask = df["id"].astype(str) == str(entry_id)
        if not mask.any():
            raise LookupError(f"Entry {entry_id} not found.")

        idx = df[mask].index[0]
        df.at[idx, "status"] = status
        df.at[idx, "updated_at"] = datetime.now(timezone.utc).isoformat()
        
        updated_row = df.loc[idx].to_dict()
        self._save_data(df)
        return updated_row

    # ── Markdown parser ───────────────────────────────────────────

    @staticmethod
    def parse_recommendations(markdown: str, instances: list[dict]) -> dict[str, dict]:
        """
        Parse LLM markdown output to build a map of instance_id → recommendation.
        Handles tables where the LLM follows the prompt's table format:
          Instance ID | Current Type | Recommended Type | Reason
        """
        known_ids = {inst["instance_id"] for inst in instances}
        rec_map: dict[str, dict] = {}

        lines = markdown.split("\n")
        headers: list[str] = []
        header_indices: dict[str, int] = {}

        for line in lines:
            stripped = line.strip()
            if not stripped.startswith("|"):
                headers = []
                header_indices = {}
                continue

            cells = [c.strip() for c in stripped.split("|") if c.strip()]

            if all(re.match(r"^[-:]+$", c) for c in cells):
                continue

            lower_cells = [c.lower() for c in cells]

            if any("recommend" in c for c in lower_cells):
                headers = lower_cells
                for i, h in enumerate(headers):
                    for key in (
                        "instance",
                        "current",
                        "recommend",
                        "saving",
                        "reason",
                        "action",
                    ):
                        if key in h:
                            header_indices[key] = i
                    # Price detection
                    if "price" in h or "$" in h:
                        if "curr" in h:
                            header_indices["current_price"] = i
                        elif "rec" in h:
                            header_indices["recommended_price"] = i
                continue

            if "recommend" not in header_indices:
                continue

            rec_idx = header_indices["recommend"]
            if rec_idx >= len(cells):
                continue

            recommended_raw = cells[rec_idx].strip("`* ")

            row_text = " ".join(cells)
            matched_iid = None
            for iid in known_ids:
                if iid in row_text:
                    matched_iid = iid
                    break

            if not matched_iid:
                for inst in instances:
                    if (
                        inst.get("instance_name")
                        and inst["instance_name"].lower() in row_text.lower()
                    ):
                        matched_iid = inst["instance_id"]
                        break

            if matched_iid and recommended_raw:
                saving = None
                saving_match = re.search(r"\$\s*([\d,]+(?:\.\d+)?)", row_text)
                if saving_match:
                    try:
                        saving = float(saving_match.group(1).replace(",", ""))
                    except ValueError:
                        pass

                # Try to extract specific prices from columns
                cprice = None
                rprice = None

                if "current_price" in header_indices:
                    try:
                        cp_raw = cells[header_indices["current_price"]]
                        m = re.search(r"([\d,]+(?:\.\d+)?)", cp_raw)
                        if m:
                            cprice = float(m.group(1).replace(",", ""))
                    except (IndexError, ValueError, TypeError):
                        pass
                if "recommended_price" in header_indices:
                    try:
                        rp_raw = cells[header_indices["recommended_price"]]
                        m = re.search(r"([\d,]+(?:\.\d+)?)", rp_raw)
                        if m:
                            rprice = float(m.group(1).replace(",", ""))
                    except (IndexError, ValueError, TypeError):
                        pass

                skip_tokens = {"keep", "no change", "n/a", "none", "—", "-", "same"}
                if recommended_raw.lower() not in skip_tokens:
                    rec_map[matched_iid] = {
                        "recommended_type": recommended_raw,
                        "saving": saving,
                        "current_price": cprice,
                        "recommended_price": rprice,
                    }

        return rec_map
