"""
savings.py — Savings Tracker CRUD + LLM markdown parser.

Handles all savings_tracker table operations and the
recommendation-parsing logic for bulk saves.
"""

from __future__ import annotations

import re
import logging
from typing import Optional

from sqlalchemy import text
from agent_backend.mcp_aws_pricing import get_price_with_mcp_fallback

log = logging.getLogger(__name__)


class SavingsTracker:
    """CRUD operations for the savings_tracker table."""

    VALID_STATUSES = {"Proposed", "Investigating", "Implemented", "Rejected"}

    def __init__(self, db) -> None:
        self._db = db

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
        async with self._db.session_factory() as session:
            result = await session.execute(text("""
                INSERT INTO savings_tracker
                    (instance_id, instance_name, current_type, recommended_type,
                     recommendation, current_monthly_cost_usd, recommended_monthly_cost_usd,
                     estimated_monthly_saving_usd, window_days,
                     current_monthly_price_usd, recommended_monthly_price_usd)
                VALUES
                    (:iid, :iname, :ctype, :rtype, :rec, :cur_cost, :rec_cost, :saving, :win, :cprice, :rprice)
                ON CONFLICT (instance_id)
                DO UPDATE SET
                    recommended_type             = COALESCE(EXCLUDED.recommended_type, savings_tracker.recommended_type),
                    current_monthly_cost_usd     = COALESCE(EXCLUDED.current_monthly_cost_usd, savings_tracker.current_monthly_cost_usd),
                    recommended_monthly_cost_usd = COALESCE(EXCLUDED.recommended_monthly_cost_usd, savings_tracker.recommended_monthly_cost_usd),
                    estimated_monthly_saving_usd = COALESCE(EXCLUDED.estimated_monthly_saving_usd, savings_tracker.estimated_monthly_saving_usd),
                    current_monthly_price_usd    = COALESCE(EXCLUDED.current_monthly_price_usd, savings_tracker.current_monthly_price_usd),
                    recommended_monthly_price_usd = COALESCE(EXCLUDED.recommended_monthly_price_usd, savings_tracker.recommended_monthly_price_usd),
                    instance_name                = COALESCE(EXCLUDED.instance_name, savings_tracker.instance_name),
                    current_type                 = COALESCE(EXCLUDED.current_type, savings_tracker.current_type),
                    recommendation               = EXCLUDED.recommendation,
                    window_days                  = COALESCE(EXCLUDED.window_days, savings_tracker.window_days),
                    updated_at                   = NOW()
                RETURNING id, created_at, status
            """), {
                "iid":    instance_id,
                "iname":  instance_name,
                "ctype":  current_type,
                "rtype":  recommended_type,
                "rec":    recommendation,
                "cur_cost": current_monthly_cost_usd,
                "rec_cost": recommended_monthly_cost_usd,
                "saving": estimated_monthly_saving_usd,
                "win":    window_days,
                "cprice": current_monthly_price_usd,
                "rprice": recommended_monthly_price_usd,
            })
            await session.commit()
            row = result.mappings().fetchone()
            return {"id": row["id"], "created_at": str(row["created_at"]), "status": row["status"]}

    # ── Bulk create from LLM markdown ─────────────────────────────

    async def create_bulk(
        self,
        markdown_text: str,
        instances: list[dict],
        window_days: int,
    ) -> dict:
        rec_map = self.parse_recommendations(markdown_text, instances)
        log.info(f"Bulk savings parse: found {len(rec_map)} recommendations from {len(instances)} instances")

        saved = []
        async with self._db.session_factory() as session:
            for inst in instances:
                iid = inst.get("instance_id")
                rec = rec_map.get(iid, {})
                # Structured prices sent by frontend (meta) or parsed from markdown (rec)
                cprice = inst.get("current_monthly_price_usd") or rec.get("current_price")
                rprice = inst.get("recommended_monthly_price_usd") or rec.get("recommended_price")

                # Fallback to fetching prices if missing
                if cprice is None and inst.get("instance_type"):
                    try:
                        p_data = await get_price_with_mcp_fallback(inst["instance_type"], "us-east-1")
                        cprice = p_data.get("monthly_usd")
                    except Exception as e:
                        log.warning(f"Failed fallback cprice fetch for {iid}: {e}")

                if rprice is None and rec.get("recommended_type"):
                    rtype_norm = rec["recommended_type"]
                    # Quick extraction if it's a "t3.medium (Save $...)" string
                    m = re.search(r"([a-z0-9]+\.[a-z0-9]+)", rtype_norm.lower())
                    if m:
                        rtype_norm = m.group(1)
                    
                    try:
                        p_data = await get_price_with_mcp_fallback(rtype_norm, "us-east-1")
                        rprice = p_data.get("monthly_usd")
                    except Exception as e:
                        log.warning(f"Failed fallback rprice fetch for {iid}: {e}")

                # Calculate savings if pieces are missing from markdown but sent in meta
                if (cprice is not None and rprice is not None):
                    rec["saving"] = round(float(cprice) - float(rprice), 2)

                result = await session.execute(text("""
                    INSERT INTO savings_tracker
                        (instance_id, instance_name, current_type, recommended_type,
                         recommendation, current_monthly_cost_usd, recommended_monthly_cost_usd,
                         estimated_monthly_saving_usd, window_days,
                         current_monthly_price_usd, recommended_monthly_price_usd)
                    VALUES
                        (:iid, :iname, :ctype, :rtype, :rec, :cur_cost, :rec_cost, :saving, :win, :cprice, :rprice)
                    ON CONFLICT (instance_id)
                    DO UPDATE SET
                        recommended_type             = COALESCE(EXCLUDED.recommended_type, savings_tracker.recommended_type),
                        current_monthly_cost_usd     = COALESCE(EXCLUDED.current_monthly_cost_usd, savings_tracker.current_monthly_cost_usd),
                        recommended_monthly_cost_usd = COALESCE(EXCLUDED.recommended_monthly_cost_usd, savings_tracker.recommended_monthly_cost_usd),
                        estimated_monthly_saving_usd = COALESCE(EXCLUDED.estimated_monthly_saving_usd, savings_tracker.estimated_monthly_saving_usd),
                        current_monthly_price_usd    = COALESCE(EXCLUDED.current_monthly_price_usd, savings_tracker.current_monthly_price_usd),
                        recommended_monthly_price_usd = COALESCE(EXCLUDED.recommended_monthly_price_usd, savings_tracker.recommended_monthly_price_usd),
                        instance_name                = COALESCE(EXCLUDED.instance_name, savings_tracker.instance_name),
                        current_type                 = COALESCE(EXCLUDED.current_type, savings_tracker.current_type),
                        recommendation               = EXCLUDED.recommendation,
                        window_days                  = COALESCE(EXCLUDED.window_days, savings_tracker.window_days),
                        updated_at                   = NOW()
                    RETURNING id, status
                """), {
                    "iid":    iid,
                    "iname":  inst.get("instance_name"),
                    "ctype":  inst.get("instance_type"),
                    "rtype":  rec.get("recommended_type"),
                    "cur_cost": None,
                    "rec_cost": None,
                    "saving": rec.get("saving"),
                    "rec":    f"Full report analysis — {window_days}d window",
                    "win":    window_days,
                    "cprice": cprice,
                    "rprice": rprice,
                })
                row = result.mappings().fetchone()
                saved.append({
                    "id": row["id"],
                    "instance_id": iid,
                    "recommended_type": rec.get("recommended_type"),
                    "status": row["status"],
                })
            await session.commit()

        return {"saved": len(saved), "parsed_recommendations": len(rec_map), "entries": saved}

    # ── List ──────────────────────────────────────────────────────

    async def list(
        self,
        instance_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> dict:
        where = ["1=1"]
        params: dict = {}
        if instance_id:
            where.append("instance_id = :iid")
            params["iid"] = instance_id
        if status:
            where.append("status = :status")
            params["status"] = status

        sql = text(f"""
            SELECT id, instance_id, instance_name, current_type, recommended_type,
                   recommendation, current_monthly_cost_usd, recommended_monthly_cost_usd,
                   estimated_monthly_saving_usd, status,
                   current_monthly_price_usd, recommended_monthly_price_usd,
                   window_days, created_at, updated_at
            FROM savings_tracker
            WHERE {' AND '.join(where)}
              AND EXISTS (
                  SELECT 1
                  FROM savings_tracker m
                  WHERE m.instance_id = savings_tracker.instance_id
              )
            ORDER BY created_at DESC
        """)
        async with self._db.session_factory() as session:
            result = await session.execute(sql, params)
            rows = result.mappings().all()

        total_saving = sum(
            float(r["estimated_monthly_saving_usd"] or 0)
            for r in rows if r["status"] == "Implemented"
        )
        return {
            "entries": [dict(r) for r in rows],
            "total_entries": len(rows),
            "total_implemented_saving_usd": round(total_saving, 2),
        }

    # ── Update status ─────────────────────────────────────────────

    async def update_status(self, entry_id: int, status: str) -> dict:
        if status not in self.VALID_STATUSES:
            raise ValueError(f"Status must be one of {self.VALID_STATUSES}")

        async with self._db.session_factory() as session:
            result = await session.execute(text("""
                UPDATE savings_tracker
                SET status = :status, updated_at = NOW()
                WHERE id = :id
                RETURNING id, status, updated_at
            """), {"status": status, "id": entry_id})
            await session.commit()
            row = result.mappings().fetchone()
            if not row:
                raise LookupError(f"Entry {entry_id} not found.")
            return dict(row)

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
                    for key in ("instance", "current", "recommend", "saving", "reason", "action"):
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
                    if inst.get("instance_name") and inst["instance_name"].lower() in row_text.lower():
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
