from __future__ import annotations
from typing import Optional, Literal, Dict, Any, List
from fastmcp import FastMCP, Context
import json
from pathlib import Path
from datetime import datetime

mcp = FastMCP(name="PaymentsAnalyticsServer")
DATA_PATH = Path(__file__).parent / "data.json"

# ---- tiny cache ----
_data_cache: List[Dict[str, Any]] | None = None

def _load_data() -> List[Dict[str, Any]]:
    global _data_cache
    if _data_cache is None:
        with open(DATA_PATH, "r", encoding="utf-8") as f:
            _data_cache = json.load(f)
    return _data_cache


def _ensure_yyyy_mm(month: str) -> None:
    try:
        datetime.strptime(month, "%Y-%m")
    except ValueError as e:
        raise ValueError("month must be in YYYY-MM format, e.g. 2025-07") from e


def _filter_rows(
    month: Optional[str] = None,
    product: Optional[str] = None,
    clientid: Optional[str] = None,
) -> List[Dict[str, Any]]:
    rows = _load_data()
    if month:
        _ensure_yyyy_mm(month)
        rows = [r for r in rows if str(r.get("date", "")).startswith(month)]
    if product:
        rows = [r for r in rows if r.get("product") == product]
    if clientid:
        rows = [r for r in rows if r.get("clientid") == clientid]
    return rows


# =========================
# Resource: get_data_product_monthly
# =========================
# URI Template rules:
# - {month}   → required path param (YYYY-MM)
# - {?product}→ optional query param
@mcp.resource("sales://data/{month}{?product}", description="List monthly rows, optionally filtered by product.")
def get_data_product_monthly(month: str, product: Optional[str] = None) -> Dict[str, Any]:
    rows = _filter_rows(month=month, product=product)
    return {
        "resource": "get_data_product_monthly",
        "month": month,
        "product": product,
        "count": len(rows),
        "rows": rows,
    }


# =========================
# Tool: calculate_tpv_total (sum amount per client)
# =========================
@mcp.tool("calculate_tpv_total", description="TPV (sum of amount) per client id; filters: month, product, clientid")
def calculate_tpv_total(
    month: Optional[str] = None,
    product: Optional[str] = None,
    clientid: Optional[str] = None,
) -> Dict[str, Any]:
    rows = _filter_rows(month=month, product=product, clientid=clientid)
    per_client: Dict[str, int] = {}
    for r in rows:
        cid = str(r["clientid"])  # normalize
        per_client[cid] = per_client.get(cid, 0) + int(r.get("amount", 0))
    grand_total = sum(per_client.values())
    return {
        "metric": "TPV",
        "filters": {"month": month, "product": product, "clientid": clientid},
        "per_client": per_client,
        "grand_total": grand_total,
    }


# =========================
# Tool: calculate_tpt_total (count rows per client)
# =========================
@mcp.tool("calculate_tpt_total", description="TPT (count of transactions/rows) per client id; filters: month, product, clientid")
def calculate_tpt_total(
    month: Optional[str] = None,
    product: Optional[str] = None,
    clientid: Optional[str] = None,
) -> Dict[str, Any]:
    rows = _filter_rows(month=month, product=product, clientid=clientid)
    per_client: Dict[str, int] = {}
    for r in rows:
        cid = str(r["clientid"])  # normalize
        per_client[cid] = per_client.get(cid, 0) + 1
    grand_total = sum(per_client.values())
    return {
        "metric": "TPT",
        "filters": {"month": month, "product": product, "clientid": clientid},
        "per_client": per_client,
        "grand_total": grand_total,
    }


# =========================
# Optional: meta resource (for debugging)
# =========================

if __name__ == "__main__":
    # Run over stdio (default). To run HTTP:
    #   fastmcp run server.py:mcp --transport http --port 8000
    mcp.run()