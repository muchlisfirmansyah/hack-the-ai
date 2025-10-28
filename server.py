from __future__ import annotations
from typing import Optional, Literal, Dict, Any, List
from fastmcp import FastMCP, Context
import json
from pathlib import Path
from datetime import datetime

mcp = FastMCP(name="PaymentsAnalyticsServer")
DATA_PATH = Path(__file__).parent / "mcp_training_data.json"

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
    # Ganti 'clientid' menjadi 'brand_id'
    brand_id: Optional[str] = None, 
) -> List[Dict[str, Any]]:
    rows = _load_data()
    if month:
        _ensure_yyyy_mm(month)
        # Gunakan kolom 'month' yang sudah berformat YYYY-MM
        rows = [r for r in rows if str(r.get("month", "")).startswith(month)]
    if product:
        rows = [r for r in rows if r.get("product") == product]
    if brand_id:
        # Gunakan kolom 'brand_id'
        rows = [r for r in rows if str(r.get("brand_id")) == brand_id]
        
    return rows

def _filter_rows_by_type(
    data_type: Literal["churn", "profit"],
    month: Optional[str] = None,
    product: Optional[str] = None,
    brand_id: Optional[str] = None, # Ganti clientid menjadi brand_id
) -> List[Dict[str, Any]]:
    rows = _load_data()
    # Filter berdasarkan tipe
    rows = [r for r in rows if r.get("type") == data_type]
    
    if month:
        _ensure_yyyy_mm(month)
        # Asumsi 'month' di data Anda sudah berformat 'YYYY-MM'
        rows = [r for r in rows if r.get("month") == month] 
    if product:
        rows = [r for r in rows if r.get("product") == product]
    if brand_id:
        # Ganti 'clientid' dengan 'brand_id'
        rows = [r for r in rows if str(r.get("brand_id")) == brand_id]
        
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
# Ganti nama parameter dari clientid menjadi brand_id
@mcp.tool(
    "calculate_tpv_total", 
    description="Menghitung total TPV (Total Payment Volume) per brand_id; filters: month, product, brand_id"
)
def calculate_tpv_total(
    month: Optional[str] = None,
    product: Optional[str] = None,
    brand_id: Optional[str] = None, # Perubahan di sini
) -> Dict[str, Any]:
    # Panggil fungsi filter yang sudah diubah
    rows = _filter_rows(month=month, product=product, brand_id=brand_id)
    per_client: Dict[str, int] = {}
    for r in rows:
        # Gunakan 'brand_id' dan kolom 'tpv'
        bid = str(r["brand_id"])
        # Asumsi 'tpv' sudah integer dari proses JSON sebelumnya
        per_client[bid] = per_client.get(bid, 0) + int(r.get("tpv", 0))
        
    grand_total = sum(per_client.values())
    return {
        "metric": "TPV",
        # Perubahan di sini
        "filters": {"month": month, "product": product, "brand_id": brand_id},
        "per_client": per_client,
        "grand_total": grand_total,
    }


# =========================
# Tool: calculate_tpt_total (count rows per client)
# =========================
# Ganti nama parameter dari clientid menjadi brand_id
@mcp.tool(
    "calculate_tpt_total", 
    description="Menghitung total TPT (Total Payment Transaction/Count) per brand_id; filters: month, product, brand_id"
)
def calculate_tpt_total(
    month: Optional[str] = None,
    product: Optional[str] = None,
    brand_id: Optional[str] = None, # Perubahan di sini
) -> Dict[str, Any]:
    # Panggil fungsi filter yang sudah diubah
    rows = _filter_rows(month=month, product=product, brand_id=brand_id)
    per_client: Dict[str, int] = {}
    for r in rows:
        # Gunakan 'brand_id' dan kolom 'tpt'
        bid = str(r["brand_id"])
        # Asumsi 'tpt' sudah integer dari proses JSON sebelumnya
        per_client[bid] = per_client.get(bid, 0) + int(r.get("tpt", 0))
        
    grand_total = sum(per_client.values())
    return {
        "metric": "TPT",
        # Perubahan di sini
        "filters": {"month": month, "product": product, "brand_id": brand_id},
        "per_client": per_client,
        "grand_total": grand_total,
    }

# =========================
# Tool: get_churn_candidates (mengidentifikasi merchant churn/potensi churn)
# =========================
@mcp.tool(
    "get_churn_candidates", 
    description="Mengidentifikasi brand_id merchant yang berpotensi churn atau sudah churn; filters: month, product"
)
def get_churn_candidates(
    month: Optional[str] = None,
    product: Optional[str] = None,
) -> Dict[str, Any]:
    # Mengambil semua baris dengan type: churn
    rows = _filter_rows_by_type(data_type="churn", month=month, product=product)
    
    # Ambil daftar unik brand_id
    churn_brand_ids = sorted(list(set(r["brand_id"] for r in rows if "brand_id" in r)))
    
    total_candidates = len(churn_brand_ids)
    
    return {
        "metric": "Churn Candidates",
        "filters": {"month": month, "product": product},
        "total_candidates": total_candidates,
        "brand_ids": churn_brand_ids,
        "note": "Brand IDs ini adalah merchant yang tercatat sebagai 'churn' dalam data training."
    }

# =========================
# Tool: calculate_profit_total (TPV dari data profit)
# =========================
@mcp.tool(
    "calculate_profit_total", 
    description="Menghitung total TPV dari transaksi berlabel 'profit' (dianggap profit); filters: month, product, brand_id"
)
def calculate_profit_total(
    month: Optional[str] = None,
    product: Optional[str] = None,
    brand_id: Optional[str] = None,
) -> Dict[str, Any]:
    # Mengambil semua baris dengan type: profit
    rows = _filter_rows_by_type(data_type="profit", month=month, product=product, brand_id=brand_id)
    
    per_client: Dict[str, int] = {}
    for r in rows:
        # Gunakan 'brand_id' dari data training
        bid = str(r["brand_id"])  
        # Asumsi 'tpv' adalah metrik profit
        per_client[bid] = per_client.get(bid, 0) + int(r.get("tpv", 0)) 
        
    grand_total = sum(per_client.values())
    
    return {
        "metric": "Profit TPV",
        "filters": {"month": month, "product": product, "brand_id": brand_id},
        "per_client": per_client,
        "grand_total": grand_total,
        "note": "Total dihitung dari kolom 'tpv' pada data training dengan type 'profit'."
    }

# =========================
# Optional: meta resource (for debugging)
# =========================

if __name__ == "__main__":
    # Run over stdio (default). To run HTTP:
    #   fastmcp run server.py:mcp --transport http --port 8000
    mcp.run()