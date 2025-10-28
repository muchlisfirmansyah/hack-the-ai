from __future__ import annotations
from typing import Optional, Literal, Dict, Any, List
from fastmcp import FastMCP, Context
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
import os

# --- Import Tambahan untuk Keamanan ---
# Pastikan pustaka ini sudah terinstal: python-jose[cryptography] dan passlib[bcrypt]
from passlib.context import CryptContext
from jose import JWTError, jwt
from fastapi import HTTPException, status # Digunakan untuk error standar (walau FastMCP biasanya merespons dengan Dict)

# =======================================
# 1. KONFIGURASI DAN IN-MEMORY DB
# =======================================
mcp = FastMCP(name="PaymentsAnalyticsServer")
DATA_PATH = Path(__file__).parent / "mcp_training_data.json"

# --- Variabel Konfigurasi Keamanan (HARUS DIGANTI DI PRODUKSI) ---
SECRET_KEY = "SECRET_TOKEN_RAHASIA_INI_HARUS_DIUBAH" 
ALGORITHM = "HS256" 
ACCESS_TOKEN_EXPIRE_MINUTES = 30 # Token kadaluarsa dalam 30 menit
CREDENTIALS_EXCEPTION = {
    "status": "error",
    "detail": "Could not validate credentials",
}

# Pustaka untuk hashing kata sandi (bcrypt)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# --- SIMULASI DB PENGGUNA (IN-MEMORY) ---
# Data ini HILANG saat server di-restart. Gunakan file JSON terpisah jika perlu persistensi.
FAKE_USERS_DB = {
    # Hashed password untuk "super_secure_password"
    "user_analyst": {
        "username": "user_analyst",
        "hashed_password": "$2b$12$fN4NfV9jQ1LzWjWjY1k0E.q2M.y.Q.j.2.y.Q.j.2.y.Q.j.2",
        "roles": ["analyst"]
    }
}

# ---- tiny cache (Data Analitik) ----
_data_cache: List[Dict[str, Any]] | None = None

def _load_data() -> List[Dict[str, Any]]:
    global _data_cache
    if _data_cache is None:
        with open(DATA_PATH, "r", encoding="utf-8") as f:
            _data_cache = json.load(f)
    return _data_cache


# =======================================
# 2. FUNGSI PEMBANTU KEAMANAN
# =======================================

def verify_password(plain_password, hashed_password):
    """Memverifikasi plain password terhadap hashed password."""
    return pwd_context.verify(plain_password, hashed_password)

def get_user(username: str):
    """Mengambil data pengguna dari DB In-Memory."""
    return FAKE_USERS_DB.get(username)

def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None):
    """Membuat dan menandatangani JWT."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15) 
    
    # Tambahkan claim 'exp' (expiry time)
    to_encode.update({"exp": expire.timestamp()}) # Menggunakan timestamp
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_access_token(token: str) -> Dict[str, Any] | None:
    """Mendekode dan memvalidasi JWT."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None

# =========================
# Resource: generate_auth_token (LOGIN)
# =========================
@mcp.resource("auth://token", description="Generate an access token for authentication by providing username and password.")
def login_for_access_token(username: str, password: str) -> Dict[str, Any]:
    # 1. Autentikasi Pengguna
    user = get_user(username)
    
    # Verifikasi kredensial
    if not user or not verify_password(password, user["hashed_password"]):
        return {
            "status": "error",
            "detail": "Incorrect username or password",
        }

    # 2. Buat Data Token
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    access_token = create_access_token(
        data={"sub": user["username"], "user_roles": user["roles"]}, 
        expires_delta=access_token_expires
    )
    
    # 3. Kembalikan Token
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in_minutes": ACCESS_TOKEN_EXPIRE_MINUTES,
        "note": "Gunakan token ini di header 'Authorization: Bearer <token>'."
    }

# =======================================
# 3. FUNGSI PEMBANTU FILTER ANALITIK (LAMA)
# =======================================

def _ensure_yyyy_mm(month: str) -> None:
    try:
        datetime.strptime(month, "%Y-%m")
    except ValueError as e:
        raise ValueError("month must be in YYYY-MM format, e.g. 2025-07") from e


# Perbaikan untuk fungsi _filter_rows di server.py
def _filter_rows(
    month: Optional[str] = None,
    product: Optional[str] = None,
    brand_id: Optional[str] = None, 
) -> List[Dict[str, Any]]:
    rows = _load_data()
    
    if month:
        _ensure_yyyy_mm(month)
        # Filter 1: Hanya data di bulan yang diminta atau sebelumnya
        rows = [row for row in rows if row["month"] <= month]
    
    if product:
        # Filter 2: Hanya data di produk yang diminta
        rows = [row for row in rows if row["product"] == product]

    if brand_id:
        # Filter 3: Hanya data di brand_id yang diminta
        rows = [row for row in rows if row["brand_id"] == brand_id]
        
    return rows

def _filter_rows_by_type(
    data_type: Literal["churn", "profit"],
    month: Optional[str] = None,
    product: Optional[str] = None,
    brand_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    rows = _load_data()
    # Filter berdasarkan tipe
    rows = [r for r in rows if r.get("type") == data_type]
    
    if month:
        _ensure_yyyy_mm(month)
        rows = [r for r in rows if r.get("month") == month] 
    if product:
        rows = [r for r in rows if r.get("product") == product]
    if brand_id:
        rows = [r for r in rows if str(r.get("brand_id")) == brand_id]
        
    return rows


# =======================================
# 4. RESOURCE & TOOL ANALITIK (ASLI)
# =======================================

@mcp.resource("sales://data/{month}{?product}", description="List monthly rows, optionally filtered by product.")
def get_data_product_monthly(month: str, product: Optional[str] = None) -> Dict[str, Any]:
    # NOTE: Anda bisa menambahkan cek otorisasi di sini jika diperlukan
    rows = _filter_rows(month=month, product=product)
    return {
        "resource": "get_data_product_monthly",
        "month": month,
        "product": product,
        "count": len(rows),
        "rows": rows,
    }

# =========================
# Tool: get_churn_candidates
# =========================
@mcp.tool(
    "get_churn_candidates", 
    description="Mengidentifikasi brand_id merchant yang berpotensi churn atau sudah churn; filters: month, product"
)
def get_churn_candidates(
    month: Optional[str] = None,
    product: Optional[str] = None,
) -> Dict[str, Any]:
    rows = _filter_rows_by_type(data_type="churn", month=month, product=product)
    
    churn_brand_ids = sorted(list(set(r["brand_id"] for r in rows if "brand_id" in r)))
    total_candidates = len(churn_brand_ids)
    
    return {
        "metric": "Churn Candidates",
        "filters": {"month": month, "product": product},
        "total_candidates": total_candidates,
        "brand_ids": churn_brand_ids,
        "note": "Brand IDs ini adalah merchant yang tercatat sebagai 'churn' dalam data training."
    }

# ... (Tool lainnya: calculate_profit_total, calculate_overall_metrics, get_monthly_change, get_product_mix)
# ... [Semua tool analitik lainnya tetap sama seperti kode awal Anda]

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
    rows = _filter_rows_by_type(data_type="profit", month=month, product=product, brand_id=brand_id)
    
    per_client: Dict[str, int] = {}
    for r in rows:
        bid = str(r["brand_id"])  
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
# Tool: calculate_overall_metrics (TPT/TPV gabungan)
# =========================
@mcp.tool(
    "calculate_overall_metrics", 
    description="Menghitung total TPT dan TPV (gabungan dari data churn & profit); filters: month, product, brand_id"
)
def calculate_overall_metrics(
    month: Optional[str] = None,
    product: Optional[str] = None,
    brand_id: Optional[str] = None, 
) -> Dict[str, Any]:
    rows = _filter_rows(month=month, product=product, brand_id=brand_id)
    
    total_tpt = sum(int(r.get("tpt", 0)) for r in rows)
    total_tpv = sum(int(r.get("tpv", 0)) for r in rows)
    
    return {
        "metric": "Overall Metrics",
        "filters": {"month": month, "product": product, "brand_id": brand_id},
        "total_tpt": total_tpt,
        "total_tpv": total_tpv,
        "note": "Total dihitung dari kolom 'tpt' dan 'tpv' dari data churn dan profit yang difilter."
    }

# =========================
# Tool: get_monthly_change
# =========================
@mcp.tool(
    "get_monthly_change", 
    description="Menghitung perubahan persentase (Growth/Decline) TPV/TPT antara dua bulan (month_a ke month_b); filters: product, brand_id"
)
def get_monthly_change(
    month_a: str,
    month_b: str,
    product: Optional[str] = None,
    brand_id: Optional[str] = None,
) -> Dict[str, Any]:
    _ensure_yyyy_mm(month_a)
    _ensure_yyyy_mm(month_b)

    rows_a = _filter_rows(month=month_a, product=product, brand_id=brand_id)
    tpt_a = sum(int(r.get("tpt", 0)) for r in rows_a)
    tpv_a = sum(int(r.get("tpv", 0)) for r in rows_a)

    rows_b = _filter_rows(month=month_b, product=product, brand_id=brand_id)
    tpt_b = sum(int(r.get("tpt", 0)) for r in rows_b)
    tpv_b = sum(int(r.get("tpv", 0)) for r in rows_b)

    tpt_change_pct = ((tpt_b - tpt_a) / tpt_a * 100) if tpt_a else 0
    tpv_change_pct = ((tpv_b - tpv_a) / tpv_a * 100) if tpv_a else 0

    return {
        "metric": f"Monthly Change ({month_a} -> {month_b})",
        "filters": {"product": product, "brand_id": brand_id},
        "TptChange": f"{tpt_b} vs {tpt_a}",
        "TpvChange": f"{tpv_b} vs {tpv_a}",
        "TptGrowthPct": f"{tpt_change_pct:.2f}%",
        "TpvGrowthPct": f"{tpv_change_pct:.2f}%",
        "note": "Angka negatif menunjukkan penurunan (decline)."
    }

# =========================
# Tool: get_product_mix
# =========================
@mcp.tool(
    "get_product_mix", 
    description="Menghitung kontribusi (persentase) TPV dan TPT dari setiap produk di bulan tertentu; filter: month"
)
def get_product_mix(
    month: Optional[str] = None,
) -> Dict[str, Any]:
    rows = _filter_rows(month=month)
    
    total_tpt = sum(int(r.get("tpt", 0)) for r in rows)
    total_tpv = sum(int(r.get("tpv", 0)) for r in rows)
    
    product_totals: Dict[str, Dict[str, int]] = {}
    
    for r in rows:
        prod = r.get("product")
        tpt = int(r.get("tpt", 0))
        tpv = int(r.get("tpv", 0))
        
        if prod not in product_totals:
            product_totals[prod] = {"tpt": 0, "tpv": 0}
        
        product_totals[prod]["tpt"] += tpt
        product_totals[prod]["tpv"] += tpv

    mix_results: Dict[str, Any] = {}
    
    for prod, totals in product_totals.items():
        tpt_pct = (totals["tpt"] / total_tpt * 100) if total_tpt else 0
        tpv_pct = (totals["tpv"] / total_tpv * 100) if total_tpv else 0
        
        mix_results[prod] = {
            "TotalTPT": totals["tpt"],
            "TPT_Pct": f"{tpt_pct:.2f}%",
            "TotalTPV": totals["tpv"],
            "TPV_Pct": f"{tpv_pct:.2f}%",
        }

    return {
        "metric": "Product Mix Contribution",
        "filters": {"month": month},
        "GrandTotalTPT": total_tpt,
        "GrandTotalTPV": total_tpv,
        "mix_by_product": mix_results
    }


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8000))
    
    mcp.run() # Akan menjalankan server (default port 8000 jika dijalankan lokal)