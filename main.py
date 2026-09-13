import duckdb
import os
import threading
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

# ── Config ─────────────────────────────────────────────────────────────────
DATASET_REPO = "sunsau91/my-fast-telegram-data"

# Render dashboard me "HF_TOKEN" env var set karo (huggingface.co/settings/tokens
# se free "Read" token bana lo). Bina token ke HF anonymous requests ko bahut
# jaldi 429 (rate limit) de deta hai.
HF_TOKEN = os.environ.get("HF_TOKEN", "")

# ── Shared DuckDB connection + per-thread cursor ───────────────────────────
# Ek hi base connection banate hain (jisme HF secret aur view sirf ek baar
# setup hoti hai), aur har thread ko uska apna cursor(). Isse:
#   - parquet metadata/footer baar baar alag-alag connections se fetch nahi
#     hota (object cache + http metadata cache shared rehta hai)
#   - HF ko lagne wale requests ki total count kaafi kam ho jaati hai
_base_con = None
_base_lock = threading.Lock()
_local = threading.local()

def _init_base_con():
    global _base_con
    if _base_con is not None:
        return _base_con
    with _base_lock:
        if _base_con is not None:
            return _base_con

        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL parquet; LOAD parquet;")

        # Caching: same file ka metadata/data baar baar fetch na ho
        con.execute("PRAGMA enable_object_cache;")
        con.execute("SET enable_http_metadata_cache=true;")
        con.execute("SET threads = 4;")
        con.execute("SET memory_limit = '512MB';")

        if HF_TOKEN:
            con.execute(f"""
                CREATE OR REPLACE SECRET hf_token (
                    TYPE huggingface,
                    TOKEN '{HF_TOKEN}'
                );
            """)

        # DuckDB ka native hf:// scheme khud dataset ke andar actual parquet
        # files resolve karta hai (jaisa naming/shard count ho) — hume koi
        # URL ya API call manually banane/handle karne ki zaroorat nahi.
        con.execute(f"""
            CREATE OR REPLACE VIEW tg AS
            SELECT * FROM read_parquet(
                'hf://datasets/{DATASET_REPO}/**/*.parquet',
                union_by_name=true,
                hive_partitioning=false
            )
        """)
        _base_con = con
        return _base_con

def get_con():
    if not hasattr(_local, "con"):
        base = _init_base_con()
        _local.con = base.cursor()
    return _local.con

# ── FastAPI ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Telegram Data Search API",
    description="Telegram ID se phone aur naam nikalo",
    version="1.0.0"
)

@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Telegram Search API",
        "dataset": "sunsau91/my-fast-telegram-data",
        "records": "244,633,144",
        "endpoints": {
            "search by id": "/search?telegram_id=123456789",
            "search by phone": "/search?phone=380500155123",
            "docs": "/docs"
        }
    }

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/debug/files")
def debug_files():
    """Sirf ye check karta hai ki dataset ke andar kaun se parquet files match ho rahe
    hain (halka glob listing hai, poora data scan nahi karta)."""
    try:
        con = get_con()
        rows = con.execute(f"""
            SELECT file FROM glob('hf://datasets/{DATASET_REPO}/**/*.parquet')
        """).fetchall()
        files = [r[0] for r in rows]
        return {"count": len(files), "files": files}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/search")
def search(
    telegram_id: str = Query(None, description="Telegram User ID"),
    phone: str = Query(None, description="Phone Number"),
    limit: int = Query(10, ge=1, le=50)
):
    if not telegram_id and not phone:
        return JSONResponse(
            status_code=400,
            content={"error": "telegram_id ya phone dono mein se ek zaroori hai"}
        )

    try:
        con = get_con()

        if telegram_id:
            tid = telegram_id.strip().replace("'", "''")
            sql = f"""
                SELECT
                    "Telegram ID"      AS telegram_id,
                    "Номер телефона"   AS phone,
                    "Имя"              AS first_name,
                    "Фамилия"          AS last_name
                FROM tg
                WHERE "Telegram ID" = '{tid}'
                LIMIT {limit}
            """
            query_val = telegram_id
            query_type = "telegram_id"

        else:
            ph = phone.strip().replace("'", "''")
            sql = f"""
                SELECT
                    "Telegram ID"      AS telegram_id,
                    "Номер телефона"   AS phone,
                    "Имя"              AS first_name,
                    "Фамилия"          AS last_name
                FROM tg
                WHERE "Номер телефона" = '{ph}'
                LIMIT {limit}
            """
            query_val = phone
            query_type = "phone"

        rows = con.execute(sql).fetchall()
        cols = ["telegram_id", "phone", "first_name", "last_name"]
        results = []
        for r in rows:
            results.append({
                "telegram_id": r[0],
                "phone":       r[1],
                "first_name":  r[2] or None,
                "last_name":   r[3] or None,
                "full_name":   f"{r[2] or ''} {r[3] or ''}".strip() or None
            })

        return {
            "success": len(results) > 0,
            "query_type": query_type,
            "query": query_val,
            "count": len(results),
            "results": results
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
