import duckdb
import os
import threading
import urllib.request
import json
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

# ── Config ─────────────────────────────────────────────────────────────────
DATASET_REPO = "sunsau91/my-fast-telegram-data"
HF_PARQUET_API = f"https://huggingface.co/api/datasets/{DATASET_REPO}/parquet"

def fetch_parquet_files():
    """
    Dataset ke andar actual kitni parquet files hain aur unke exact URLs kya
    hain, ye hardcode karne ke bajaye HF ke official parquet API se runtime
    pe nikalte hain. Shard count/naming badal bhi jaye (jaisa isse pehle
    0000.parquet se badal ke 0.parquet ho gaya tha), ye khud-ba-khud
    current files use karega.
    """
    with urllib.request.urlopen(HF_PARQUET_API, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    urls = []
    for split_files in data.values():       # e.g. {"default": {"train": [...]}}
        for file_list in split_files.values():
            urls.extend(file_list)

    if not urls:
        raise RuntimeError(
            f"HF parquet API se koi file nahi mili: {HF_PARQUET_API}"
        )
    return urls

# ── Thread-local DuckDB connections ────────────────────────────────────────
_local = threading.local()

def get_con():
    if not hasattr(_local, "con"):
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL parquet; LOAD parquet;")
        # Performance settings
        con.execute("SET threads = 4;")
        con.execute("SET memory_limit = '512MB';")

        parquet_files = fetch_parquet_files()
        files = ", ".join(f"'{u}'" for u in parquet_files)
        con.execute(f"""
            CREATE OR REPLACE VIEW tg AS
            SELECT * FROM read_parquet([{files}], union_by_name=true, hive_partitioning=false)
        """)
        _local.con = con
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
    """Verify karne ke liye ki actual mein kaun se parquet files load ho rahe hain."""
    try:
        files = fetch_parquet_files()
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
