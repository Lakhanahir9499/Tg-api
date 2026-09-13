import duckdb
import os
import threading
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

# ── Config ─────────────────────────────────────────────────────────────────
HF_BASE = "https://huggingface.co/datasets/sunsau91/my-fast-telegram-data/resolve/refs%2Fconvert%2Fparquet/default/train"

# Dataset mein kitne parquet files hain check karne ke liye:
# curl https://huggingface.co/api/datasets/sunsau91/my-fast-telegram-data/parquet
# Filhaal 0000 se 0019 tak try kar rahe hain (adjust karna ho sakta hai)
PARQUET_FILES = [
    f"{HF_BASE}/{str(i).zfill(4)}.parquet" for i in range(20)
]

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
        files = ", ".join(f"'{u}'" for u in PARQUET_FILES)
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
