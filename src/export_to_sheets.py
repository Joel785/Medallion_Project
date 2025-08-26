# src/export_to_sheets.py
import os
import io
import re
import time
import hashlib
import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from gspread_dataframe import set_with_dataframe

# -------------------------------------------------
# Env + logging
# -------------------------------------------------
load_dotenv()

DB_URL = os.getenv("DB_URL")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

LOGS_DIR = os.getenv("LOGS_DIR", "./logs")
os.makedirs(LOGS_DIR, exist_ok=True)

# If your main ETL already configures logging, you can remove this.
logging.basicConfig(
    filename=os.path.join(LOGS_DIR, "export_to_sheets.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# -------------------------------------------------
# Tunables for rate limits
# -------------------------------------------------
RATE_LIMIT_SLEEP_PER_TABLE = float(os.getenv("GSHEETS_SLEEP_PER_TABLE", "1.0"))  # seconds between tables
MAX_RETRIES = int(os.getenv("GSHEETS_MAX_RETRIES", "5"))
BASE_SLEEP = float(os.getenv("GSHEETS_BASE_SLEEP", "1.0"))  # base backoff (1, 2, 4, 8, 16...)

# -------------------------------------------------
# Gold tables (stable order & names => stable tabs)
# -------------------------------------------------
GOLD_TABLES = [
    "revenue_by_department",
    "revenue_by_payment_method",
    "total_revenue",
    "revenue_monthly",
    "appointment_utilization_doctor",
    "appointment_utilization_patient",
    "doctor_performance",
    "patient_insights",
    "medicine_utilization",
    "outstanding_revenue",
    "appointments_summary",
    "total_patients",
    "dashboard_summary",
]

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def _engine():
    if not DB_URL:
        raise RuntimeError("DB_URL not set in environment/.env")
    return create_engine(DB_URL)

def _gspread():
    if not SERVICE_ACCOUNT_PATH or not os.path.exists(SERVICE_ACCOUNT_PATH):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS path invalid or missing")
    return gspread.service_account(filename=SERVICE_ACCOUNT_PATH)

def _md5_df(df: pd.DataFrame) -> str:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return hashlib.md5(buf.getvalue()).hexdigest()

def _is_429(e: Exception) -> bool:
    # gspread APIError string contains "[429]" for rate-limit
    return isinstance(e, APIError) and re.search(r"\[429\]", str(e)) is not None

def _retry(callable_, *args, **kwargs):
    """Retry wrapper for gspread calls with exponential backoff on 429."""
    attempt = 0
    while True:
        try:
            return callable_(*args, **kwargs)
        except Exception as e:
            if _is_429(e) and attempt < MAX_RETRIES:
                sleep_s = BASE_SLEEP * (2 ** attempt)
                logging.warning(f"429 rate limit hit; retrying in {sleep_s:.1f}s (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(sleep_s)
                attempt += 1
                continue
            raise

def _ensure_ws(sh, title: str, rows: int, cols: int):
    """
    Get or create worksheet by title; resize & clear.
    Returns (worksheet, created_bool).
    """
    try:
        ws = _retry(sh.worksheet, title)  # read op
        # Writes: resize + clear
        _retry(ws.resize, max(rows, 2), max(cols, 2))
        _retry(ws.clear)
        return ws, False
    except WorksheetNotFound:
        ws = _retry(sh.add_worksheet, title=title, rows=max(rows, 2), cols=max(cols, 2))
        return ws, True

def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Light coercions so Looker Studio infers types well.
    Adjust/extend rules per your schemas if needed.
    """
    if df.empty:
        return df

    # Numeric-ish columns by suffix/keywords
    for col in df.columns:
        low = col.lower()
        if low.endswith(("_amount", "_revenue", "_count", "_total")) or low in {"revenue", "amount", "count", "total"}:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Dates: cast to ISO string YYYY-MM-DD (or keep as datetime and let LS infer)
        if "date" in low or low.endswith(("_dt", "_at")):
            try:
                # If monthly aggregations like '2025-08' exist, keep as-is
                parsed = pd.to_datetime(df[col], errors="coerce")
                # If it's all date-like, convert to date string
                if parsed.notna().sum() > 0:
                    df[col] = parsed.dt.strftime("%Y-%m-%d")
            except Exception:
                pass

    return df

# -------------------------------------------------
# Main export
# -------------------------------------------------
def export_gold_to_sheets():
    logging.info("Gold→Sheets export started")
    engine = _engine()
    gc = _gspread()
    sh = gc.open_by_key(SPREADSHEET_ID)

    run_stats = []

    with engine.connect() as conn:
        for t in GOLD_TABLES:
            # Read gold.<table> (Postgres schema-qualified)
            query = text(f'SELECT * FROM gold."{t}"')
            df = pd.read_sql_query(query, conn)
            df = _coerce_types(df)

            # Ensure a tab exists, clear it, and size it
            ws, created = _ensure_ws(sh, title=t, rows=len(df) + 1, cols=max(len(df.columns), 1))

            # Single data write (sends values in one call)
            _retry(
                set_with_dataframe,
                ws,
                df,
                include_index=False,
                include_column_header=True,
                resize=False,
            )

            # Only do formatting writes once (on first creation)
            if created:
                try:
                    _retry(ws.freeze, rows=1)
                except Exception:
                    pass
                try:
                    _retry(ws.set_basic_filter)
                except Exception:
                    pass

            md5 = _md5_df(df)
            run_stats.append((t, len(df), md5))
            logging.info(f"Exported {t} | rows={len(df)} | md5={md5}")

            # Throttle between tables to avoid per-minute write caps
            time.sleep(RATE_LIMIT_SLEEP_PER_TABLE)

    # Meta tab with refresh info
    meta_rows = [
        {"key": "last_run_utc", "value": datetime.now(timezone.utc).isoformat()},
        {"key": "tables_exported", "value": len(run_stats)},
    ]
    meta_rows += [{"key": f"rows.{t}", "value": r} for (t, r, _) in run_stats]
    meta_rows += [{"key": f"md5.{t}", "value": m} for (t, _, m) in run_stats]
    meta = pd.DataFrame(meta_rows)

    ws_meta, created_meta = _ensure_ws(sh, "meta_refresh", rows=len(meta) + 1, cols=2)
    _retry(
        set_with_dataframe,
        ws_meta,
        meta,
        include_index=False,
        include_column_header=True,
        resize=False,
    )
    if created_meta:
        try:
            _retry(ws_meta.freeze, rows=1)
        except Exception:
            pass
        try:
            _retry(ws_meta.set_basic_filter)
        except Exception:
            pass

    logging.info("Gold→Sheets export finished")


if __name__ == "__main__":
    export_gold_to_sheets()
