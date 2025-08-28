# src/export_to_sheets.py
import os
import io
import re
import time
import hashlib
import logging
from datetime import datetime, date, time as dtime
import numpy as np
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

DB_URL               = os.getenv("DB_URL")
GOLD_SPREADSHEET_ID  = os.getenv("GOLD_SPREADSHEET_ID")
SERVICE_ACCOUNT_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# export tuning
CHUNK_ROWS       = 2000
MAX_RETRIES      = 5
RETRY_BASE_SLEEP = 1.5
RESIZE_STEP_ROWS = 10000
RESIZE_STEP_COLS = 50

ROW_LIMITS = {
    # Uncomment if you want to cap big tables
    # "appointment_utilization_doctor": 30000,
    # "appointment_utilization_patient": 30000,
    # "outstanding_revenue": 20000,
}

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

# ----------------------
# LOGGING
# ----------------------
os.makedirs("logs", exist_ok=True)
logger = logging.getLogger("export_to_sheets")
logger.setLevel(logging.INFO)
logger.handlers.clear()
fh = logging.FileHandler("logs/export_to_sheets.log", encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(fh)


# ----------------------
# HELPERS
# ----------------------
def _retry(fn, *args, **kwargs):
    for i in range(MAX_RETRIES):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            msg = str(e)
            transient = any(code in msg for code in ("429", "500", "503"))
            if transient and i < MAX_RETRIES - 1:
                sleep_s = RETRY_BASE_SLEEP * (2 ** i)
                logger.warning(f"{fn.__name__} retry {i+1}/{MAX_RETRIES} after {sleep_s:.1f}s due to: {e}")
                time.sleep(sleep_s)
                continue
            raise


def _ensure_ws(sh, title: str, rows: int, cols: int):
    try:
        ws = sh.worksheet(title)
        created = False
    except gspread.exceptions.WorksheetNotFound:
        ws = _retry(sh.add_worksheet, title=title, rows=max(rows, 2), cols=max(cols, 2))
        created = True

    try:
        need_rows = rows > ws.row_count
        need_cols = cols > ws.col_count
        if need_rows or need_cols:
            r = ws.row_count
            c = ws.col_count
            target_r = max(rows, 2)
            target_c = max(cols, 2)
            while r < target_r or c < target_c:
                r = min(target_r, r + RESIZE_STEP_ROWS) if r < target_r else r
                c = min(target_c, c + RESIZE_STEP_COLS) if c < target_c else c
                _retry(ws.resize, r, c)
                time.sleep(0.2)
    except Exception as e:
        logger.warning(f"Resize skipped for '{title}': {e}")

    return ws, created


def _clear_ws(ws):
    try:
        _retry(ws.clear)
    except Exception as e:
        logger.warning(f"Clear skipped for '{ws.title}': {e}")


def _to_sheet_friendly(df: pd.DataFrame) -> pd.DataFrame:
    """Convert date/time/etc. into strings/None for Sheets API."""
    out = df.copy()

    for col in out.columns:
        s = out[col]
        if pd.api.types.is_datetime64_any_dtype(s):
            out[col] = s.dt.strftime("%Y-%m-%d %H:%M:%S").where(~s.isna(), None)
        elif pd.api.types.is_timedelta64_dtype(s):
            out[col] = s.astype("string").where(~s.isna(), None)
        elif hasattr(pd.api.types, "is_period_dtype") and pd.api.types.is_period_dtype(s):
            out[col] = s.astype("string").where(~s.isna(), None)

    def _cell(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        if isinstance(v, (pd.Timestamp, datetime)):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(v, date):
            return v.isoformat()
        if isinstance(v, dtime):
            return v.strftime("%H:%M:%S")
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v)
        if isinstance(v, (np.bool_,)):
            return bool(v)
        return v

    for col in out.columns:
        if out[col].dtype == "object":
            out[col] = out[col].map(_cell)

    out = out.where(pd.notnull(out), None)
    return out


def _write_dataframe_chunked(ws, df: pd.DataFrame, chunk_size: int = CHUNK_ROWS):
    df_norm = _to_sheet_friendly(df)

    # header
    header = [list(map(str, df_norm.columns.tolist()))]
    _retry(ws.update, "A1", header)
    time.sleep(0.1)

    n = len(df_norm)
    if n == 0:
        return

    for start in range(0, n, chunk_size):
        end = min(n, start + chunk_size)
        values = df_norm.iloc[start:end].values.tolist()
        a1_row = 2 + start
        a1_range = f"A{a1_row}"
        _retry(ws.update, a1_range, values)
        logger.info(f"[{ws.title}] wrote rows {start+1}–{end}")
        time.sleep(0.2)


# ----------------------
# MAIN EXPORT
# ----------------------
def export_gold_to_sheets():
    if not all([DB_URL, GOLD_SPREADSHEET_ID, SERVICE_ACCOUNT_PATH]):
        raise RuntimeError("Missing DB_URL / GOLD_SPREADSHEET_ID / GOOGLE_APPLICATION_CREDENTIALS")

    engine = create_engine(DB_URL)
    gc = gspread.service_account(filename=SERVICE_ACCOUNT_PATH)
    sh = gc.open_by_key(GOLD_SPREADSHEET_ID)

    logger.info("Gold→Sheets export started")
    with engine.connect() as conn:
        for t in GOLD_TABLES:
            df = pd.read_sql(text(f'SELECT * FROM gold."{t}"'), conn)

            # optional row cap
            limit = ROW_LIMITS.get(t)
            if limit and len(df) > limit:
                logger.warning(f"Table {t} has {len(df)} rows; exporting only first {limit}.")
                df = df.head(limit)

            rows_needed = len(df) + 1
            cols_needed = max(len(df.columns), 1)
            ws, created = _ensure_ws(sh, title=t, rows=rows_needed, cols=cols_needed)

            _clear_ws(ws)
            _write_dataframe_chunked(ws, df, chunk_size=CHUNK_ROWS)

            md5 = pd.util.hash_pandas_object(df.fillna("__NA__"), index=True).sum()
            logger.info(f"Exported {t} | rows={len(df)} | md5_like={md5}")

        # meta tab
        try:
            meta = pd.DataFrame([{
                "last_export_utc": pd.Timestamp.utcnow().isoformat(timespec="seconds"),
                "table_count": len(GOLD_TABLES)
            }])
            ws_meta, _ = _ensure_ws(sh, title="meta_refresh", rows=5, cols=len(meta.columns))
            _clear_ws(ws_meta)
            _write_dataframe_chunked(ws_meta, meta, chunk_size=CHUNK_ROWS)
        except Exception as e:
            logger.warning(f"meta_refresh write skipped: {e}")

    logger.info("Gold→Sheets export finished")