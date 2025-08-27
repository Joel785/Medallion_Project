# src/extract_from_sheets.py
import os, glob, hashlib, logging, time
import pandas as pd
import gspread
from gspread.exceptions import WorksheetNotFound, APIError
from gspread_dataframe import get_as_dataframe
from dotenv import load_dotenv

# -------------------------
# Env & paths
# -------------------------
load_dotenv()

SOURCE_SPREADSHEET_ID = os.getenv("SOURCE_SPREADSHEET_ID")
SERVICE_ACCOUNT_PATH  = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BRONZE_DIR            = os.getenv("BRONZE_DIR", "./bronze_inputs")

if not SOURCE_SPREADSHEET_ID:
    raise RuntimeError("SOURCE_SPREADSHEET_ID not set in .env")
if not SERVICE_ACCOUNT_PATH or not os.path.exists(SERVICE_ACCOUNT_PATH):
    raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS path invalid/missing")

os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)

# Tabs → CSV filenames (must match your tab titles exactly)
TAB_ORDER = ["patients", "doctors", "appointments", "prescriptions", "billing"]
TAB_TO_CSV = {t: f"{t}.csv" for t in TAB_ORDER}

# -------------------------
# File-only logging (no console output)
# -------------------------
logger = logging.getLogger("extract_from_sheets")
logger.setLevel(logging.INFO)
logger.handlers.clear()

fh = logging.FileHandler("logs/extract_from_sheets.log")
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(fh)

# -------------------------
# Helpers
# -------------------------
def _md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()

def _gspread_client():
    return gspread.service_account(filename=SERVICE_ACCOUNT_PATH)

def _retry(call, *args, **kwargs):
    """Retry wrapper to handle Google Sheets 429 rate limits."""
    for i in range(5):
        try:
            return call(*args, **kwargs)
        except APIError as e:
            if "[429]" in str(e) and i < 4:
                wait = 1.5 * (2 ** i)
                logger.warning(f"Rate limit hit, retrying in {wait:.1f}s...")
                time.sleep(wait)
                continue
            raise

def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Drop empty rows/cols, normalize headers."""
    df = df.dropna(how="all").dropna(axis=1, how="all")
    df.columns = [
        (str(c).strip() if str(c) not in ("", "nan", "None") else f"col_{i}")
        for i, c in enumerate(df.columns)
    ]
    return df

# -------------------------
# Main Extractor
# -------------------------
def export_tabs_to_bronze_inputs():
    logger.info("Sheets → CSV export started")

    # 1) Remove old CSVs (keeps folder tidy & idempotent)
    for old in glob.glob(os.path.join(BRONZE_DIR, "*.csv")):
        try:
            os.remove(old)
            logger.info(f"Removed old CSV: {old}")
        except Exception as e:
            logger.warning(f"Could not remove {old}: {e}")

    # 2) Connect to Sheets
    gc = _gspread_client()
    sh = _retry(gc.open_by_key, SOURCE_SPREADSHEET_ID)

    # 3) Export each tab to CSV
    stats = []
    for tab in TAB_ORDER:
        try:
            ws = _retry(sh.worksheet, tab)
        except WorksheetNotFound:
            raise RuntimeError(f"Tab not found in spreadsheet: '{tab}'")

        df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
        df = _clean_df(df)

        path = os.path.join(BRONZE_DIR, TAB_TO_CSV[tab])
        df.to_csv(path, index=False)

        with open(path, "rb") as f:
            md5 = _md5(f.read())

        stats.append((tab, len(df), md5, path))
        logger.info(f"Wrote {path} | rows={len(df)} | md5={md5}")

        time.sleep(0.5)  # gentle throttle

    logger.info("Sheets → CSV export completed")
    return stats

# -------------------------
# Standalone run
# -------------------------
if __name__ == "__main__":
    export_tabs_to_bronze_inputs()
