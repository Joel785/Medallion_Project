import os
import logging
from db import get_connection
from silver_transform import (
    transform_patients, transform_doctors,
    transform_appointments, transform_prescriptions, transform_billing
)
from gold_transform import build_gold
from load import run_bronze_load
from export_to_sheets import export_gold_to_sheets
from extract_from_csv import export_tabs_to_bronze_inputs

from dotenv import load_dotenv
load_dotenv()

import time 
def _run_step(name, fn):
    start = time.time()
    logging.info(f"=== {name} Started ===")
    try:
        fn()
        logging.info(f"=== {name} Completed in {time.time() - start:.2f}s ===")
    except Exception as e:
        logging.error(f"*** {name} FAILED after {time.time() - start:.2f}s: {e}")
        raise

# -------------------------
# Logging setup
# -------------------------
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/dq_checks.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# -------------------------
# Extract From Csv
# -------------------------


def extract():
    try:
        logging.info("=== Extract from Google Sheets Started ===")
        stats = export_tabs_to_bronze_inputs()
        # optional compact summary
        logging.info("Extract summary: " + ", ".join([f"{t}:{r}" for t, r, _, _ in stats]))
        logging.info("=== Extract from Google Sheets Completed ===")
    except Exception as e:
        logging.error(f"Error in extract: {e}")
        raise



# -------------------------
# Bronze Builder
# -------------------------
def build_bronze():
    logging.info("=== Building Bronze Layer Started ===")
    run_bronze_load(truncate=True)   # set to True if you want clean bronze on every run
    logging.info("=== Building Bronze Layer Completed ===")


# -------------------------
# Silver Builder
# -------------------------
def build_silver():
    conn = get_connection()
    try:
        logging.info("=== Building Silver Layer Started ===")
        transform_patients(conn)
        transform_doctors(conn)
        transform_appointments(conn)
        transform_prescriptions(conn)
        transform_billing(conn)
        logging.info("=== Building Silver Layer Completed ===")
    except Exception as e:
        logging.error(f"Error in build_silver: {str(e)}")
        raise
    finally:
        conn.close()

# -------------------------
# Gold Builder
# -------------------------
def build_gold_layer():
    conn = get_connection()
    try:
        logging.info("=== Building Gold Layer Started ===")
        build_gold(conn)
        logging.info("=== Building Gold Layer Completed ===")
    except Exception as e:
        logging.error(f"Error in build_gold: {str(e)}")
        raise
    finally:
        conn.close()

# -------------------------
# Gold Builder
# -------------------------

def export_sheets():
    try:
        logging.info("=== Export to Google Sheets Started ===")
        export_gold_to_sheets()
        logging.info("=== Export to Google Sheets Completed ===")
    except Exception as e:
        logging.error(f"Error in export_sheets: {str(e)}")
        raise

# -------------------------
# Orchestration Runner
# -------------------------
if __name__ == "__main__":
    import sys

    # Accept command-line args: extract | bronze | silver | gold | export_sheets | all
    task = sys.argv[1] if len(sys.argv) > 1 else "all"

    if task == "extract":
        _run_step("Extract (Sheets → bronze_inputs CSVs)", extract)
    elif task == "bronze":
        _run_step("Bronze Layer", build_bronze)
    elif task == "silver":
        _run_step("Silver Layer", build_silver)
    elif task == "gold":
        _run_step("Gold Layer", build_gold_layer)
    elif task == "export_sheets":
        _run_step("Export to Google Sheets", export_sheets)
    elif task == "all":
        _run_step("Extract (Sheets → bronze_inputs CSVs)", extract)
        _run_step("Bronze Layer", build_bronze)
        _run_step("Silver Layer", build_silver)
        _run_step("Gold Layer", build_gold_layer)
        _run_step("Export to Google Sheets", export_sheets)
    else:
        print("Usage: python etl.py [extract|bronze|silver|gold|export_sheets|all]")
