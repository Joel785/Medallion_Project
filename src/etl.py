import os
import logging
from db import get_connection
from silver_transform import (
    transform_patients, transform_doctors,
    transform_appointments, transform_prescriptions, transform_billing
)
from gold_transform import build_gold
from load import run_bronze_load
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
# Orchestration Runner
# -------------------------
if __name__ == "__main__":
    import sys

    # Accept command-line args: silver | gold | all
    task = sys.argv[1] if len(sys.argv) > 1 else "all"

    if task == "bronze":
        build_bronze()
    elif task == "silver":
        build_silver()
    elif task == "gold":
        build_gold_layer()
    elif task == "all":
        build_bronze()  
        build_silver()
        build_gold_layer()
    else:
        print("Usage: python etl.py [silver|gold|all]")
