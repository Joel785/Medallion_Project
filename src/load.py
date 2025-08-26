import os
import hashlib
import pandas as pd
from sqlalchemy import create_engine, text


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
BRONZE_INPUT_DIR = os.path.join(PROJECT_ROOT, "bronze_inputs")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)


# DB connection
engine = create_engine("postgresql+psycopg2://postgres:1234@localhost:5432/Medallion_Project")

# File to table mapping
files_tables = {
   "patients.csv": "bronze.patients",
    "doctors.csv": "bronze.doctors",
    "appointments.csv": "bronze.appointments",
    "prescriptions.csv": "bronze.prescriptions",
    "billing.csv": "bronze.billing"
}

def checksum(file_path: str) -> str:
    """Return md5 checksum of a file."""
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

def load_csv_to_db(file_path: str, table_name: str) -> tuple[int, str]:
    """Load a CSV to the given schema.table and return (row_count, checksum)."""
    df = pd.read_csv(file_path)
    row_count = len(df)
    chksum = checksum(file_path)

    schema, table = table_name.split(".", 1)
    df.to_sql(
        table,
        engine,
        schema=schema,
        if_exists="append",   # keep as append; use truncate option before calling if you want a fresh run
        index=False,
    )
    return row_count, chksum

def truncate_bronze_tables():
    """(Optional) Truncate bronze tables to make loads idempotent."""
    with engine.begin() as conn:
        for t in files_tables.values():
            conn.execute(text(f"TRUNCATE {t}"))
    print("Truncated all bronze tables.")

# -------------------------
# Main entry to be used by etl.py
# -------------------------
def run_bronze_load(truncate: bool = False) -> None:
    """
    Loads all CSVs in bronze_inputs into bronze.* tables.
    Set truncate=True to clear bronze tables before loading (idempotent runs).
    """
    if truncate:
        truncate_bronze_tables()

    log_rows = []

    for file_name, table in files_tables.items():
        file_path = os.path.join(BRONZE_INPUT_DIR, file_name)

        if not os.path.exists(file_path):
            # Skip missing files but log the event
            print(f"[WARN] File not found, skipping: {file_path}")
            log_rows.append({
                "table": table,
                "file": file_name,
                "rows": 0,
                "checksum": None,
                "status": "missing_file",
            })
            continue

        rows, chksum = load_csv_to_db(file_path, table)
        print(f"Loaded {rows} rows into {table} from {file_name}, checksum={chksum}")
        log_rows.append({
            "table": table,
            "file": file_name,
            "rows": rows,
            "checksum": chksum,
            "status": "loaded",
        })

    # Save consolidated bronze load log alongside your other logs
    pd.DataFrame(log_rows).to_csv(os.path.join(LOG_DIR, "bronze_load_log.csv"), index=False)
    print("\n Bronze load completed. Logs saved at logs/bronze_load_log.csv")


if __name__ == "__main__":
    run_bronze_load(truncate=False)