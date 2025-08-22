import os
import hashlib
import pandas as pd
from sqlalchemy import create_engine, text

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

# Logs
log_data = []

def checksum(file_path):
    """Return md5 checksum of a file"""
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

def load_csv_to_db(file_path, table_name):
    df = pd.read_csv(file_path)
    row_count = len(df)
    chksum = checksum(file_path)

    # Load to DB (append mode)
    df.to_sql(
        table_name.split(".")[1], 
        engine, 
        schema=table_name.split(".")[0], 
        if_exists="append", 
        index=False
    )

    return row_count, chksum

if __name__ == "__main__":
    for file, table in files_tables.items():
        file_path = os.path.join("bronze_inputs", file)
        row_count, chksum = load_csv_to_db(file_path, table)

        log_entry = {
            "table": table,
            "file": file,
            "rows": row_count,
            "checksum": chksum
        }
        log_data.append(log_entry)

        # Print log immediately
        print(f"Loaded {row_count} rows into {table} from {file}, checksum={chksum}")

    # Save logs
    log_df = pd.DataFrame(log_data)
    os.makedirs("logs", exist_ok=True)  # make sure logs dir exists
    log_df.to_csv("logs/bronze_load_log.csv", index=False)

    print("\n Bronze load completed. Logs saved in logs/bronze_load_log.csv")
