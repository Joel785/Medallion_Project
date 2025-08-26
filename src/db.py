import psycopg2
import json
from sqlalchemy import create_engine

def get_engine():
    return create_engine("postgresql+psycopg2://postgres:1234@localhost:5432/Medallion_Project")

def get_connection():
    return psycopg2.connect(
        dbname="Medallion_Project",
        user="postgres",
        password="1234",
        host="localhost",
        port="5432"
    )

def reject_row(table, row, reason, conn):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO audit.rejected_rows (table_name, row_data, error_reason)
        VALUES (%s, %s::jsonb, %s)
    """, (table, json.dumps(row), reason))
    conn.commit()
    cursor.close()
