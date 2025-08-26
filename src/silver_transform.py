import pandas as pd
import logging
from db import get_engine, reject_row


# -------------------------
# Utility: Safe integer conversion
# -------------------------
def safe_int(value):
    """Convert strings like '000123', '46601.0' to int. Return None if invalid."""
    try:
        cleaned = str(value).strip()
        return int(float(cleaned))
    except Exception:
        return None


# -------------------------
# Patients Transformation
# -------------------------
def transform_patients(conn):
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM bronze.patients", engine)

    valid_rows, rejected = [], []

    for _, row in df.iterrows():
        try:
            patient_id = int(row["patient_id"])
            dob = pd.to_datetime(row["dob"], errors="coerce")
            if pd.isna(dob) or dob > pd.Timestamp.today():
                raise ValueError("Invalid DOB")

            gender = str(row["gender"]).strip().lower()
            if gender in ["m", "male"]:
                gender = "M"
            elif gender in ["f", "female"]:
                gender = "F"
            else:
                gender = "Other"

            valid_rows.append((
                patient_id,
                row["name"],
                gender,
                dob.date(),
                row["city"],
                row["contact_no"]
            ))
        except Exception as e:
            rejected.append((row.to_dict(), str(e)))

    if valid_rows:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO silver.patients (patient_id, name, gender, dob, city, contact_no)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (patient_id) DO NOTHING
        """, valid_rows)
        conn.commit()
        cursor.close()

    for row, reason in rejected:
        reject_row("patients", row, reason, conn)

    logging.info(f"patients | {len(df)} rows checked | {len(valid_rows)} loaded | {len(rejected)} rejected")

# -------------------------
# Doctors Transformation
# -------------------------
def transform_doctors(conn):
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM bronze.doctors", engine)

    valid_rows, rejected = [], []

    for _, row in df.iterrows():
        try:
            doctor_id = int(row["doctor_id"])
            years_experience = int(row["years_experience"])
            if years_experience < 0:
                raise ValueError("Negative experience not allowed")

            valid_rows.append((
                doctor_id,
                row["name"],
                row["specialization"]
            ))
        except Exception as e:
            rejected.append((row.to_dict(), str(e)))

    if valid_rows:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO silver.doctors (doctor_id, name, specialization)
            VALUES (%s, %s, %s)
            ON CONFLICT (doctor_id) DO NOTHING
        """, valid_rows)
        conn.commit()
        cursor.close()

    for row, reason in rejected:
        reject_row("doctors", row, reason, conn)

    logging.info(f"doctors | {len(df)} rows checked | {len(valid_rows)} loaded | {len(rejected)} rejected")

# -------------------------
# Appointments Transformation
# -------------------------
def transform_appointments(conn):
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM bronze.appointments", engine)

    valid_rows, rejected = [], []

    # FK checks: valid patients & doctors
    cursor = conn.cursor()
    cursor.execute("SELECT patient_id FROM silver.patients")
    valid_patients = {row[0] for row in cursor.fetchall()}

    cursor.execute("SELECT doctor_id FROM silver.doctors")
    valid_doctors = {row[0] for row in cursor.fetchall()}
    cursor.close()

    for _, row in df.iterrows():
        try:
            appointment_id = int(row["appointment_id"])
            patient_id = int(row["patient_id"])
            doctor_id = int(row["doctor_id"])
            appointment_date = pd.to_datetime(row["appointment_date"], errors="coerce")
            if pd.isna(appointment_date):
                raise ValueError("Invalid appointment_date")

            if patient_id not in valid_patients:
                raise ValueError(f"Patient {patient_id} not found in silver.patients")
            if doctor_id not in valid_doctors:
                raise ValueError(f"Doctor {doctor_id} not found in silver.doctors")

            status = str(row["status"]).capitalize()
            if status not in ["Scheduled", "Completed", "Cancelled"]:
                raise ValueError("Invalid status")

            valid_rows.append((appointment_id, patient_id, doctor_id, appointment_date.date(), status))
        except Exception as e:
            rejected.append((row.to_dict(), str(e)))

    if valid_rows:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO silver.appointments (appointment_id, patient_id, doctor_id, appointment_date, status)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (appointment_id) DO NOTHING
        """, valid_rows)
        conn.commit()
        cursor.close()

    for row, reason in rejected:
        reject_row("appointments", row, reason, conn)

    logging.info(f"appointments | {len(df)} rows checked | {len(valid_rows)} loaded | {len(rejected)} rejected")

def safe_int(value):
    """Convert strings like '000123', '46601.0' to int. Return None if invalid."""
    try:
        cleaned = str(value).strip()
        return int(float(cleaned))
    except Exception:
        return None


# -------------------------
# Prescriptions Transformation
# -------------------------
def transform_prescriptions(conn):
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM bronze.prescriptions", engine)

    # Load valid appointment IDs from silver
    valid_appointments = pd.read_sql("SELECT appointment_id FROM silver.appointments", engine)
    valid_appointments_set = set(valid_appointments["appointment_id"].astype(int))

    valid_rows, rejected = [], []

    for _, row in df.iterrows():
        try:
            prescription_id = safe_int(row["prescription_id"])
            appointment_id = safe_int(row["appointment_id"])
            medicine = str(row["medicine"]).strip() if pd.notna(row["medicine"]) else None
            dosage = row["dosage"]
            duration_days = safe_int(row["duration_days"])

            if prescription_id is None:
                raise ValueError(f"Invalid prescription_id: {row['prescription_id']}")

            if appointment_id is None or appointment_id not in valid_appointments_set:
                raise ValueError(f"Invalid or missing appointment_id: {row['appointment_id']}")

            if not medicine:
                raise ValueError("Medicine cannot be NULL or empty")

            valid_rows.append((
                prescription_id,
                appointment_id,
                medicine,
                dosage,
                duration_days
            ))
        except Exception as e:
            rejected.append((row.to_dict(), str(e)))

    if valid_rows:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO silver.prescriptions (prescription_id, appointment_id, medicine, dosage, duration_days)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (prescription_id) DO NOTHING
        """, valid_rows)
        conn.commit()
        cursor.close()

    for row, reason in rejected:
        reject_row("prescriptions", row, reason, conn)

    logging.info(f"prescriptions | {len(df)} rows checked | {len(valid_rows)} loaded | {len(rejected)} rejected")

# -------------------------
# Billing Transformation
# -------------------------
def transform_billing(conn):
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM bronze.billing", engine)

    valid_rows, rejected = [], []

    # FK checks: valid patients & appointments
    cursor = conn.cursor()
    cursor.execute("SELECT patient_id FROM silver.patients")
    valid_patients = {row[0] for row in cursor.fetchall()}

    cursor.execute("SELECT appointment_id FROM silver.appointments")
    valid_appointments = {row[0] for row in cursor.fetchall()}
    cursor.close()

    for _, row in df.iterrows():
        try:
            bill_id = int(row["bill_id"])
            patient_id = int(row["patient_id"])
            appointment_id = int(row["appointment_id"])
            amount = float(row["amount"])
            if amount < 0:
                raise ValueError("Negative billing amount")

            if patient_id not in valid_patients:
                raise ValueError(f"Patient {patient_id} not found in silver.patients")
            if appointment_id not in valid_appointments:
                raise ValueError(f"Appointment {appointment_id} not found in silver.appointments")

            payment_status = str(row["payment_status"]).capitalize()
            if payment_status not in ["Paid", "Pending"]:
                raise ValueError("Invalid payment_status")

            valid_rows.append((
                bill_id,
                patient_id,
                appointment_id,
                amount,
                payment_status,
                row["payment_method"]
            ))
        except Exception as e:
            rejected.append((row.to_dict(), str(e)))

    if valid_rows:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO silver.billing (bill_id, patient_id, appointment_id, amount, payment_status, payment_method)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (bill_id) DO NOTHING
        """, valid_rows)
        conn.commit()
        cursor.close()

    for row, reason in rejected:
        reject_row("billing", row, reason, conn)

    logging.info(f"billing | {len(df)} rows checked | {len(valid_rows)} loaded | {len(rejected)} rejected")
