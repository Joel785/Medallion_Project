CREATE SCHEMA bronze;

CREATE TABLE bronze.patients (
    patient_id TEXT,
    name TEXT,
    gender TEXT,
    dob TEXT,
    city TEXT,
    contact_no TEXT
);

CREATE TABLE bronze.doctors (
    doctor_id TEXT,
    name TEXT,
    specialization TEXT,
    years_experience TEXT,
    department TEXT,
    availability_status TEXT
);

CREATE TABLE bronze.appointments (
    appointment_id TEXT,
    patient_id TEXT,
    doctor_id TEXT,
    appointment_date TEXT,
    status TEXT
);

CREATE TABLE bronze.prescriptions (
    prescription_id TEXT,
    appointment_id TEXT,
    medicine TEXT,
    dosage TEXT,
    duration_days TEXT
);

CREATE TABLE bronze.billing (
    bill_id TEXT,
    patient_id TEXT,
    appointment_id TEXT,
    amount TEXT,
    payment_status TEXT,
    payment_method TEXT
);
