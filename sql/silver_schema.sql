CREATE SCHEMA IF NOT EXISTS silver;

-- 2. Patients
CREATE TABLE IF NOT EXISTS silver.patients (
    patient_id      INT PRIMARY KEY,                                   
    name            VARCHAR(100) NOT NULL,
    gender          VARCHAR(10) NOT NULL CHECK (gender IN ('M','F','Other')),
    dob             DATE NOT NULL CHECK (dob <= CURRENT_DATE),
    city            VARCHAR(100),
    contact_no      VARCHAR(15)
);

-- 3. Doctors
CREATE TABLE IF NOT EXISTS silver.doctors (
    doctor_id       INT PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    specialization  VARCHAR(100) NOT NULL,
    years_experience INT CHECK (years_experience >= 0),
    department      VARCHAR(100),
    availability_status VARCHAR(20) CHECK (availability_status IN ('Available','Unavailable'))
);

-- 4. Appointments
CREATE TABLE IF NOT EXISTS silver.appointments (
    appointment_id   INT PRIMARY KEY,
    patient_id       INT NOT NULL,
    doctor_id        INT NOT NULL,
    appointment_date DATE NOT NULL,
    status           VARCHAR(20) DEFAULT 'Scheduled' 
                     CHECK (status IN ('Scheduled','Completed','Cancelled')),
    FOREIGN KEY (patient_id) REFERENCES silver.patients(patient_id),
    FOREIGN KEY (doctor_id) REFERENCES silver.doctors(doctor_id)
);

-- 5. Prescriptions
CREATE TABLE IF NOT EXISTS silver.prescriptions (
    prescription_id  INT PRIMARY KEY,
    appointment_id   INT NOT NULL,
    medicine         VARCHAR(100) NOT NULL,
    dosage           VARCHAR(50),
    duration_days    INT CHECK (duration_days >= 0),
    FOREIGN KEY (appointment_id) REFERENCES silver.appointments(appointment_id)
);

-- 6. Billing
CREATE TABLE IF NOT EXISTS silver.billing (
    bill_id         INT PRIMARY KEY,
    patient_id      INT NOT NULL,
    appointment_id  INT NOT NULL,
    amount          NUMERIC(10,2) NOT NULL CHECK (amount >= 0),
    payment_status  VARCHAR(20) NOT NULL CHECK (payment_status IN ('Paid','Pending')),
    payment_method  VARCHAR(20) CHECK (payment_method IN ('Cash','Card','UPI','Insurance')),
    FOREIGN KEY (patient_id) REFERENCES silver.patients(patient_id),
    FOREIGN KEY (appointment_id) REFERENCES silver.appointments(appointment_id)
);
