import logging

def build_gold(conn):
    cursor = conn.cursor()
    logging.info("Building Gold Layer...")

    # 1. Revenue by department
    cursor.execute("TRUNCATE gold.revenue_by_department")
    cursor.execute("""
        INSERT INTO gold.revenue_by_department (department, total_revenue)
        SELECT 
            d.specialization,
            SUM(b.amount) AS total_revenue
        FROM silver.billing b
        JOIN silver.appointments a ON b.appointment_id = a.appointment_id
        JOIN silver.doctors d ON a.doctor_id = d.doctor_id
        WHERE b.payment_status = 'Paid'
        GROUP BY d.specialization
    """)

    # 2. Revenue by payment method
    cursor.execute("TRUNCATE gold.revenue_by_payment_method")
    cursor.execute("""
        INSERT INTO gold.revenue_by_payment_method (payment_method, total_revenue)
        SELECT 
            b.payment_method,
            SUM(b.amount)::NUMERIC(12,2) AS total_revenue
        FROM silver.billing b
        WHERE b.payment_status = 'Paid'
        GROUP BY b.payment_method
        ORDER BY total_revenue DESC
    """)

    # 3. Total revenue
    cursor.execute("TRUNCATE gold.total_revenue")
    cursor.execute("""
        INSERT INTO gold.total_revenue (total_revenue)
        SELECT 
            SUM(b.amount)::NUMERIC(12,2) AS total_revenue
        FROM silver.billing b
        WHERE b.payment_status = 'Paid'
    """)

    # 4. Monthly revenue
    cursor.execute("TRUNCATE gold.revenue_monthly")
    cursor.execute("""
        INSERT INTO gold.revenue_monthly (month_year, total_revenue)
        SELECT 
            DATE_TRUNC('month', a.appointment_date)::DATE AS month_year,
            SUM(b.amount)::NUMERIC(12,2) AS total_revenue
        FROM silver.billing b
        JOIN silver.appointments a 
            ON b.appointment_id = a.appointment_id
        WHERE b.payment_status = 'Paid'
        GROUP BY DATE_TRUNC('month', a.appointment_date)
        ORDER BY month_year
    """)

    # 5. Appointment utilization per doctor
    cursor.execute("TRUNCATE gold.appointment_utilization_doctor")
    cursor.execute("""
        INSERT INTO gold.appointment_utilization_doctor 
        (doctor_id, doctor_name, total_appointments, completed_appointments, completion_rate)
        SELECT 
            d.doctor_id,
            d.name AS doctor_name,
            COUNT(a.appointment_id) AS total_appointments,
            SUM(CASE WHEN a.status = 'Completed' THEN 1 ELSE 0 END) AS completed_appointments,
            ROUND(
                (SUM(CASE WHEN a.status = 'Completed' THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(a.appointment_id),0)) * 100,
                2
            ) AS completion_rate
        FROM silver.appointments a
        JOIN silver.doctors d ON a.doctor_id = d.doctor_id
        GROUP BY d.doctor_id, d.name
        ORDER BY completion_rate DESC
    """)

    # 6. Appointment utilization per patient
    cursor.execute("TRUNCATE gold.appointment_utilization_patient")
    cursor.execute("""
        INSERT INTO gold.appointment_utilization_patient 
        (patient_id, patient_name, total_appointments, completed_appointments, completion_rate)
        SELECT 
            p.patient_id,
            p.name AS patient_name,
            COUNT(a.appointment_id) AS total_appointments,
            SUM(CASE WHEN a.status = 'Completed' THEN 1 ELSE 0 END) AS completed_appointments,
            ROUND(
                (SUM(CASE WHEN a.status = 'Completed' THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(a.appointment_id),0)) * 100,
                2
            ) AS completion_rate
        FROM silver.appointments a
        JOIN silver.patients p ON a.patient_id = p.patient_id
        GROUP BY p.patient_id, p.name
        ORDER BY completion_rate DESC
    """)

    # 7. Doctor performance (Top 2 per department)
    cursor.execute("TRUNCATE gold.doctor_performance")
    cursor.execute("""
        INSERT INTO gold.doctor_performance (doctor_id, department, doctor_name, patient_count)
        WITH ranked_doctors AS (
            SELECT 
                d.doctor_id,
                d.specialization AS department,
                d.name AS doctor_name,
                COUNT(DISTINCT a.patient_id) AS patient_count,
                ROW_NUMBER() OVER (PARTITION BY d.specialization ORDER BY COUNT(DISTINCT a.patient_id) DESC) AS rn
            FROM silver.doctors d
            LEFT JOIN silver.appointments a ON d.doctor_id = a.doctor_id
            GROUP BY d.doctor_id, d.name, d.specialization
        )
        SELECT doctor_id, department, doctor_name, patient_count
        FROM ranked_doctors
        WHERE rn <= 2
    """)

    # 8. Patient insights
    cursor.execute("TRUNCATE gold.patient_insights")
    cursor.execute("""
        INSERT INTO gold.patient_insights (age_group, gender, patient_count)
        SELECT 
            CASE 
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, dob)) < 18 THEN '0-17'
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, dob)) BETWEEN 18 AND 35 THEN '18-35'
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, dob)) BETWEEN 36 AND 50 THEN '36-50'
                WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, dob)) BETWEEN 51 AND 65 THEN '51-65'
                ELSE '65+'
            END AS age_group,
            gender,
            COUNT(*) AS patient_count
        FROM silver.patients
        GROUP BY age_group, gender
        ORDER BY age_group, gender
    """)

    # 9. Medicine utilization
    cursor.execute("TRUNCATE gold.medicine_utilization")
    cursor.execute("""
        INSERT INTO gold.medicine_utilization (medicine_name, prescription_count)
        SELECT 
            medicine AS medicine_name,
            COUNT(*) AS prescription_count
        FROM silver.prescriptions
        GROUP BY medicine
        ORDER BY prescription_count DESC
    """)

    # 10. Outstanding revenue
    cursor.execute("TRUNCATE gold.outstanding_revenue")
    cursor.execute("""
        INSERT INTO gold.outstanding_revenue (patient_id, patient_name, pending_amount)
        SELECT 
            b.patient_id,
            p.name AS patient_name,
            SUM(b.amount) AS pending_amount
        FROM silver.billing b
        JOIN silver.patients p 
            ON b.patient_id = p.patient_id
        WHERE b.payment_status = 'Pending'
        GROUP BY b.patient_id, p.name
        ORDER BY pending_amount DESC
    """)

    # 11. Appointments summary
    cursor.execute("TRUNCATE gold.appointments_summary")
    cursor.execute("""
        INSERT INTO gold.appointments_summary (total_appointments, completed_appointments, completion_rate)
        SELECT 
            COUNT(*) AS total_appointments,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) AS completed_appointments,
            ROUND(
                (SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END)::NUMERIC 
                 / NULLIF(COUNT(*),0)) * 100, 
                2
            ) AS completion_rate
        FROM silver.appointments
    """)

    # 12. Total patients
    cursor.execute("TRUNCATE gold.total_patients")
    cursor.execute("""
        INSERT INTO gold.total_patients (total_patients)
        SELECT COUNT(*) AS total_patients
        FROM silver.patients
    """)

    # 13. Dashboard summary
    cursor.execute("TRUNCATE gold.dashboard_summary")
    cursor.execute("""
        INSERT INTO gold.dashboard_summary (
            total_patients, total_doctors, total_appointments,
            completed_appointments, completion_rate, total_revenue, pending_revenue
        )
        SELECT 
            (SELECT COUNT(*) FROM silver.patients) AS total_patients,
            (SELECT COUNT(*) FROM silver.doctors) AS total_doctors,
            (SELECT COUNT(*) FROM silver.appointments) AS total_appointments,
            (SELECT COUNT(*) FROM silver.appointments WHERE status = 'Completed') AS completed_appointments,
            ROUND(
                (COUNT(*) FILTER (WHERE status = 'Completed')::NUMERIC / NULLIF(COUNT(*),0)) * 100, 2
            ) AS completion_rate,
            (SELECT COALESCE(SUM(amount),0) FROM silver.billing WHERE payment_status='Paid') AS total_revenue,
            (SELECT COALESCE(SUM(amount),0) FROM silver.billing WHERE payment_status='Pending') AS pending_revenue
        FROM silver.appointments
    """)

    conn.commit()
    cursor.close()
    logging.info("Gold Layer build complete ✅")
