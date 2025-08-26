Reconciliation Notes (Silver → Gold)
Reconciliation ensures that the aggregated KPIs in Gold are exactly consistent with the detailed data in Silver.

Patients
COUNT(*) from silver.patients = gold.total_patients.total_patients.
Confirms patient totals are identical.

Appointments
COUNT(*) from silver.appointments = gold.appointments_summary.total_appointments.
COUNT(*) where status = 'Completed' in Silver = gold.appointments_summary.completed_appointments.
Validates appointment counts across layers.

Revenue
SUM(amount) in silver.billing = SUM(pending_amount) + paid revenue in Gold.
SUM(amount) where payment_status = 'Paid' in Silver = gold.total_revenue.total_revenue.
Ensures revenue figures are consistent.

Revenue by Department
Aggregated revenue from Silver (billing → appointments → doctors) = gold.revenue_by_department.
Guarantees departmental revenue breakdown matches.

There are no expected differences between Silver and Gold values, as transformations are exact.
