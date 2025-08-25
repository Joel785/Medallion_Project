CREATE SCHEMA IF NOT EXISTS audit;

-- Rejected rows table
CREATE TABLE IF NOT EXISTS audit.rejected_rows (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(50),
    row_data JSONB,
    error_reason TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
