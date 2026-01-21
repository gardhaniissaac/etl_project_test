from pipeline.db import read_sql

def extract_reconciliation_sources():
    leads = read_sql("SELECT * FROM sales_leads")
    appointments = read_sql("SELECT * FROM appointments")
    records = read_sql("SELECT * FROM medical_records")
    slips = read_sql("SELECT * FROM slips")

    return leads, appointments, records, slips
