# ETL Project Test

This project is a **ETL project Test** designed to demonstrate data engineering skills such as:

- Data reconciliation across multiple services
- External API enrichment
- Dimensional modeling (facts & dimensions)
- Writing back to a data warehouse (PostgreSQL)
- Unit testing of transformation logic

The pipeline simulates a real-world healthcare / service business use case.

---

## 🧠 Business Problems Addressed

1. **Reconcile business flow**
   - Lead → Appointment → Treatment → Invoice

2. **Enrich data**
   - Insurance status from external API (JSON mock)

3. **Normalize operational data**
   - Manufacturing SKU normalization into dimensions

---

## 🏗️ Architecture Overview

```text
PostgreSQL (Source Tables)
  ├── sales_leads
  ├── appointments
  ├── medical_records
  ├── slips
  ├── manufacturing_orders
  │
  ▼
Python ETL Pipeline
  ├── Extract
  ├── Transform
  ├── Enrich (Insurance API)
  ├── Normalize (SKU)
  │
  ▼
PostgreSQL (Analytics Tables)
  ├── recon_lead_to_invoice
  ├── dim_insurance_status
  ├── dim_sku
  ├── fact_manufacturing_normalized
