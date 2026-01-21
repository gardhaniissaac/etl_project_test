from pipeline.extract import extract_reconciliation_sources
from pipeline.transform import reconcile_lead_to_invoice, normalize_sku
from pipeline.insurance import load_insurance_data, enrich_with_insurance
from pipeline.load import load_outputs
from pipeline.db import read_sql

def main():
    leads, appointments, records, slips = extract_reconciliation_sources()

    recon = reconcile_lead_to_invoice(
        leads, appointments, records, slips
    )

    insurance = load_insurance_data("pipeline/resources/insurance_api_mock.json")
    recon = enrich_with_insurance(recon, insurance)

    manufacturing_orders = read_sql("SELECT * FROM manufacturing_orders")
    dim_sku, fact_mfg = normalize_sku(manufacturing_orders)

    load_outputs(recon, insurance, dim_sku, fact_mfg)

if __name__ == "__main__":
    main()
