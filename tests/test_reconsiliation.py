import pandas as pd
from pipeline.transform import reconcile_lead_to_invoice

def test_reconciliation_with_valid_payment_return_valid_conversion():
    leads = pd.DataFrame({"lead_id": [1], "customer_name": ["A"]})
    appointments = pd.DataFrame({"appointment_id": [10], "lead_id": [1]})
    records = pd.DataFrame({"appointment_id": [10], "treatment_type": ["X"]})
    slips = pd.DataFrame({"appointment_id": [10], "slip_id": [100], "total_cost": [500], "payment_method": ["cash"]})

    result = reconcile_lead_to_invoice(leads, appointments, records, slips)

    assert result.iloc[0]['is_converted'] == True
    assert result.iloc[0]['revenue_amount'] == 500

def test_reconciliation_with_valid_payment_return_invalid_conversion():
    leads = pd.DataFrame({"lead_id": [1], "customer_name": ["A"]})
    appointments = pd.DataFrame({"appointment_id": [10], "lead_id": [1]})
    records = pd.DataFrame({"appointment_id": [10], "treatment_type": ["X"]})
    slips = pd.DataFrame({"appointment_id": [10], "slip_id": [100]})

    result = reconcile_lead_to_invoice(leads, appointments, records, slips)

    print(f"Print: {result.iloc[0]['is_converted']}")
    assert result.iloc[0]['is_converted'] == False
