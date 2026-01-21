import pandas as pd
from pipeline.insurance import enrich_with_insurance

def test_insurance_enrichment():
    base = pd.DataFrame({"customer_name": ["Customer 367"]})
    insurance = pd.DataFrame({
        "patient_name": ["Customer 367"],
        "insurance_status": ["pending"]
    })

    result = enrich_with_insurance(base, insurance)
    assert result.iloc[0]["insurance_status"] == "pending"
