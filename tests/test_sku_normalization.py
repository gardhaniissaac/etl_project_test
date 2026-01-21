import pandas as pd
from pipeline.transform import normalize_sku

def test_sku_dimension_created():
    df = pd.DataFrame({"sku": ["A", "A", "B"]})

    dim_sku, fact = normalize_sku(df)

    assert len(dim_sku) == 2
    assert "sku_id" in fact.columns
