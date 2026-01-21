import pandas as pd
import json

def load_insurance_data(path: str) -> pd.DataFrame:
    with open(path, "r") as f:
        data = json.load(f)
    return pd.DataFrame(data)

def enrich_with_insurance(df: pd.DataFrame, insurance_df: pd.DataFrame):
    return df.merge(
        insurance_df,
        left_on="customer_name",
        right_on="patient_name",
        how="left"
    )
