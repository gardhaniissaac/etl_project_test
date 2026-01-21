from pipeline.db import write_df

def load_outputs(recon_df, insurance_df, dim_sku, fact_mfg):
    write_df(recon_df, "recon_lead_to_invoice")
    write_df(insurance_df, "dim_insurance_status")
    write_df(dim_sku, "dim_sku")
    write_df(fact_mfg, "fact_manufacturing_normalized")
