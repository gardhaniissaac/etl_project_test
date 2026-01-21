import pandas as pd

def reconcile_lead_to_invoice(leads, appointments, records, slips):
    """
    Reconcile business flow from lead → appointment → treatment → invoice.

    This function performs a left-joined reconciliation across multiple
    business entities to produce an end-to-end view of the customer journey.

    Grain:
        One row per lead, enriched with appointment, treatment, and invoice data
        if available.

    Business Logic:
        - A lead may or may not have an appointment
        - An appointment may or may not have treatments
        - An appointment may or may not have an invoice (slip)
        - Conversion is defined as having a non-null slip_id
        - Revenue is derived from slip total_cost, defaulting to 0 if missing

    Parameters:
        leads (pd.DataFrame):
            Source from sales_leads table
        appointments (pd.DataFrame):
            Source from appointments table
        records (pd.DataFrame):
            Source from medical_records table
        slips (pd.DataFrame):
            Source from slips table

    Returns:
        pd.DataFrame:
            Reconciled dataset with columns:
            - lead_id
            - customer_name
            - appointment_id
            - treatment_type
            - slip_id
            - revenue_amount
            - is_converted

    Equivalent SQL Query:
    SELECT
        l.lead_id,
        l.customer_name,
        a.appointment_id,
        mr.treatment_type,
        s.slip_id,
        COALESCE(s.total_cost, 0) AS revenue_amount,
        CASE
            WHEN (s.payment_method IS NOT NULL AND s.payment_method is in ('cash', 'debit', 'credit', 'ewallet')) THEN TRUE
            ELSE FALSE
        END AS is_converted
    FROM sales_leads l
    LEFT JOIN appointments a
        ON l.lead_id = a.lead_id
    LEFT JOIN medical_records mr
        ON a.appointment_id = mr.appointment_id
    LEFT JOIN slips s
        ON a.appointment_id = s.appointment_id;

    """
    df = (
        leads
        .merge(appointments, on="lead_id", how="left")
        .merge(records, on="appointment_id", how="left")
        .merge(slips, on="appointment_id", how="left")
    )

    required_columns = ['slip_id', 'payment_method', 'total_cost']

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    valid_payment_methods = ["cash", "debit", "credit", "ewallet"]

    df["is_converted"] = (
        df["slip_id"].notnull()
        & df["payment_method"].isin(valid_payment_methods)
    )

    df["revenue_amount"] = df["total_cost"].fillna(0)

    return df[[
        "lead_id",
        "customer_name",
        "appointment_id",
        "treatment_type",
        "slip_id",
        "revenue_amount",
        "is_converted"
    ]]

def normalize_sku(manufacturing_orders: pd.DataFrame):
    """
    Normalize SKU values into a dimension table and create a SKU-based fact table.

    This function converts a denormalized SKU column into:
        1. A dimension table (dim_sku)
        2. A fact table with foreign key reference to dim_sku

    Grain:
        - dim_sku: one row per unique SKU
        - fact table: one row per manufacturing order

    Business Logic:
        - Each unique SKU is assigned a surrogate key (sku_id)
        - Manufacturing orders reference sku_id instead of raw SKU string

    Parameters:
        manufacturing_orders (pd.DataFrame):
            Source from manufacturing_orders table

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]:
            - dim_sku: normalized SKU dimension
            - fact: manufacturing orders enriched with sku_id
    
    Equivalent SQL Query:
    CREATE TABLE dim_sku AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY sku) AS sku_id,
        sku
    FROM (
        SELECT DISTINCT sku
        FROM manufacturing_orders
    ) t;

    CREATE TABLE fact_manufacturing_normalized AS
    SELECT
        mo.order_id,
        mo.sku,
        ds.sku_id,
        mo.quantity,
        mo.production_date,
        mo.status
    FROM manufacturing_orders mo
    JOIN dim_sku ds
        ON mo.sku = ds.sku;
    """
    dim_sku = (
        manufacturing_orders[["sku"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_sku["sku_id"] = dim_sku.index + 1

    fact = manufacturing_orders.merge(dim_sku, on="sku")

    return dim_sku, fact

