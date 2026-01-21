import pandas as pd
from sqlalchemy import create_engine

def get_engine():
    return create_engine('postgresql://user:password@localhost:5432/rata_id')

def read_sql(query: str) -> pd.DataFrame:
    return pd.read_sql(query, get_engine())

def write_df(df: pd.DataFrame, table: str):
    df.to_sql(table, get_engine(), if_exists="replace", index=False)

    print(f"Table {table} successfully loaded to destinations")
    return True
