import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def get_connection():
    return psycopg2.connect(
        host="localhost",
        dbname="rata_id",
        user="metabase",
        password="metabasepass",
        port=5433
    )

def get_engine():
    return create_engine('postgresql://metabase:metabasepass@localhost:5433/rata_id')

def read_sql(query: str) -> pd.DataFrame:
    return pd.read_sql(query, get_engine())

def write_df(df: pd.DataFrame, table: str):
    df.to_sql(table, get_engine(), if_exists="replace", index=False)

    print(f"Table {table} successfully loaded to destinations")
    return True
