# sql_cache_utils.py
import sqlite3
import pandas as pd

DB_PATH = "cache/bloomberg_data.db"

def init_connection():
    return sqlite3.connect(DB_PATH)

def read_cache(table_name):
    with init_connection() as conn:
        return pd.read_sql(f"SELECT * FROM {table_name}", conn, parse_dates=["date"]).set_index("date")

def write_cache(table_name, df):
    with init_connection() as conn:
        df.to_sql(table_name, conn, if_exists="replace")

def append_to_cache(table_name, df):
    try:
        existing = read_cache(table_name)
        new = df[~df.index.isin(existing.index)]
        if not new.empty:
            combined = pd.concat([existing, new])
            write_cache(table_name, combined)
        else:
            combined = existing
    except Exception as e:
        print(f"Table '{table_name}' not found or failed to load. Creating new table. Reason: {e}")
        combined = df
        write_cache(table_name, df)
    
    return combined

def list_cached_tables():
    conn = sqlite3.connect("cache/bloomberg_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [x[0] for x in cursor.fetchall()]
    conn.close()
    return tables




# import os
# import sqlite3

# def drop_table(table_name, db_path='your_cache.sqlite'):
#     with sqlite3.connect(db_path) as conn:
#         conn.execute(f"DROP TABLE IF EXISTS {table_name}")
#         print(f"Dropped {table_name}")


# drop_table("valuation_regionals")