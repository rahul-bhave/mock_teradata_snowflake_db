import snowflake.connector
import teradatasql
import pandas as pd
from typing import Dict, Any

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def connect_to_teradata(config: Dict[str, Any]) -> teradatasql.connect:
    """Connects to Teradata database"""
    conn = teradatasql.connect(
        host=config['teradata']['host'],
        user=config['teradata']['user'],
        password=config['teradata']['password']
    )
    return conn

def connect_to_snowflake(config: Dict[str, Any]) -> snowflake.connector.connect:
    """Connects to Snowflake database"""
    conn = snowflake.connector.connect(
        user=config['snowflake']['user'],
        password=config['snowflake']['password'],
        account=config['snowflake']['account'],
        warehouse=config['snowflake']['warehouse'],
        database=config['snowflake']['database'],
        schema=config['snowflake']['schema']
    )
    return conn

def compare_tables(config: Dict[str, Any], conn_td: teradatasql.connect, conn_sf: snowflake.connector.connect,
                   table_name: str) -> pd.DataFrame:
    """Compares the data between the specified Teradata and Snowflake tables"""
    # Retrieve the data from Teradata
    td_query = f"SELECT * FROM {table_name}"
    df_td = pd.read_sql(td_query, conn_td)

    # Retrieve the data from Snowflake
    sf_query = f"SELECT * FROM {table_name}"
    df_sf = pd.read_sql(sf_query, conn_sf)

    # Compare the data
    df_diff = pd.concat([df_td, df_sf]).drop_duplicates(keep=False)

    return df_diff
