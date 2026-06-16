import pandas as pd
import psycopg2 as ps
from psycopg2.extras import execute_values
from airflow.hooks.base import BaseHook

import logging

def write_pg(
        frame:pd.DataFrame, 
        connect:str, 
        table_name:str
    ) -> bool:
    hook = BaseHook.get_connection(connect)
    reslt = True
    
    conn = ps.connect(
        dbname = table_name.split(".")[0],
        user = hook.login,
        password = hook.password,
        host = hook.host
    )

    cursor = conn.cursor()
    query:str = f"""
    insert into {table_name} ({",".join(frame.columns.tolist())})
    values %s
    """
    try:
        execute_values(
            cursor,
            query,
            frame.to_records(index=False).tolist(),
        )
        conn.commit()
    except Exception as e:
        logging.warning(f"\n\nError:\n{e}")
        reslt = False
    finally:
        cursor.close()
        conn.close()
    return reslt

def read_pg(
        connect:str,
        query:str,
        dbase:str,
    ) -> pd.DataFrame:
    hook = BaseHook.get_connection(connect)
    conn = ps.connect(
        dbname = dbase,
        user = hook.login,
        password = hook.password,
        host = hook.host
    )
    
    res_frame = pd.read_sql(
        query,
        conn,
    )
    conn.close()

    return res_frame