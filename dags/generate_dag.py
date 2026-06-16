import pandas as pd
from datetime import datetime as dt
from dateutil import relativedelta as rd

from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.models.taskinstance import TaskInstance
from subs.generate_funcs import (
    get_users, 
    get_events_for_users,
)
from subs.database_funcs import (
    write_pg
)
from subs.validate_data import (
    simple_validate,
    logic_validate,
)


def generate_users() -> pd.DataFrame:
    users =  get_users()
    
    users["created_at"] = users["created_at"]\
        .fillna(pd.Timestamp("1970-01-01"))
    users["created_at"] = pd.to_datetime(users["created_at"])\
        .dt.tz_localize('UTC')   
    
    return users


def generate_logs(ti:TaskInstance) -> pd.DataFrame:
    users:pd.DataFrame = ti.xcom_pull(task_ids = "generate_users")
    logs:pd.DataFrame = pd.DataFrame()
    
    days = sorted([ 
            dt.today().date() - rd.relativedelta(days=i) for i in range(35)
        ])

    for day_ in days:
        reg_events = get_events_for_users(
            users = users.loc[users["username"].notna(), "user_id"],
            day = day_,
            is_reg = True,
        )

        non_reg_events = get_events_for_users(
            users = users.loc[users["username"].isna(), "user_id"],
            day = day_,
            is_reg = False,
        )

        if len(reg_events) > 0:
            logs = pd.concat(
                [logs, reg_events], 
                ignore_index = True,
            )

        if len(non_reg_events) > 0:
            logs = pd.concat(
                [logs, non_reg_events], 
                ignore_index = True,
            )

    logs["log_time"] = pd.to_datetime(logs["log_time"])
    return logs

def validate_generated_data(ti:TaskInstance) -> pd.DataFrame:
    
    validate_data:pd.DataFrame = ti.xcom_pull(task_ids = "generate_logs")
    users:pd.DataFrame = ti.xcom_pull(task_ids = "generate_users")
    
    validate_data = simple_validate(
        validate_data
    )

    validate_data = logic_validate(
        validate_data, 
        users
    )
    
    return validate_data


def load_to_postgres(ti:TaskInstance) -> None: 
    events_frame = ti.xcom_pull(task_ids = "validate_generated_data")
    users = ti.xcom_pull(task_ids = "generate_users") 

    events_frame["log_time"] = events_frame["log_time"]\
        .dt.tz_localize("UTC")
    
    res = write_pg(
        frame = users,
        connect = "source_base",
        table_name = "log.users",
    )
    if not res:
        raise "Error in database insert"

    res = write_pg(
        frame = events_frame,
        connect = "source_base",
        table_name = "log.logs",
    )
    if not res:
        raise "Error in database insert"



with DAG(
    dag_id = "forum_data_generation",
    start_date = dt(2026,6,11),
    schedule_interval = None,
    catchup = False
) as dag:

    generate_users_task = PythonOperator(
        task_id = "generate_users",
        python_callable = generate_users,
    )

    generate_logs_task = PythonOperator(
        task_id = "generate_logs",
        python_callable = generate_logs,
    )

    validate_generated_data_task = PythonOperator(
        task_id = "validate_generated_data",
        python_callable = validate_generated_data,
    )

    load_to_postgres_task = PythonOperator(
        task_id = "load_to_postgres",
        python_callable = load_to_postgres,
    )

    generate_users_task >> \
    generate_logs_task >> \
    validate_generated_data_task >> \
    load_to_postgres_task
