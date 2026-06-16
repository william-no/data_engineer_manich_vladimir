# utf-8
import os
import pandas as pd
from datetime import datetime as dt
from dateutil import relativedelta as rd

from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.models.taskinstance import TaskInstance
from airflow.models.param import Param

from subs.database_funcs import read_pg
from subs.validate_data import (
    simple_validate,
    logic_validate,
)
from subs.aggregation_funcs import (
    avg_session,
    topics_growth,
    retention_d1,
)

DAGS_FOLDER = os.environ.get(
    "AIRFLOW_HOME", 
    "/opt/airflow",
) + "/dags/"


def check_data_availability(**context) -> None:
    day_ = context["params"]["aggregation_day"]
    
    frame:pd.DataFrame = read_pg(
        connect = "source_base",
        query = f"""select * from log.logs
                    where date_trunc("day", log_time) = "{day_}"
                    limit 10;
                """,
        dbase= "log",
    )
    
    if frame.empty:
        raise "No data for aggrigate!"

    pass


def extract_raw_logs(**context) -> pd.DataFrame:
    day_ = context["params"]["aggregation_day"]
    
    frame:pd.DataFrame = read_pg(
        connect="source_base",
        query = f"""
            select
                    main.user_id as user_id,
                    main.event_type as event_type,
                    us.username as username,
                    main.object_type as object_type,
                    main.object_id as object_id,
                    main.server_response as server_response,
                    main.log_time as log_time 
            from log.logs main
            left join log.users us on main.user_id = us.user_id
            where date_trunc("day", main.log_time) 
                    between "{day_}"::date - interval "1 day" 
                    and "{day_}"::date
        """,
        dbase= "log",
    )
    return frame


def validate_data(ti:TaskInstance) -> pd.DataFrame:
    frame:pd.DataFrame = ti.xcom_pull(task_ids= "extract_raw_logs")

    res_frame = simple_validate(frame)
    res_frame = logic_validate(res_frame)
    
    return res_frame


def transform_aggregates(**context) -> pd.DataFrame:
    day_:dt = context["params"]["aggregation_day"]

    ti:TaskInstance = context["ti"]    
    frame:pd.DataFrame = ti.xcom_pull(task_ids= "validate_data")
    
    result_data = {
        "day": day_,

        "new_accounts": frame[
                frame["event_type"] == "registarion"
            ]["log_time"].count(),

        "anon_messages_pct": frame[
                    (frame["event_type"] == "write_message")
                    & (frame["username"] == "")
                    & (pd.to_datetime(frame["log_time"]) >= day_ )
                ]["log_time"].count(),

        "total_messages": frame[
                    frame["event_type"] =="write_message"
                ]["log_time"].count(),

        "topics_growth_pct": topics_growth(frame, day_),
        "avg_session_duration_sec": avg_session(frame),
        "d1_retention_pct": retention_d1(frame, day_),
    }
    nframe = pd.DataFrame([result_data])
    
    return nframe

def export_to_csv(ti:TaskInstance) -> None:
    frame:pd.DataFrame = ti.xcom_pull(task_ids= "transform_aggregates")
    
    frame.to_csv(
        f"{DAGS_FOLDER}result_file_{dt.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv", 
        index=False
    )
    pass

def alert_on_anomaly(**context) -> None:
    day_:dt = context["params"]["aggregation_day"]

    ti:TaskInstance = context["ti"]    
    frame:pd.DataFrame = ti.xcom_pull(task_ids= "validate_data")
    
    non_reg_messages = frame[
        (frame["event_type"] == "write_message")
        & (frame["username"] == "")
        & (frame["log_time"] >=day_)
    ]["log_time"].count()

    total_messages = frame[
        (frame["event_type"] == "write_message")
        & (frame["log_time"] >=day_)
    ]["log_time"].count()

    error_responce = frame[
        (frame["server_response"] == "error")
        & (frame["log_time"] >=day_)
    ]["log_time"].count()

    all_responce = frame[
        (frame["log_time"] >=day_)
    ]["log_time"].count()
    
    results_ = ""
    if non_reg_messages / total_messages > 0.8:
        results_ += f"Anonaly high anonimus activity! ({non_reg_messages / total_messages})\n"
    
    if error_responce / all_responce > 0.5:
        results_ += f"Some problems with server responce! ({error_responce / all_responce})\n"
    
    if results_ != "":
        file_name = f"{DAGS_FOLDER}report_{dt.now().strftime("%Y-%m-%d_%H-%M-%S")}.txt"
        with open(
            file_name,
            "w",
        ) as fl:
            fl.write(results_)    
        raise f"Some anomaly detectet\nReport in file <{file_name}>"
    pass


with DAG(
    dag_id = "forum_log_aggregation",
    start_date = dt(2026,6,11),
    schedule_interval = None,
    catchup = False,
    params={"aggregation_day": Param(
            default = (dt.now() - rd.relativedelta(days=1)).date, 
            type = "string", 
            format = "date",
        )},
) as dag:
    
    check_data_availability_task = PythonOperator(
        task_id = "check_data_availability",
        python_callable = check_data_availability,
    )

    extract_raw_logs_task = PythonOperator(
        task_id = "extract_raw_logs",
        python_callable = extract_raw_logs,
    )

    validate_data_task = PythonOperator(
        task_id = "validate_data",
        python_callable = validate_data,
    )

    transform_aggregates_task = PythonOperator(
        task_id = "transform_aggregates",
        python_callable = transform_aggregates,
    )

    export_to_csv_task = PythonOperator(
        task_id = "export_to_csv",
        python_callable = export_to_csv,
    )

    alert_on_anomaly_task = PythonOperator(
        task_id = "alert_on_anomaly",
        python_callable = alert_on_anomaly,
    )

    check_data_availability_task >> \
    extract_raw_logs_task >> \
    validate_data_task >> \
    transform_aggregates_task >> \
    export_to_csv_task >> \
    alert_on_anomaly_task