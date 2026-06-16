import pandas as pd

from datetime import datetime as dt, timedelta

def topics_growth(
        frame:pd.DataFrame, 
        day:dt,
    ) -> float:
    cnt_day = frame[
        (frame["event_type"] == "create_topic")
        & (pd.to_datetime(frame["log_time"]) >= day )
    ]["log_time"].count() 

    cnt_day_before = frame[
        (frame["event_type"] == "create_topic")
        & (pd.to_datetime(frame["log_time"]) < day )
    ]["log_time"].count()
    
    return (cnt_day - cnt_day_before) / cnt_day_before * 100 


def avg_session(frame:pd.DataFrame) -> int:
    sub_frame = frame.loc[frame["event_type"]
            .isin(["login","logout"])
            ]
    sub_frame.sort_values(
        by=["user_id", "log_time"],
        ascending=[True,True],
        inplace = True,
    )

    sub_frame["prev_time"] = sub_frame \
        .groupby("user_id")["log_time"] \
        .shift(1)
    
    sub_frame["session_dur"] = sub_frame["log_time"] \
        - sub_frame["prev_time"]
    
    duration:timedelta = sub_frame["session_dur"].mean()

    return int(duration.total_seconds())


def retention_d1(
        frame:pd.DataFrame, 
        day:dt,
    ) -> int:
    id_registration = frame[
        (frame["event_type"] == "registration")
        & (pd.to_datetime(frame["log_time"]) >= day )
    ]["user_id"].unique().tolist()

    cnt_returned = frame[
        (frame["user_id"].isin(id_registration))
        & (pd.to_datetime(frame["log_time"]) < day )
    ]["user_id"].unique().tolist()

    return len(cnt_returned)