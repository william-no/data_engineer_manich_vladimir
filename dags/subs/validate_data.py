import pandas as pd

import pandera.pandas as pa
from pandera import Check, Column
from pandera.errors import SchemaError
from subs.events_lists import all_events_list

import logging


def simple_validate(frame:pd.DataFrame) -> None:
    
    checked_schema = pa.DataFrameSchema({
        "user_id" : Column(
            pa.String,
            nullable= False,
        ) ,
        "event_type" : Column(
            pa.String,
            Check.isin(all_events_list),
        ) ,
        "object_type" : Column(
            pa.String, 
            Check.isin(["topic","message",None]),
            nullable=True,
        ) ,
        "object_id" : Column(
            pa.String, 
            nullable= True,
        ) ,
        "server_response" : Column(
            pa.String ,
            Check.isin(["success", "error"])
        ) ,
        "response_details" : Column(
            pa.String,
            nullable= False
        ) ,
        "log_time" : Column(
            pa.DateTime, 
            nullable= False,
        ) ,
    })
    
    try:
        validated_frame = checked_schema.validate(frame)
    except SchemaError as exc:
        logging.warning(f"\n\n\nSome problems with dataframe:\n{exc}")
        invalid_indexes = exc.failure_cases['index'].dropna().unique()
        validated_frame = validated_frame.drop(index = invalid_indexes)

    return validated_frame


def logic_validate(
        frame:pd.DataFrame,
        users:pd.DataFrame = None,
    ) -> pd.DataFrame:
    
    sub_frame = frame.loc[frame["event_type"]
            .isin(["login","logout"])
            ]
    sub_frame.sort_values(
        by=["user_id", "log_time"],
        ascending=[True,True],
        inplace = True,
    )


    sub_frame["prev_in_out"] = sub_frame \
        .groupby("user_id")["event_type"] \
        .shift(1)
    
    res_frame = pd.merge(
        frame, 
        sub_frame, 
        on = frame.columns.to_list(), 
        how = "inner",
    )

    if users is not None:
        res_frame = pd.merge(
            res_frame,
            users,
            on = "user_id",
            how = "inner",
        )

    err_logouts = res_frame.loc[
        (res_frame["event_type"] == "logout")
        & (res_frame["prev_in_out"] == "logout")
    ].index

    err_topic_crt = res_frame.loc[
        (res_frame["event_type"] == "create_topic")
        & (res_frame["username"] == None)
    ].index
    
    validated_frame = frame.drop(
        index= err_logouts
    )

    validated_frame = validated_frame.drop(
        index= err_topic_crt
    )

    return validated_frame