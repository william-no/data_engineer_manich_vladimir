import pandas as pd
import random as rnd
import numpy as np
import uuid
from datetime import datetime as dt
from dateutil import relativedelta as rd
from faker import Faker

from subs.events_lists import (
    reg_events_list,
    non_reg_events_list,
    non_reg_error_events_list,
)

rand=np.random.default_rng(seed=50)

def is_happend_rnd(
        trigger:int = 550
    ) -> bool:
    if int(rand.integers(low=1, high=1000)) < trigger:
        return True
    return False

def get_user_data(
        id:str = "", 
        faker:Faker = None,
        crdt:dt = None
    ) -> dict:
    if faker == None:
        faker = Faker("ru_RU") 
    return {
            "user_id"   :id,
            "username"  :faker.name(),
            "email"     :faker.email(),
            "created_at":dt(
                            2026,
                            rnd.randint(4,5),
                            rnd.randint(1,25), 
                            rnd.randint(0,23), 
                            rnd.randint(0,59),
                            rnd.randint(0,59)) if crdt is None else crdt
    }


def get_users() -> pd.DataFrame:
    faker = Faker("ru_RU")
    cnt_users = 500
    ids = [str(uuid.uuid4()) for _ in range(cnt_users)]
    users_frame:pd.DataFrame = pd.DataFrame([
        get_user_data(id, faker) if is_happend_rnd()
        else 
        {
            "user_id"   : id,
            "username"  : None,
            "email"     : None,
            "created_at": None,
        }
        for id in ids
    ])
    del faker
    return users_frame


def get_events_for_users(
        users: list,
        day: dt,
        is_reg: bool = True
    ) -> pd.DataFrame:
    day_events:pd.DataFrame = pd.DataFrame()
    ev_list = reg_events_list if is_reg else non_reg_events_list
    for user in users:
        if is_happend_rnd(trigger = 650):
            event_time:dt = day + rd.relativedelta(
                                        hours= rnd.randint(0,10),
                                        minutes= rnd.randint(0,59),
                                        seconds= rnd.randint(0,59)
                                    )
            list_event_for_day = []
            for _ in range(rnd.randint(5,12)):               
                if not is_reg:
                    # add random errors
                    ev_list = ev_list \
                        if is_happend_rnd(trigger = 350) \
                        else non_reg_error_events_list
                
                event_time += rd.relativedelta(
                                    seconds= rnd.randint(15,1400)
                                )
                event_type = ev_list[rnd.randint(0,len(ev_list)-1)]
                event = {
                    "user_id" : user,
                    "event_type" : event_type,
                    "object_type" : "topic" if "topic" in event_type\
                                    else "message" if "message" in event_type\
                                    else None,
                    "object_id" : str(uuid.uuid4()) \
                                    if "topic" in event_type or "message" in event_type \
                                    else None,
                    "server_response" : "success" if is_happend_rnd(trigger = 730) else "error",
                    "response_details" : "{}",
                    "log_time" : event_time,
                }
                list_event_for_day.append(event)
            day_events = pd.concat(
                    [
                        day_events,
                        pd.DataFrame(list_event_for_day),
                    ],
                    ignore_index=True,
                )
            del list_event_for_day
    return day_events
