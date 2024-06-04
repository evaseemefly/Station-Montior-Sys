from typing import List
from datetime import datetime

import arrow
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request, Query

from dao.fub import FubDao
from schema.fub import DistFubObservationSchema, FubObservationSchema

app = APIRouter()


# station_codes: List[str] = Query(None), ):
# Query(alias="station_codes", type=List[str])
# def get_many_fubs_byts(start_ts: int, end_ts: int, station_codes: Query(alias="station_codes", type=List[str])):
@app.get('/realtime/many/perclock/', response_model=List[DistFubObservationSchema],
         response_model_include=['code', 'observation_list', 'obs_type'],
         summary="获取不同站点的风要素集合")
def get_many_fubs_byts(start_ts: int, end_ts: int,
                       station_codes: List[str] = Query(None, alias="station_codes[]", type=List[str])):
    """
        + 24-04-10 根据起止时间获取对应的站点风要素集合，并一 ws,dir,ts 集合的形式返回
    :param start_ts:
    :param end_ts:
    :param station_codes:
    :return:
    """
    # TODO:[-] 24-05-08 此处遇见一个bug是
    dao = FubDao()
    res = dao.get_fubs_realdata_list(station_codes, start_ts, end_ts)
    return res


@app.get('/all/codes/', response_model=List[str],
         summary="所有浮标站点codes")
def get_all_fubs_codes():
    """
        获取所有浮标站点的codes
    :return:[
    "MF02004",
    "MF02001"
]
    """
    dao = FubDao()
    res = dao.get_all_fubs_codes()
    return res


@app.get('/all/info/', response_model=List[dict],
         summary="所有浮标站点codes")
def get_all_fubs_info():
    """
        获取所有浮标站点的信息
    :return: [
    {
        "id": 2,
        "name": "大型浮标02001",
        "code": "MF02001",
        "lat": 38.548,
        "lon": 121.166,
        "sort": 1,
        "is_del": false,
        "fub_type": 602,
        "fub_kind": 611
    },
    """
    dao = FubDao()
    res = dao.get_all_fubs()
    return res
