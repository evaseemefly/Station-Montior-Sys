from typing import List
from datetime import datetime

import arrow
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request, Query

from dao.fub import FubDao
from schema.fub import DistFubObservationSchema, FubObservationSchema

app = APIRouter()


@app.get('/many/perclock/', response_model=List[DistFubObservationSchema],
         response_model_include=['code', 'observation_list'],
         summary="获取不同站点的风要素集合")
def get_many_fubs_byts(start_ts: int, end_ts: int, station_codes: List[str] = Query(None), ):
    """
        + 24-04-10 根据起止时间获取对应的站点风要素集合，并一 ws,dir,ts 集合的形式返回
    :param start_ts:
    :param end_ts:
    :param station_codes:
    :return:
    """
    dao = FubDao()
    res = dao.get_fubs_realdata_list(station_codes, start_ts, end_ts)
    return res
