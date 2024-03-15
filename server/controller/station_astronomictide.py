from typing import List, Dict
from datetime import datetime

import arrow
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request, Query

from dao.station import StationBaseDao
from dao.station_alert import AlertBaseDao
from dao.station_surge import StationSurgeDao, StationSurgeExtremeDao
from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel
from schema.station_status import StationSurgeSchema
from schema.station_surge import SurgeRealDataSchema, SurgeRealDataJoinStationSchema, AstronomicTideSchema, \
    DistStationTideListSchema

app = APIRouter()


@app.get('/one', response_model=List[AstronomicTideSchema])
def get_astronomictide_list(station_code: str, start_ts: int, end_ts: int):
    """
        + 23-07-20
        获取指定站点的天文潮位
    @param station_code:
    @param start_ts:起始时间戳
    @param end_ts:结束时间戳
    @return:
    """

    # TODO:[*] 23-07-20 此处需要修改为采用服务发现
    # 目前请求地址
    # http://128.5.10.21:8000/station/station/astronomictide/list?station_code=BHI&start_dt=2023-07-19T16:00:00.000Z&end_dt=2023-07-21T16:00:00.000Z
    # TODO:[*] 23-11-17 修改为通过服务发现调用服务获取指定站点的天文潮集合
    list_res: List[AstronomicTideSchema] = StationBaseDao().get_target_astronomictide(station_code, start_ts, end_ts)
    return list_res


@app.get('/dist/dtrang', response_model=List[DistStationTideListSchema])
def get_astronomictide_list(start_ts: int, end_ts: int):
    """
        + 23-07-20
        获取指定站点的天文潮位
    @param station_code:
    @param start_ts:起始时间戳
    @param end_ts:结束时间戳
    @return:
    """

    # TODO:[*] 23-07-20 此处需要修改为采用服务发现
    # 目前请求地址
    # http://128.5.10.21:8000/station/station/astronomictide/list?station_code=BHI&start_dt=2023-07-19T16:00:00.000Z&end_dt=2023-07-21T16:00:00.000Z
    # TODO:[*] 23-11-17 修改为通过服务发现调用服务获取指定站点的天文潮集合
    list_res: List[DistStationTideListSchema] = StationBaseDao().get_dist_station_astronomictide_list(start_ts, end_ts)
    return list_res
