from typing import List
from datetime import datetime

import arrow
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

from dao.station_surge import StationSurgeDao, StationSurgeExtremeDao
from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel
from schema.station_status import StationSurgeSchema
from schema.station_surge import SurgeRealDataSchema, SurgeRealDataJoinStationSchema

app = APIRouter()


@app.get('/one/dtrange/perclock', response_model=List[StationSurgeSchema],
         response_model_include=['station_code', 'issue_ts', 'surge', 'issue_dt'],
         summary="获取指定站点指定起止时间(ts)的整点增水|整点总潮位(water level)数据集合")
def get_one_station_perclock_byts(station_code: str, start_ts: int, end_ts: int):
    dao = StationSurgeDao()
    res = dao.get_station_realdata_list(station_code, start_ts, end_ts)
    return res


@app.get('/one/targetdt/perclock', response_model=List[StationSurgeSchema],
         response_model_include=['station_code', 'issue_ts', 'surge', 'issue_dt'],
         summary="获取指定站点指定日期(dt->ts)的整点增水|整点总潮位(water level)数据集合")
def get_one_station_perclock_bydt(station_code: str, dt: str):
    dt_str: str = dt
    """YYYY-MM-DD格式的日期"""
    # 03-15 -> start: 03-14 16:00
    start_ts: int = arrow.get(dt_str, 'YYYY-MM-DD').shift(hours=-8).int_timestamp
    end_ts: int = start_ts + 24 * 60 * 60
    dao = StationSurgeDao()
    res = dao.get_station_realdata_list(station_code, start_ts, end_ts)
    return res


@app.get('/one/dtrange/extreme', response_model=List[StationSurgeSchema],
         response_model_include=['station_code', 'issue_ts', 'surge', 'issue_dt'],
         summary="获取指定站点指定时间范围内的极值集合")
def get_one_station_extreme_byts(station_code: str, start_ts: int, end_ts: int):
    dao = StationSurgeExtremeDao()
    res = dao.get_station_extreme_list(station_code, start_ts, end_ts)
    return res
