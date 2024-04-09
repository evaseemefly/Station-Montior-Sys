from typing import List
from datetime import datetime

import arrow
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request, Query

from dao.station_surge import StationSurgeDao, StationSurgeExtremeDao
from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel
from schema.station_status import StationSurgeSchema
from schema.station_surge import SurgeRealDataSchema, SurgeRealDataJoinStationSchema, DistStationSurgeListSchema, \
    DistStationRealdataListSchema

app = APIRouter()


@app.get('/one/dtrange/perclock', response_model=List[StationSurgeSchema],
         response_model_include=['station_code', 'issue_ts', 'surge', 'issue_dt'],
         summary="获取指定站点指定起止时间(ts)的整点增水|整点总潮位(water level)数据集合")
def get_one_station_perclock_byts(station_code: str, start_ts: int, end_ts: int):
    dao = StationSurgeDao()
    res = dao.get_station_realdata_list(station_code, start_ts, end_ts)
    return res


@app.get('/many/dtrange/perclock', response_model=List[StationSurgeSchema],
         response_model_include=['station_code', 'issue_ts', 'surge', 'issue_dt'],
         summary="获取指定站点集合指定起止时间(ts)的整点增水|整点总潮位(water level)数据集合")
def get_many_station_perclock_byts(start_ts: int, end_ts: int, station_codes: List[str] = Query(None), ):
    dao = StationSurgeDao()
    res = dao.get_stations_realdata_list(station_codes, start_ts, end_ts)
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


@app.get('/many/dtrange/extreme', response_model=List[StationSurgeSchema],
         response_model_include=['station_code', 'issue_ts', 'surge', 'issue_dt'],
         summary="获取指定站点指定时间范围内的极值集合")
def get_many_station_extreme_byts(start_ts: int, end_ts: int, station_codes: List[str] = Query(None), ):
    dao = StationSurgeExtremeDao()
    res = dao.get_stations_extreme_list(station_codes, start_ts, end_ts)
    return res


@app.get('/one/dtrange/extreme/maximum', response_model=List[StationSurgeSchema],
         response_model_include=['station_code', 'issue_ts', 'surge', 'issue_dt'],
         summary="获取指定时间范围内的指定站点的极值")
def get_one_station_maximum_byts(start_ts: int, end_ts: int, station_code: str):
    dao = StationSurgeExtremeDao()
    res = dao.get_one_extreme_maximum(station_code=station_code, start_ts=start_ts, end_ts=end_ts)
    return res


@app.get('/many/dtrange/extreme/maximum', response_model=List[StationSurgeSchema],
         response_model_include=['station_code', 'issue_ts', 'surge', 'issue_dt'],
         summary="获取指定时间范围内的指定站点的极值")
def get_many_station_maximum_byts(start_ts: int, end_ts: int, station_codes: List[str] = Query(None), ):
    dao = StationSurgeExtremeDao()
    res = dao.get_many_extreme_maximum(station_codes=station_codes, start_ts=start_ts, end_ts=end_ts)
    return res


@app.get('/dist/dtrange/extreme/maximum', response_model=List[SurgeRealDataSchema],
         response_model_include=['station_code', 'issue_ts', 'surge', 'issue_dt'],
         summary="获取指定时间范围内的指定站点的极值")
def get_dist_station_maximum_byts(start_ts: int, end_ts: int, station_codes: List[str] = Query(None), ):
    dao = StationSurgeExtremeDao()
    res = dao.get_dist_extreme_maximum(start_ts=start_ts, end_ts=end_ts)
    return res


@app.get('/one/targetdt/extreme', response_model=List[StationSurgeSchema],
         response_model_include=['station_code', 'issue_ts', 'surge', 'issue_dt'],
         summary="获取指定站点指定日期(dt)的极值集合")
def get_one_station_extreme_bydt(station_code: str, dt: str):
    dt_str: str = dt
    """YYYY-MM-DD格式的日期"""
    # 03-15 -> start: 03-14 16:00
    start_ts: int = arrow.get(dt_str, 'YYYY-MM-DD').shift(hours=-8).int_timestamp
    end_ts: int = start_ts + 24 * 60 * 60
    dao = StationSurgeExtremeDao()
    res = dao.get_station_extreme_list(station_code, start_ts, end_ts)
    return res


@app.get('/all/dtrange/max/', response_model=List[SurgeRealDataSchema],
         response_model_include=['station_code', 'issue_ts', 'surge', 'issue_dt'],
         summary="获取指定时间范围内的所有站点的极值实况极值")
def get_all_station_surgemax_byts(start_ts: int, end_ts: int):
    dao = StationSurgeDao()
    res = dao.get_all_stations_realdata_max(start_ts=start_ts, end_ts=end_ts)
    return res


@app.get('/dist/dtrange/perclock/', response_model=List[DistStationRealdataListSchema],
         response_model_include=['station_code', 'surge_list', 'ts_list'],
         summary="获取指定时间范围内的所有站点的实况集合")
def get_dist_station_surge_list(start_ts: int, end_ts: int):
    dao = StationSurgeDao()
    res = dao.get_all_stations_realdata_list(start_ts, end_ts)
    return res
