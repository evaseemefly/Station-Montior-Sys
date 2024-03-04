from typing import List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

from dao.station_surge import StationSurgeDao
from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel
from schema.station_status import StationSurgeSchema
from schema.station_surge import SurgeRealDataSchema, SurgeRealDataJoinStationSchema

app = APIRouter()


@app.get('/one/dtrange/perclock', response_model=List[StationSurgeSchema],
         summary="获取指定站点指定日期(ts->dt)的整点增水|整点总潮位(water level)数据集合")
def get_one_station_perclock(station_code: str, start_ts: int, end_ts: int):
    dao = StationSurgeDao()
    res = dao.get_station_realdata_list(station_code, start_ts, end_ts)
    return res
