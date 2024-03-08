from typing import List, Dict
from datetime import datetime

import arrow
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request, Query

from dao.station_alert import AlertBaseDao
from dao.station_surge import StationSurgeDao, StationSurgeExtremeDao
from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel
from schema.station_status import StationSurgeSchema
from schema.station_surge import SurgeRealDataSchema, SurgeRealDataJoinStationSchema

app = APIRouter()


@app.get('/one', response_model=List[Dict],
         response_model_include=['station_code', 'tide', 'alert'], summary="获取 station_code 的四色警戒潮位")
def get_station_alert(station_code: str):
    """
    @param station_code:
    @return:
    """
    # target_url: str = f'http://128.5.10.21:8000/station/station/alert?station_code={station_code}'
    res = AlertBaseDao().get_target_station_alert(station_code)
    return res
