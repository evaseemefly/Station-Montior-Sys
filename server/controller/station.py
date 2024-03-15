from typing import List, Type, Any, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

from dao.station import StationBaseDao
from schema.station import StationBaseInfoSchema

app = APIRouter()


@app.get('/all/list', response_model=List[StationBaseInfoSchema],
         response_model_include=['id', 'name', 'code', 'lat', 'lon', 'is_abs', 'pid', 'base_level_diff', 'd85',
                                 'is_in_use', 'sort', 'is_in_common_use'],
         summary="获取所有in use 的站点")
async def get_all_station():
    station_dao = StationBaseDao()
    res_list = station_dao.get_dist_station_list()
    return res_list
