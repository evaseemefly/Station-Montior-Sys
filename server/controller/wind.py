from typing import List
from datetime import datetime

import arrow
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request, Query

from dao.station_wind import StationWindDao
from schema.station_surge import DistStationWindListSchema

app = APIRouter()


@app.get('/dist/dtrange/perclock/', response_model=List[DistStationWindListSchema],
         response_model_include=['station_code', 'ws_list', 'ts_list', 'dir_list'],
         summary="获取不同站点的风要素集合")
def get_dist_station_wind_byts(start_ts: int, end_ts: int, station_codes: List[str] = Query(None), ):
    """
        + 24-04-10 根据起止时间获取对应的站点风要素集合，并一 ws,dir,ts 集合的形式返回
    :param start_ts:
    :param end_ts:
    :param station_codes:
    :return:
    """
    dao = StationWindDao()
    res = dao.get_all_stations_realdata_list(start_ts, end_ts)
    return res
    pass
