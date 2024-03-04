from typing import List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

from dao.station_astronomictide import StationAstronomicTideDao
from models.models import StationRealDataSpecific
from schema.station_status import StationSurgeSchema
from schema.station_surge import TideRealDataSchema

app = APIRouter()


@app.get('/one/', response_model=List[TideRealDataSchema],
         response_model_include=['station_code', 'gmt_realtime', 'surge', 'ts', ],
         summary="获取单站的天文潮位集合")
def get_one_station_tide(station_code: str, start_dt: datetime, end_dt: datetime):
    """

    :param station_code:
    :param start_dt:
    :param end_dt:
    :return:
    """

    """
        raise ValidationError(errors, field.type_)
        pydantic.error_wrappers.ValidationError: 24 validation errors for TideRealDataSchema
        response -> 0
          value is not a valid dict (type=type_error.dict)
    """
    surge_list = StationAstronomicTideDao().get_station_tide_list(station_code, start_dt, end_dt)
    return surge_list
