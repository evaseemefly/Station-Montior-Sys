from typing import List, Optional, Any

from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel
from schema.station_status import StationStatusAndGeoInfoSchema
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from dao.base import BaseDao


class StationDao(BaseDao):
    pass
