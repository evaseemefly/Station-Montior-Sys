from typing import List, Optional, Any, Dict
import json

import arrow

from dao.station import get_remote_service
from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel
from schema.station import StationRegionSchema
from schema.station_status import StationStatusAndGeoInfoSchema
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from dao.base import BaseDao
from schema.station_surge import AstronomicTideSchema, DistStationTideListSchema


class AlertBaseDao(BaseDao):
    def get_target_station_alert(self, station_code: str) -> List[Dict]:
        """
            获取指定站点的警戒潮位集合
        @param station_code:
        @return:
        """
        # target_url: str = f'http://128.5.10.21:8000/station/station/alert?station_code={station_code}'
        res_content: str = get_remote_service('/station/alert',
                                              params={'station_code': station_code, })
        list_region: List[Dict] = json.loads(res_content)

        return list_region

    def get_dist_station_alert(self) -> List[Dict]:
        """
            获取所有站点的警戒潮位集合
        @return:
        """
        res_content: str = get_remote_service('/station/dist/alert',
                                              params={})
        list_region: List[Dict] = json.loads(res_content)

        return list_region
