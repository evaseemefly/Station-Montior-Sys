from typing import List, Optional, Any

from models.models import StationStatus, RegionInfo, StationInfo
from schema.station_status import StationStatusAndGeoInfoSchema
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from dao.base import BaseDao


class StationDao(BaseDao):
    @classmethod
    def get_station_by_code(cls, list_station: List[StationInfo], code: str) -> Optional[StationInfo]:
        """
            根据站点代码获取站点信息
        :param list_station:
        :param code:
        :return:
        """
        for station in list_station:
            if station.station_code == code:
                return station
        return None

    def get_all_station(self) -> List[StationInfo]:
        """
            获取所有站点信息
        :return:
        """
        # TODO:[*] 23-04-04 Too many connections
        #  sqlalchemy.exc.OperationalError: (MySQLdb._exceptions.OperationalError)
        #  (1040, 'Too many connections')
        session: Session = self.db.session
        return session.query(StationInfo).filter(StationInfo.is_del == 0).all()

    def get_one_station(self, code: str) -> Optional[StationInfo]:
        """
            获取指定code 的站点 info
        :param code:
        :return:
        """
        session: Session = self.db.session
        return session.query(StationInfo).filter(StationInfo.station_code == code).first()
