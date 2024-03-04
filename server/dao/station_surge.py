from typing import List, Optional, Any
from datetime import datetime
import arrow
from sqlalchemy import select, update, func, and_, text
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel
from schema.region import RegionSchema
from schema.station_surge import SurgeRealDataSchema

from dao.base import BaseDao


class StationSurgeDao(BaseDao):
    def get_station_realdata_list(self, station_code: str, start_ts: int, end_ts: int) -> List[
        SurgeRealDataSchema]:
        """
            根据传入的海洋站 code 以及 起止时间获取时间范围内的 surge data 集合
            起止时间不需要超过1个月
        @param station_code:
        @param gmt_start:
        @param gmt_end:
        @return:
        """
        # TODO:[*] 23-03-12 此处存在一个问题:当 起止时间不在一月(已在spider模块修改)
        # 先判断是否需要联合表进行查询
        list_surge: List[SurgePerclockDataModel] = self.get_realdata_by_params(station_code=station_code,
                                                                               start_ts=start_ts, end_ts=end_ts)
        return list_surge

    def get_realdata_by_params(self, **kwargs):
        """
            根据传入的动态参数查询实况
        :param kwargs:
        :return:
        """
        session: Session = self.db.session
        station_code: str = kwargs.get('station_code')
        """站点code"""
        start_ts: int = kwargs.get('start_ts')
        """起始ts"""
        end_ts: int = kwargs.get('end_ts')
        """结束ts(可空)"""
        if end_ts is None:
            # 未传入结束时间，按照start_ts+24h赋值
            end_ts = start_ts + 24 * 60 * 60
        stmt = select(SurgePerclockDataModel).where(SurgePerclockDataModel.station_code == station_code,
                                                    SurgePerclockDataModel.issue_ts > start_ts,
                                                    SurgePerclockDataModel.issue_ts < end_ts).order_by(
            SurgePerclockDataModel.issue_ts)
        res = session.execute(stmt).fetchall()
        return res
