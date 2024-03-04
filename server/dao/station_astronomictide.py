from typing import List, Optional
from datetime import datetime
import arrow
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from common.default import DEFAULT_LATLNG, DEFAULT_SURGE
from models.models import StationAstronomicTideRealDataModel, StationRealDataSpecific
from schema.region import RegionSchema
from schema.station_surge import SurgeRealDataSchema, TideRealDataSchema

from dao.base import BaseDao


class StationAstronomicTideDao(BaseDao):
    def get_station_tide_list(self, station_code: str, gmt_start: datetime, gmt_end: datetime) -> List[
        TideRealDataSchema]:
        list_tide: List[StationAstronomicTideRealDataModel] = self.get_daterange_tide_list(station_code, gmt_start,
                                                                                           gmt_end)
        # step1: 根据起止时间生成时间集合
        # 时间间隔单位(单位:s)——以1h为时间间隔步长
        dt_step_unit: int = 60 * 60
        dt_diff = int((arrow.get(gmt_end).timestamp() - arrow.get(gmt_start).timestamp()) / dt_step_unit)
        dt_index_list = [i for i in range(dt_diff)]
        # 根据传入的起止时间按照指定的时间间隔(dt_step_unit) 生成时间集合
        # 起始时间(arrow)
        arrow_start: arrow.Arrow = arrow.get(gmt_start)
        # 起始时间(整点时刻:arrow)
        arrow_start_hourly: arrow.Arrow = arrow.Arrow(arrow_start.year, arrow_start.month, arrow_start.day,
                                                      arrow_start.hour, 0)
        # 时间列表(整点)
        dt_utc_list: List[arrow.Arrow] = [arrow.get(arrow_start_hourly).shift(hours=i) for i in dt_index_list]
        result: List[TideRealDataSchema] = []
        # ERROR: 注意 dt_list 是 utc 时间,而 list_surge 中的时间为 local
        for temp_dt_ar_utc in dt_utc_list:
            temp_dt_utc: datetime.datetime = temp_dt_ar_utc.datetime
            filter_obj = list(filter(lambda x: x.ts == temp_dt_ar_utc.timestamp(), list_tide))
            # filter_obj[0].gmt_realtime = temp_dt_utc
            temp_obj = TideRealDataSchema(station_code=station_code,
                                          surge=filter_obj[0].surge if len(filter_obj) > 0 else DEFAULT_SURGE,
                                          gmt_realtime=temp_dt_utc,
                                          ts=temp_dt_ar_utc.timestamp(),
                                          )
            result.append(temp_obj)
        return result

    def get_daterange_tide_list(self, station_code: str, start: datetime, end: datetime) -> List[
        StationAstronomicTideRealDataModel]:
        """
            + 23-04-07 根据条件获取对应的 tide 集合
        :param station_code:
        :param start:
        :param end:
        :return:
        """
        session: Session = self.db.session
        # 使用此种方式不需要将 时间进行手动转换
        filter_query = session.query(StationAstronomicTideRealDataModel).filter(
            StationAstronomicTideRealDataModel.station_code == station_code,
            StationAstronomicTideRealDataModel.gmt_realtime >= start,
            StationAstronomicTideRealDataModel.gmt_realtime <= end)
        return filter_query.all()
