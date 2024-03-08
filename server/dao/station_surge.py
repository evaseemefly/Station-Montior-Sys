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
            根据传入的单站 code 以及 起止时间获取时间范围内的 surge data 集合
            起止时间不需要超过1个月
        @param station_code:
        @param start_ts:
        @param end_ts:
        @return:
        """
        # TODO:[*] 23-03-12 此处存在一个问题:当 起止时间不在一月(已在spider模块修改)
        # 先判断是否需要联合表进行查询
        list_surge: List[SurgePerclockDataModel] = self.get_realdata_by_params(station_code=station_code,
                                                                               start_ts=start_ts, end_ts=end_ts)
        return list_surge

    def get_stations_realdata_list(self, station_codes: List[str], start_ts: int, end_ts: int) -> List[
        SurgeRealDataSchema]:
        """
            根据传入的多站 code 以及 起止时间获取时间范围内的 surge data 集合
            起止时间不需要超过1个月
        @param station_codes:
        @param start_ts:
        @param end_ts:
        @return:
        """
        # TODO:[*] 23-03-12 此处存在一个问题:当 起止时间不在一月(已在spider模块修改)
        # 先判断是否需要联合表进行查询
        list_surge: List[SurgePerclockDataModel] = self.get_stations_realdata_by_params(station_codes=station_codes,
                                                                                        start_ts=start_ts,
                                                                                        end_ts=end_ts)
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
                                                    SurgePerclockDataModel.issue_ts >= start_ts,
                                                    SurgePerclockDataModel.issue_ts <= end_ts).order_by(
            SurgePerclockDataModel.issue_ts)
        res = session.execute(stmt).scalars().all()
        return res

    def get_stations_realdata_by_params(self, **kwargs):
        """
            获取对应 station_codes的实况
        :param kwargs:
        :return:
        """
        session: Session = self.db.session
        station_codes: List[str] = kwargs.get('station_codes')
        res: List[SurgePerclockDataModel] = []
        """站点code"""
        start_ts: int = kwargs.get('start_ts')
        """起始ts"""
        end_ts: int = kwargs.get('end_ts')
        """结束ts(可空)"""
        if end_ts is None:
            # 未传入结束时间，按照start_ts+24h赋值
            end_ts = start_ts + 24 * 60 * 60
        for temp_code in station_codes:
            stmt = select(SurgePerclockDataModel).where(SurgePerclockDataModel.station_code == temp_code,
                                                        SurgePerclockDataModel.issue_ts >= start_ts,
                                                        SurgePerclockDataModel.issue_ts <= end_ts).order_by(
                SurgePerclockDataModel.issue_ts)
            temp_res = session.execute(stmt).scalars().all()
            res.extend(temp_res)
        return res


class StationSurgeExtremeDao(BaseDao):
    def get_station_extreme_list(self, station_code: str, start_ts: int, end_ts: int) -> List[
        SurgePerclockExtremumDataModel]:
        """
            获取单站的极值集合
        :param station_code:
        :param start_ts:
        :param end_ts:
        :return:
        """
        list_extreme: List[SurgePerclockExtremumDataModel] = self.get_extreme_byparams(station_code=station_code,
                                                                                       start_ts=start_ts, end_ts=end_ts)
        return list_extreme

    def get_stations_extreme_list(self, station_codes: List[str], start_ts: int, end_ts: int) -> List[
        SurgePerclockExtremumDataModel]:
        """
            获取多站极值集合
        :param station_codes:
        :param start_ts:
        :param end_ts:
        :return:
        """
        list_extreme: List[SurgePerclockExtremumDataModel] = self.get_stations_extreme_byparams(
            station_code=station_codes,
            start_ts=start_ts, end_ts=end_ts)
        return list_extreme

    def get_stations_extreme_byparams(self, **kwargs):
        """
            获取站点集合对应时间内的极值集合
        :param kwargs:station_code,start_ts,end_ts
        :return:
        """
        session: Session = self.db.session
        station_codes: List[str] = kwargs.get('station_code')
        """站点code"""
        start_ts: int = kwargs.get('start_ts')
        """起始ts"""
        end_ts: int = kwargs.get('end_ts')
        res: List[SurgePerclockExtremumDataModel] = []
        """结束ts(可空)"""
        if end_ts is None:
            # 未传入结束时间，按照start_ts+24h赋值
            end_ts = start_ts + 24 * 60 * 60
        for temp_code in station_codes:
            stmt = select(SurgePerclockExtremumDataModel).where(
                SurgePerclockExtremumDataModel.station_code == temp_code,
                SurgePerclockExtremumDataModel.issue_ts >= start_ts,
                SurgePerclockExtremumDataModel.issue_ts <= end_ts).order_by(
                SurgePerclockExtremumDataModel.issue_ts)
            temp_res = session.execute(stmt).scalars().all()
            res.extend(temp_res)
        return res

    def get_extreme_byparams(self, **kwargs):
        """
            根据参数直接查询 model 集合
        :param kwargs:station_code,start_ts,end_ts
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
        stmt = select(SurgePerclockExtremumDataModel).where(SurgePerclockExtremumDataModel.station_code == station_code,
                                                            SurgePerclockExtremumDataModel.issue_ts >= start_ts,
                                                            SurgePerclockExtremumDataModel.issue_ts <= end_ts).order_by(
            SurgePerclockExtremumDataModel.issue_ts)
        res = session.execute(stmt).scalars().all()
        return res

    def get_one_extreme_maximum(self, **kwargs):
        """
            获取单站极值model集合
        :param kwargs:station_code,start_ts,end_ts
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
        stmt = select(SurgePerclockExtremumDataModel).where(
            SurgePerclockExtremumDataModel.station_code == station_code,
            SurgePerclockExtremumDataModel.issue_ts >= start_ts,
            SurgePerclockExtremumDataModel.issue_ts <= end_ts)
        sub_stmt = select(func.max(SurgePerclockExtremumDataModel.surge)).where(
            SurgePerclockExtremumDataModel.station_code == station_code,
            SurgePerclockExtremumDataModel.issue_ts >= start_ts,
            SurgePerclockExtremumDataModel.issue_ts <= end_ts)
        stmt = stmt.where(SurgePerclockExtremumDataModel.surge == sub_stmt)
        res = session.execute(stmt).scalars().all()
        return res

    def get_many_extreme_maximum(self, **kwargs):
        """
            获取多个站点的极值最大值(指定时间范围内)集合
        :param kwargs: station_codes,start_ts,end_ts
        :return:
        """
        session: Session = self.db.session
        station_codes: str = kwargs.get('station_codes')
        """站点code"""
        start_ts: int = kwargs.get('start_ts')
        """起始ts"""
        end_ts: int = kwargs.get('end_ts')
        res: List[SurgePerclockExtremumDataModel] = []
        """结束ts(可空)"""
        if end_ts is None:
            # 未传入结束时间，按照start_ts+24h赋值
            end_ts = start_ts + 24 * 60 * 60
        for temp_code in station_codes:
            stmt = select(SurgePerclockExtremumDataModel).where(
                SurgePerclockExtremumDataModel.station_code == temp_code,
                SurgePerclockExtremumDataModel.issue_ts >= start_ts,
                SurgePerclockExtremumDataModel.issue_ts <= end_ts)
            sub_stmt = select(func.max(SurgePerclockExtremumDataModel.surge)).where(
                SurgePerclockExtremumDataModel.station_code == temp_code,
                SurgePerclockExtremumDataModel.issue_ts >= start_ts,
                SurgePerclockExtremumDataModel.issue_ts <= end_ts)
            stmt = stmt.where(SurgePerclockExtremumDataModel.surge == sub_stmt)
            temp_res = session.execute(stmt).scalars().all()
            res.extend(temp_res)
        return res
