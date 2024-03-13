from typing import List, Optional, Any
from datetime import datetime
import arrow
from sqlalchemy import select, update, func, and_, text, TextClause
from sqlalchemy.orm import Session, aliased
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

    def get_all_stations_realdata_max(self, start_ts: int, end_ts: int) -> List[
        SurgeRealDataSchema]:
        """
            获取所有站点的 指定时间范围内的极值
        :param start_ts:
        :param end_ts:
        :return:
        """

        """
            查询sql
            SELECT station_code, surge, issue_dt
            FROM surge_perclock_data_realtime_template
            WHERE issue_ts >= 1708344000
              and issue_ts <= 1708426800
              and surge in (
                SELECT MAX(surge)
                FROM surge_perclock_data_realtime_template
                WHERE issue_ts >= 1708344000
                  and issue_ts <= 1708426800
                GROUP BY station_code
            )
        """
        session: Session = self.db.session
        res: List[SurgePerclockDataModel] = []
        """站点code"""
        if end_ts is None:
            # 未传入结束时间，按照start_ts+24h赋值
            end_ts = start_ts + 24 * 60 * 60
        """
            SELECT max(surge_perclock_data_realtime_template.surge) AS max_1, surge_perclock_data_realtime_template.station_code 
            FROM surge_perclock_data_realtime_template 
            WHERE surge_perclock_data_realtime_template.issue_ts >= :issue_ts_1 
            AND surge_perclock_data_realtime_template.issue_ts <= :issue_ts_2 
            GROUP BY surge_perclock_data_realtime_template.station_code
        """
        # 注意此处不能使用 select([xx,xx])
        # stmt_sub = select(func.max(SurgePerclockDataModel.surge).label('max_surge'),
        #                   SurgePerclockDataModel.station_code
        #                   ).where(
        #     SurgePerclockDataModel.issue_ts >= start_ts,
        #     SurgePerclockDataModel.issue_ts <= end_ts).group_by(SurgePerclockDataModel.station_code)
        # # SELECT max(surge_perclock_data_realtime_template.surge) AS max_surge, surge_perclock_data_realtime_template.station_code
        # # FROM surge_perclock_data_realtime_template
        # # WHERE surge_perclock_data_realtime_template.issue_ts >= :issue_ts_1 AND surge_perclock_data_realtime_template.issue_ts <= :issue_ts_2 GROUP BY surge_perclock_data_realtime_template.station_code
        # sub_q = stmt_sub.subquery()
        # # [526.0, 274.0, 537.0, 150.0, 183.0, 436.0, 263.0]
        # aliased(SurgePerclockDataModel, sub_q, name='perclock_data')
        # res_sub = session.execute(stmt_sub).scalars().all()
        # # TODO:[-] 24-03-12 AttributeError: 'Subquery' object has no attribute 'station_code'
        # stmt = select(SurgePerclockDataModel.surge, SurgePerclockDataModel.station_code,
        #               SurgePerclockDataModel.issue_ts, SurgePerclockDataModel.issue_dt).where(
        #     SurgePerclockDataModel.issue_ts >= start_ts,
        #     SurgePerclockDataModel.issue_ts <= end_ts).join(sub_q,
        #                                                     SurgePerclockDataModel.station_code == sub_q.station_code,
        #                                                     sub_q.max_surge == SurgePerclockDataModel.surge)
        # sub_q.station_code == SurgePerclockDataModel.station_code,
        # sub_q.max_surge == SurgePerclockDataModel.surge)
        # temp_res = session.execute(stmt).scalars().all()
        # 方法2:建议对于复杂的子查询直接使用sql语句拼接的方式

        # TODO:[-] 24-03-12 此处需要将 table name 修改为动态表名
        sql_str: TextClause = text(f"""
            SELECT MAIN.station_code, MAIN.surge, MAIN.issue_ts,MAIN.issue_dt
            FROM surge_perclock_data_realtime_template AS MAIN
            JOIN (
                SELECT MAX(surge) as surge_max,surge_perclock_data_realtime_template.station_code
                FROM surge_perclock_data_realtime_template
                WHERE issue_ts >= {start_ts}
                  and issue_ts <= {end_ts}
                GROUP BY station_code
            )AS SUB
            ON SUB.station_code=MAIN.station_code AND SUB.surge_max=MAIN.surge
            WHERE issue_ts >= {start_ts}
              and issue_ts <= {end_ts}
        """)

        res = session.execute(sql_str)
        res = res.fetchall()
        res_schema: List[SurgeRealDataSchema] = []
        for temp in res:
            res_schema.append(
                SurgeRealDataSchema(station_code=temp[0], surge=temp[1], issue_ts=temp[2], issue_dt=temp[3]))
        return res_schema


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
