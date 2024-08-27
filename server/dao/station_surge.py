from typing import List, Optional, Any
from datetime import datetime
import arrow
from sqlalchemy import select, update, func, and_, text, TextClause, union_all, MetaData, Table, Column, String, \
    Integer, Float, Date
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import func

from db.db import session_scope
from mid_models.stations import DistStationSurgeListMidModel
from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel, get_table
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
        # session: Session = self.db.session
        station_code: str = kwargs.get('station_code')
        """站点code"""
        start_ts: int = kwargs.get('start_ts')
        """起始ts"""
        end_ts: int = kwargs.get('end_ts')
        """结束ts(可空)"""
        if end_ts is None:
            # 未传入结束时间，按照start_ts+24h赋值
            end_ts = start_ts + 24 * 60 * 60
        with session_scope() as session:
            stmt = select(SurgePerclockDataModel).where(SurgePerclockDataModel.station_code == station_code,
                                                        SurgePerclockDataModel.issue_ts >= start_ts,
                                                        SurgePerclockDataModel.issue_ts <= end_ts).order_by(
                SurgePerclockDataModel.issue_ts)

            res = session.execute(stmt).scalars().all()
        # session.close()
        return res

    def get_stations_realdata_by_params(self, **kwargs):
        """
            获取对应 station_codes的实况
        :param kwargs:
        :return:
        """
        # session: Session = self.db.session
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
        with session_scope() as session:
            for temp_code in station_codes:
                stmt = select(SurgePerclockDataModel).where(SurgePerclockDataModel.station_code == temp_code,
                                                            SurgePerclockDataModel.issue_ts >= start_ts,
                                                            SurgePerclockDataModel.issue_ts <= end_ts).order_by(
                    SurgePerclockDataModel.issue_ts)
                temp_res = session.execute(stmt).scalars().all()
                res.extend(temp_res)
        # session.close()
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
        # session: Session = self.db.session
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
        with session_scope() as session:
            res = session.execute(sql_str)
            res = res.fetchall()
            res_schema: List[SurgeRealDataSchema] = []
            for temp in res:
                res_schema.append(
                    SurgeRealDataSchema(station_code=temp[0], surge=temp[1], issue_ts=temp[2], issue_dt=temp[3]))

        # session.close()
        return res_schema

    def get_all_stations_realdata_list(self, start_ts: int, end_ts: int) -> List[DistStationSurgeListMidModel]:
        """
            获取所有站点的时间范围内的实况集合
        :param start_ts:
        :param end_ts:
        :return:
        """
        # session = self.db.session

        tab_name: str = SurgePerclockDataModel.get_split_tab_name(start_ts)

        with session_scope() as session:
            sql_str: text = text(f"""
                SELECT station_code,
                    group_concat(issue_ts  order by issue_ts) as issue_ts_list,
                    group_concat(surge  order by issue_ts) as surge_list
                    FROM {tab_name}
                    WHERE {tab_name}.issue_ts >= {start_ts}
                      AND {tab_name}.issue_ts <= {end_ts}
                    GROUP BY station_code
            """)
            res = session.execute(sql_str)

            # TODO:[*] 24-04-07 此处加入动态获取表名以及是否需要动态跨表查询
            is_need: bool = SurgePerclockDataModel.check_needsplittab(start_ts, end_ts)
            """是否需要分表查询"""
            if is_need:
                # TODO:[*] 24-04-07 此处需要检验
                tab_start_name: str = SurgePerclockDataModel.get_split_tab_name(start_ts)
                tab_end_name: str = SurgePerclockDataModel.get_split_tab_name(end_ts)

                sql_start_tab_str: text = text(f"""
                SELECT station_code,
                    group_concat(issue_ts  order by issue_ts) as issue_ts_list,
                    group_concat(surge  order by issue_ts) as surge_list
                    FROM {tab_start_name}
                    WHERE {tab_start_name}.issue_ts >= {start_ts}
                      AND {tab_start_name}.issue_ts <= {end_ts}
                    GROUP BY station_code
            """)
                res_start = session.execute(sql_start_tab_str)

                sql_end_tab_str: text = text(f"""
                            SELECT station_code,
                                group_concat(issue_ts  order by issue_ts) as issue_ts_list,
                                group_concat(surge  order by issue_ts) as surge_list
                                FROM {tab_end_name}
                                WHERE {tab_end_name}.issue_ts >= {start_ts}
                                  AND {tab_end_name}.issue_ts <= {end_ts}
                                GROUP BY station_code
                        """)
                res_end = session.execute(sql_end_tab_str)
                dist_station_surge_lists_merge: List[List[DistStationSurgeListMidModel]] = []
                dist_station_surge_list_finally: List[DistStationSurgeListMidModel] = []
                """两个数组 index=0 为 start part;index=1 为 end part"""
                # 对以上两个 res根据code进行拼接
                for index, res in enumerate([res_start, res_end]):
                    dist_station_surge_list: List[DistStationSurgeListMidModel] = []
                    """start 或 end part"""
                    for temp_start in res:
                        temp_code: str = temp_start.station_code
                        temp_surge_str_list: List[str] = temp_start.surge_list.split(',')
                        temp_surge_list: List[float] = []
                        for temp_surge_str in temp_surge_str_list:
                            if temp_surge_str != '' or temp_surge_str != ',':
                                temp_surge_list.append(float(temp_surge_str))
                        # 再从 res_end 中过滤出指定station_code

                        temp_ts_str_list: List[str] = temp_start.issue_ts_list.split(',')
                        temp_ts_list: List[int] = []
                        for temp_ts_str in temp_ts_str_list:
                            if temp_ts_str != '':
                                temp_ts_list.append(int(temp_ts_str))

                        temp_tide_middelmodel: DistStationSurgeListMidModel = DistStationSurgeListMidModel(
                            code=temp_code,
                            surge_list=temp_surge_list,
                            ts_list=temp_ts_list)
                        dist_station_surge_list.append(temp_tide_middelmodel)
                    dist_station_surge_lists_merge.append(dist_station_surge_list)
                for temp in dist_station_surge_lists_merge[0]:
                    temp_code: str = temp.station_code
                    for filter_temp in dist_station_surge_lists_merge[1]:
                        if filter_temp.station_code == temp_code:
                            temp.surge_list.extend(filter_temp.surge_list)
                            temp.ts_list.extend(filter_temp.ts_list)
                            dist_station_surge_list_finally.append(temp)
                pass
                # 循环后需要根据 station_code 进行拼接生成最终的 集合

                # 以下有错误，注释掉
                # metadata = MetaData()
                # table = Table(tab_start_name, metadata,
                #               Column('station_code', String),
                #               Column('surge', Integer),
                #               Column('issue_ts', Integer),
                #               Column('issue_dt', String)
                #               )
                # main_table = table
                # subquery = get_table(tab_end_name)
                # tabs = [main_table, subquery]

                # 构建主查询语句
                # stmt = select([
                #     main_table.c.station_code,
                #     main_table.c.surge,
                #     main_table.c.issue_ts.label('ts'),
                #     main_table.c.issue_dt.label('dt')
                # ]).select_from(main_table.join(subquery, (subquery.c.station_code == main_table.c.station_code) & (
                #         subquery.c.surge_max == main_table.c.surge))).where(
                #     (main_table.c.issue_ts >= start_ts) &
                #     (main_table.c.issue_ts <= end_ts)
                # )
            else:
                pass
                tab_name: str = SurgePerclockDataModel.get_split_tab_name(start_ts)
                metadata = MetaData()
                # main_table = Table(tab_name, metadata,
                #                    Column('station_code', String),
                #                    Column('surge', Float),
                #                    Column('issue_ts', Integer),
                #                    Column('issue_dt', Date)
                #                    )
                # 获取动态表对象
                main_table = get_table(tab_name)
                # 构建子查询语句
                # TODO:[*] 24-04-07 出现错误:
                #
                """
                        raise exc.ArgumentError(msg, code=code) from err
    sqlalchemy.exc.ArgumentError: Column expression,
     FROM clause, or other columns clause element expected, 
     got [Column('station_code', VARCHAR(length=10), table=<surge_perclock_data_realtime_2024>, nullable=False), 
     Column('surge', DOUBLE(asdecimal=True), table=<surge_perclock_data_realtime_2024>, nullable=False)].
      Did you mean to say select(Column('station_code', VARCHAR(length=10), table=<surge_perclock_data_realtime_2024>,
       nullable=False), 
    Column('surge', DOUBLE(asdecimal=True), table=<surge_perclock_data_realtime_2024>, nullable=False))?
                """
                # TODO:[*] 24-04-07 方法2 直接使用 model，而非动态映射
                # subquery = select([main_table.c.station_code, main_table.c.surge]).where(
                #     (main_table.c.issue_ts >= start_ts) &
                #     (main_table.c.issue_ts <= end_ts)
                # ).group_by(main_table.c.station_code).alias("SUB")

                # 使用以下stmt的方式提示有错误，放弃
                # stmt = select([
                #     main_table.c.station_code,
                #     func.group_concat(main_table.c.issue_ts),
                #     func.group_concat(main_table.c.surge)
                # ]).where(
                #     (main_table.c.issue_ts >= start_ts) &
                #     (main_table.c.issue_ts <= end_ts)
                # ).group_by(main_table.c.station_code)
                pass

                # SurgePerclockDataModel.set_split_tab_name(start_ts)
                # ERROR: sqlalchemy.exc.ArgumentError: Column expression, FROM clause, or other columns clause element expected, got [<sqlalchemy.orm.attributes.InstrumentedAttribute object at 0x000001F0C2A2F188>,
                # subquery = select([SurgePerclockDataModel.station_code, SurgePerclockDataModel.surge]).where(
                #     (SurgePerclockDataModel.issue_ts >= start_ts) &
                #     (SurgePerclockDataModel.issue_ts <= end_ts)
                # ).group_by(SurgePerclockDataModel.station_code).alias("SUB")

                # subquery = select(SurgePerclockDataModel).where(
                #     (SurgePerclockDataModel.issue_ts >= start_ts) &
                #     (SurgePerclockDataModel.issue_ts <= end_ts)
                # )
                #
                # # 构建主查询语句
                # # stmt = select([
                # #     main_table.c.station_code,
                # #     main_table.c.surge,
                # #     main_table.c.issue_ts.label('ts'),
                # #     main_table.c.issue_dt.label('dt')
                # # ]).select_from(main_table.join(subquery, (subquery.c.station_code == main_table.c.station_code) & (
                # #         subquery.c.surge == main_table.c.surge))).where(
                # #     (main_table.c.issue_ts >= start_ts) &
                # #     (main_table.c.issue_ts <= end_ts)
                # # )
                # stmt = select(
                #     SurgePerclockDataModel).select_from(
                #     SurgePerclockDataModel.join(subquery,
                #                                 (subquery.c.station_code == SurgePerclockDataModel.station_code) & (
                #                                         subquery.c.surge == SurgePerclockDataModel.surge))).where(
                #     (SurgePerclockDataModel.issue_ts >= start_ts) &
                #     (SurgePerclockDataModel.issue_ts <= end_ts)
                # )
            dist_station_surge_list: List[DistStationSurgeListMidModel] = []
            for temp in res:
                temp_code: str = temp.station_code
                temp_surge_str_list: List[str] = temp.surge_list.split(',')
                temp_surge_list: List[float] = []
                for temp_surge_str in temp_surge_str_list:
                    if temp_surge_str != '' or temp_surge_str != ',':
                        temp_surge_list.append(float(temp_surge_str))

                temp_ts_str_list: List[str] = temp.issue_ts_list.split(',')
                temp_ts_list: List[int] = []
                for temp_ts_str in temp_ts_str_list:
                    if temp_ts_str != '':
                        temp_ts_list.append(int(temp_ts_str))
                temp_tide_middelmodel: DistStationSurgeListMidModel = DistStationSurgeListMidModel(code=temp_code,
                                                                                                   surge_list=temp_surge_list,
                                                                                                   ts_list=temp_ts_list)
                dist_station_surge_list.append(temp_tide_middelmodel)
            # session.close()
        return dist_station_surge_list


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
        # session: Session = self.db.session
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

        # TODO:[-] 24-04-07 此处加入动态获取表名以及是否需要动态跨表查询
        is_need: bool = SurgePerclockDataModel.check_needsplittab(start_ts, end_ts)
        """是否需要分表查询"""

        with session_scope() as session:
            if is_need:
                tab_start_name: str = SurgePerclockDataModel.get_split_tab_name(start_ts)
                tab_end_name: str = SurgePerclockDataModel.get_split_tab_name(end_ts)
                model_start = get_table(tab_start_name)
                model_end = get_table(tab_end_name)
                tabs = [model_start, model_end]
                stmts = []
                for tab_temp in tabs:
                    stmt = select([tab_temp]).where(
                        tab_temp.c.station_code == [station_codes[0]],
                        tab_temp.c.issue_ts >= start_ts,
                        tab_temp.c.issue_ts <= end_ts).order_by(
                        tab_temp.c.issue_ts)
                    stmts.append(stmt)
                # 组合多个查询结果
                combined_stmt = union_all(*stmts)
                pass
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
        # session: Session = self.db.session
        station_code: str = kwargs.get('station_code')
        """站点code"""
        start_ts: int = kwargs.get('start_ts')
        """起始ts"""
        end_ts: int = kwargs.get('end_ts')
        """结束ts(可空)"""
        if end_ts is None:
            # 未传入结束时间，按照start_ts+24h赋值
            end_ts = start_ts + 24 * 60 * 60
        with session_scope() as session:
            stmt = select(SurgePerclockExtremumDataModel).where(
                SurgePerclockExtremumDataModel.station_code == station_code,
                SurgePerclockExtremumDataModel.issue_ts >= start_ts,
                SurgePerclockExtremumDataModel.issue_ts <= end_ts).order_by(
                SurgePerclockExtremumDataModel.issue_ts)
            res = session.execute(stmt).scalars().all()
            # session.close()
        return res

    def get_one_extreme_maximum(self, **kwargs):
        """
            获取单站极值model集合
        :param kwargs:station_code,start_ts,end_ts
        :return:
        """
        # session: Session = self.db.session
        station_code: str = kwargs.get('station_code')
        """站点code"""
        start_ts: int = kwargs.get('start_ts')
        """起始ts"""
        end_ts: int = kwargs.get('end_ts')
        """结束ts(可空)"""
        if end_ts is None:
            # 未传入结束时间，按照start_ts+24h赋值
            end_ts = start_ts + 24 * 60 * 60
        with session_scope() as session:
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
            # session.close()
        return res

    def get_many_extreme_maximum(self, **kwargs):
        """
            获取多个站点的极值最大值(指定时间范围内)集合
        :param kwargs: station_codes,start_ts,end_ts
        :return:
        """
        # session: Session = self.db.session
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
        with session_scope() as session:
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
            # session.close()
        return res

    def get_dist_extreme_maximum(self, start_ts: int, end_ts: int) -> List[SurgeRealDataSchema]:
        """
            + 24-03-19 获取所有站点的每日高高潮/低高潮集合
        :param start_ts:
        :param end_ts:
        :return:
        """
        # session = self.db.session

        """
        step1: sql 查询
            SELECT MAIN.station_code, MAIN.surge, MAIN.issue_dt,MAIN.issue_dt
            FROM surge_perclock_data_extremum_template AS MAIN
            JOIN (
                SELECT MAX(surge) as surge_max,surge_perclock_data_extremum_template.station_code
                FROM surge_perclock_data_extremum_template
                WHERE issue_ts >= 1708344000
                  and issue_ts <= 1708426800
                GROUP BY station_code
            )AS SUB
            ON SUB.station_code=MAIN.station_code AND SUB.surge_max=MAIN.surge
            WHERE issue_ts >= 1708344000
              and issue_ts <= 1708426800
        step2: 提取所有 station_code
        step3: 封装至 -> station_code:xx                        
        """
        # TODO:[-] 24-03-12 此处需要将 table name 修改为动态表名
        tab_name: str = SurgePerclockExtremumDataModel.get_tab_name()
        """表名"""
        sql_str: text = text(f"""
            SELECT MAIN.station_code, MAIN.surge, MAIN.issue_ts as ts,MAIN.issue_dt as dt
            FROM {tab_name} AS MAIN
            JOIN (
                SELECT MAX(surge) as surge_max,{tab_name}.station_code
                FROM {tab_name}
                WHERE issue_ts >= {start_ts}
                  and issue_ts <= {end_ts}
                GROUP BY station_code
            )AS SUB
            ON SUB.station_code=MAIN.station_code AND SUB.surge_max=MAIN.surge
            WHERE issue_ts >= {start_ts}
              and issue_ts <= {end_ts}
        """)
        with session_scope() as session:
            res = session.execute(sql_str)

            res_schema: List[SurgeRealDataSchema] = []
            for temp in res:
                res_schema.append(
                    SurgeRealDataSchema(station_code=temp[0], surge=temp[1], issue_ts=temp[2], issue_dt=temp[3]))
            # session.close()
        return res_schema
