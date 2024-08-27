from typing import List, Optional, Any
from datetime import datetime
import arrow
from sqlalchemy import select, update, func, and_, text, TextClause, union_all, MetaData, Table, Column, String, \
    Integer, Float, Date
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import func

from db.db import session_scope
from mid_models.stations import DistStationWindListMidModel
from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel, get_table, WindPerclockDataModel
from schema.region import RegionSchema
from schema.station_surge import SurgeRealDataSchema

from dao.base import BaseDao


class StationWindDao(BaseDao):
    def get_all_stations_realdata_list(self, start_ts: int, end_ts: int) -> List[DistStationWindListMidModel]:
        """
            根据起止时间获取所有站点的实况集合
        :param start_ts: 起始时间戳
        :param end_ts: 结束时间戳
        :return:
        """
        # session = self.db.session

        tab_name: str = WindPerclockDataModel.get_split_tab_name(start_ts)

        # TODO:[-] 24-04-10 注意此处需要加入跨表的设计
        sql_str: text = text(f"""
                    SELECT station_code,
                        group_concat(issue_ts  order by issue_ts) as issue_ts_list,
                        group_concat(ws  order by issue_ts) as ws_list,
                        group_concat(wd  order by issue_ts) as wd_list
                        FROM {tab_name}
                        WHERE {tab_name}.issue_ts >= {start_ts}
                          AND {tab_name}.issue_ts <= {end_ts}
                        GROUP BY station_code
                """)
        with session_scope() as session:
            res = session.execute(sql_str)

            # TODO:[-] 24-04-07 此处加入动态获取表名以及是否需要动态跨表查询
            # is_need: bool = WindPerclockDataModel.check_needsplittab(start_ts, end_ts)
            """是否需要分表查询"""

            dist_station_wind_list: List[DistStationWindListMidModel] = []
            for temp in res:
                temp_code: str = temp.station_code
                temp_wd_str_list: List[str] = temp.wd_list.split(',')
                temp_wd_list: List[int] = []
                for temp_wd_str in temp_wd_str_list:
                    if temp_wd_str != '' or temp_wd_str != ',':
                        temp_wd_list.append(int(temp_wd_str))
                temp_ws_str_list: List[str] = temp.ws_list.split(',')
                temp_ws_list: List[float] = []
                for temp_ws_str in temp_ws_str_list:
                    if temp_ws_str != '' or temp_ws_str != ',':
                        temp_ws_list.append(float(temp_ws_str))

                temp_ts_str_list: List[str] = temp.issue_ts_list.split(',')
                temp_ts_list: List[int] = []
                for temp_ts_str in temp_ts_str_list:
                    if temp_ts_str != '':
                        temp_ts_list.append(int(temp_ts_str))
                temp_tide_middelmodel: DistStationWindListMidModel = DistStationWindListMidModel(code=temp_code,
                                                                                                 ws_list=temp_ws_list,
                                                                                                 dir_list=temp_wd_list,

                                                                                                 ts_list=temp_ts_list)
                dist_station_wind_list.append(temp_tide_middelmodel)
            # session.close()
        return dist_station_wind_list
