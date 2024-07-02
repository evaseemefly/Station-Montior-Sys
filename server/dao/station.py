from typing import List, Optional, Any, Dict
import json

import arrow
import numpy as np
import pandas as pd

from common.default import DEFAULT_SURGE, NAN_VAL
from common.enums import ElementTypeEnum, ObservationTypeEnum
from mid_models.stations import DistStationListMidModel, StationInstanceMidModel
from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel, WindPerclockDataModel, \
    get_wind_instance_model, get_surge_instance_model
from schema.station import StationRegionSchema
from schema.station_status import StationStatusAndGeoInfoSchema
from sqlalchemy import select, update, func, and_, text
from sqlalchemy import union, union_all
from sqlalchemy.orm import Session
from dao.base import BaseDao
from schema.station_surge import AstronomicTideSchema, DistStationTideListSchema
from util.common import get_diff_timestamp_list

from util.consul import ConsulExtractClient

CONSUL_SERVICE_NAME = 'station-base'
"""注册服务的名称——站点静态服务"""

consul_client = ConsulExtractClient(CONSUL_SERVICE_NAME)
"""consul客户端"""


# 以 consul 实现服务发现 调用被注册服务的接口
def get_remote_service(uri: str, params: dict):
    """
        通过相对 url 路径 url 获取对应远端接口
    :param uri:
    :param params:
    :return:
    """
    return consul_client.get(uri, params=params)


class StationBaseDao(BaseDao):
    """
        站点基础信息 dao (访问台风预报系统)
    """

    def get_dist_station_code(self, **kwargs) -> set:
        """
            获取不同的站点 code set
        @param kwargs:
        @return:
        """
        # 此处替换为使用 consul 获取动态服务地址并调用获取结果
        # target_url: str = f'http://128.5.10.21:8000/station/station/all/list'
        # res = requests.get(target_url)
        # res_content: str = res.content.decode('utf-8')
        res_content = get_remote_service('/station/all/list', {})

        # [{'id': 4, 'code': 'SHW', 'name': '汕尾', 'lat': 22.7564, 'lon': 115.3572, 'is_abs': False, 'sort': -1,
        #  'is_in_common_use': True}]
        list_region_dict: List[Dict] = json.loads(res_content)
        list_region: List[StationRegionSchema] = []
        for region_dict in list_region_dict:
            list_region.append(StationRegionSchema.parse_obj(region_dict))
        # 针对code 进行去重操作
        list_codes: List[str] = [station.code for station in list_region]
        return set(list_codes)

    def get_dist_station_list(self, **kwargs) -> List[Dict]:
        """
            + 23-11-17 获取所有站点基础信息字典集合
        @param kwargs:
        @return:
        """
        res_content = get_remote_service('/station/all/list', {})
        return json.loads(res_content)

    def get_dist_region(self, **kwargs) -> List[str]:
        """
            获取不同的行政区划 code
        @param kwargs:
        @return:
        """

    def get_target_astronomictide(self, code: str, start_ts: int, end_ts: int) -> List[AstronomicTideSchema]:
        """
            获取指定站点的天文潮
            step1: 获取指定站点的 [start,end] 范围内的天文潮集合(间隔1h)
        @param code: 站点
        @param start_ts: 起始时间戳
        @param end_ts: 结束时间戳
        @return:
        """
        # target_url: str = f'http://128.5.10.21:8000/station/station/astronomictide/list'
        start_arrow: arrow.Arrow = arrow.get(start_ts)
        end_arrow: arrow.Arrow = arrow.get(end_ts)
        start_dt_str: str = f"{start_arrow.format('YYYY-MM-DDTHH:mm:ss')}Z"
        end_dt_str: str = f"{end_arrow.format('YYYY-MM-DDTHH:mm:ss')}Z"
        # 注意时间格式 2023-07-31T16:00:00Z
        # res = requests.get(target_url,
        #                    data={'station_code': code, 'start_dt': start_dt_str, 'end_dt': end_dt_str})
        # res = requests.get(target_url,
        #                    params={'station_code': code, 'start_dt': start_dt_str, 'end_dt': end_dt_str})
        # res_content: str = res.content.decode('utf-8')
        # TODO:*] 23-11-17 加载指定站点的天文潮集合
        res_content: str = get_remote_service('/station/astronomictide/list',
                                              params={'station_code': code, 'start_dt': start_dt_str,
                                                      'end_dt': end_dt_str})
        # {'station_code': 'CGM', 'forecast_dt': '2023-07-31T17:00:00Z', 'surge': 441.0}
        # 天文潮字典集合
        list_tide_dict: List[Dict] = json.loads(res_content)
        # 天文潮 schema 集合
        list_tide: List[AstronomicTideSchema] = []
        for tide_dict in list_tide_dict:
            list_tide.append(AstronomicTideSchema.parse_obj(tide_dict))
        return list_tide

    def get_dist_station_tide_list(self, start_ts: int, end_ts: int) -> List[DistStationTideListSchema]:
        """
            + 23-08-16
            获取所有站点 [start,end] 范围内的 天文潮+时间 集合
        @param start_ts:
        @param end_ts:
        @return:
        """
        start_arrow: arrow.Arrow = arrow.get(start_ts)
        end_arrow: arrow.Arrow = arrow.get(end_ts)
        start_dt_str: str = f"{start_arrow.format('YYYY-MM-DDTHH:mm:ss')}Z"
        end_dt_str: str = f"{end_arrow.format('YYYY-MM-DDTHH:mm:ss')}Z"
        # TODO:[*] 23-11-17 修改为通过服务发现调用接口
        res_content: str = get_remote_service('/station/dist/astronomictide/list',
                                              params={'start_dt': start_dt_str, 'end_dt': end_dt_str})
        # {'station_code': 'CGM', 'forecast_dt': '2023-07-31T17:00:00Z', 'surge': 441.0}
        # 天文潮字典集合
        list_tide_dict: List[Dict] = json.loads(res_content)
        list_tide: List[DistStationTideListSchema] = []
        for temp in list_tide_dict:
            list_tide.append(DistStationTideListSchema.parse_obj(temp))
        return list_tide

    def get_dist_station_astronomictide_list(self, start_ts: int, end_ts: int) -> List[DistStationTideListSchema]:
        """
            通过 consul 调用remote中的天文潮集合
        :param start_ts:
        :param end_ts:
        :return:
        """
        start_arrow: arrow.Arrow = arrow.get(start_ts)
        end_arrow: arrow.Arrow = arrow.get(end_ts)
        start_dt_str: str = f"{start_arrow.format('YYYY-MM-DDTHH:mm:ss')}Z"
        end_dt_str: str = f"{end_arrow.format('YYYY-MM-DDTHH:mm:ss')}Z"
        # TODO:[*] 23-11-17 修改为通过服务发现调用接口
        res_content: str = get_remote_service('/station/dist/astronomictide/list',
                                              params={'start_dt': start_dt_str, 'end_dt': end_dt_str})
        # {'station_code': 'CGM', 'forecast_dt': '2023-07-31T17:00:00Z', 'surge': 441.0}
        # 天文潮字典集合
        list_tide_dict: List[Dict] = json.loads(res_content)
        list_tide: List[DistStationTideListSchema] = []
        for temp in list_tide_dict:
            list_tide.append(DistStationTideListSchema.parse_obj(temp))
        return list_tide

    def get_stations_realdata_list(self, codes: List[str], start_ts: int, end_ts: int) -> List[DistStationListMidModel]:
        """
            + 24-05-29
            根据传入的 codes 获取起止时间范围内的所有观测要素的集合(海洋站)
            TODO:[*] 24-06-04 后续问题需要加入可配置的观测实况选项，目前是加载 wd,ws,wl;后续会有其他观测数据
        :param codes:
        :param start_ts:
        :param end_ts:
        :return:
        """
        session = self.db.session
        elements: List[ElementTypeEnum] = [ElementTypeEnum.WD, ElementTypeEnum.WS, ElementTypeEnum.WL]
        """要素枚举集合"""
        tab_name: str = WindPerclockDataModel.get_split_tab_name(start_ts)
        station_obserivation_list: List[DistStationListMidModel] = []
        """被查询海洋站的观测数据集合"""
        # TODO:[*] 24-06-18 应加入基于起止时间的时间戳集合范围标准化（对于缺省值进行填充） 时间戳单位(s)
        list_ts_standard: List[int] = get_diff_timestamp_list(start_ts, end_ts)

        for temp_code in codes:
            # step 2-1: 获取海洋站风要素实况
            # 获取指定站点的风要素(ws,wd)

            # # TODO:[*] 24-05-28 注意使用 stmt.union_all 的方式拼接另一个 stmt表达式
            # res_wind_next_ = session.execute(wind_stmt_next_).scalars().all()
            # 循环第二次时:
            # sqlalchemy.exc.InvalidRequestError: Table 'wind_perclock_data_realtime_2023' is already defined for this MetaData instance.
            # Specify 'extend_existing=True' to redefine options and columns on an existing Table object.
            query_model_ = get_wind_instance_model(session, start_ts)
            """风要素model实体"""
            wind_stmt_ = select(query_model_.ws, query_model_.wd, query_model_.station_code,
                                query_model_.issue_ts).where(query_model_.station_code == temp_code,
                                                             query_model_.issue_ts >= start_ts,
                                                             query_model_.issue_ts <= end_ts).order_by(
                query_model_.issue_ts)
            """根据起始时间戳的stmt表达式"""
            combined_stmt = wind_stmt_
            """合并后的查询stmt表达式(可能跨表)"""

            # 若起止时间涉及跨年的情况，需要跨表查询
            if arrow.get(start_ts).date().year != arrow.get(end_ts).date().year:
                query_model_next_ = get_wind_instance_model(session, end_ts)
                """获取跨表的model实体"""
                wind_stmt_next_ = select(query_model_next_.ws, query_model_next_.wd,
                                         query_model_next_.station_code,
                                         query_model_next_.issue_ts).where(
                    query_model_next_.station_code == temp_code,
                    query_model_next_.issue_ts >= start_ts,
                    query_model_next_.issue_ts <= end_ts).order_by(
                    query_model_next_.issue_ts)
                """针对跨表的查询表达式"""
                combined_stmt = union_all(wind_stmt_, wind_stmt_next_)

            # TODO:[-] 24-05-30 若使用 union 拼接两个子查询结果，只输出第一列——此查询中为 ws ,是查询导致的，不要使用 .scalars().all() 。使用 .all()
            # eg: [(9.1, 11, 'BYQ', 1708347600), (8.7, 15, 'BYQ', 1708351200), ..]
            combined_res_wind = session.execute(combined_stmt).all()
            """风要素查询结果(含跨表查询)"""
            res_wind_next_ = []
            temp_ts_list: List[int] = []
            """时间戳集合"""
            temp_wd_list: List[int] = []
            """风要素-风向集合"""
            temp_ws_list: List[float] = []
            """风要素-风速集合"""

            # TODO:[*] 24-06-19 以下内容使用列表推导替代
            # 根据查询结果生成风要素数据集
            # for temp_wind_ in combined_res_wind:
            #     """
            #         query_model_.c.ws,              0
            #         query_model_.c.wd,              1
            #         query_model_.c.station_code,    2
            #         query_model_.c.issue_ts         3
            #     """
            #     # fub_realdata_list: List[DistStationListMidModel] = []
            #     temp_ts_: int = temp_wind_[3]
            #     """时间戳"""
            #     temp_wd_: int = temp_wind_[1]
            #     """风向"""
            #     temp_ws_: float = temp_wind_[0]
            #     """风速"""
            #     temp_ts_list.append(temp_ts_)
            #     temp_wd_list.append(temp_wd_)
            #     temp_ws_list.append(temp_ws_)
            # step1-2 :针对风要素生成对应的数组
            temp_wind_ts_list = [i[3] for i in combined_res_wind]
            """风要素对应的ts数组"""
            temp_wd_list = [i[1] for i in combined_res_wind]
            temp_ws_list = [i[0] for i in combined_res_wind]

            # step1-3: 生成风要素df
            combined_res_wind_df = pd.DataFrame({"ts": temp_wind_ts_list, 'wd': temp_wd_list, 'ws': temp_ws_list})
            # step1-4: 重置索引
            combined_res_wind_df.set_index("ts", inplace=True)
            aligned_res_wind_df = combined_res_wind_df.reindex(list_ts_standard, fill_value=NAN_VAL)
            wd_standard_list = aligned_res_wind_df['wd'].tolist()
            ws_standard_lis = aligned_res_wind_df['ws'].tolist()

            # step 2-2: 获取海洋站潮位要素实况
            query_surge_model_ = get_surge_instance_model(session, start_ts)
            surge_stmt_ = select(query_surge_model_.surge, query_surge_model_.station_code,
                                 query_surge_model_.issue_ts).where(query_surge_model_.station_code == temp_code,
                                                                    query_surge_model_.issue_ts >= start_ts,
                                                                    query_surge_model_.issue_ts <= end_ts).order_by(
                query_surge_model_.issue_ts)
            combined_surge_stmt = surge_stmt_
            if arrow.get(start_ts).date().year != arrow.get(end_ts).date().year:
                query_model_next_ = get_surge_instance_model(session, end_ts)
                surge_stmt_next_ = select(query_surge_model_.surge, query_surge_model_.station_code,
                                          query_surge_model_.issue_ts).where(
                    query_surge_model_.station_code == temp_code,
                    query_surge_model_.issue_ts >= start_ts,
                    query_surge_model_.issue_ts <= end_ts).order_by(
                    query_surge_model_.issue_ts)
                combined_surge_stmt = union_all(surge_stmt_, surge_stmt_next_)
            # TODO:[*] 24-05-30 若使用 union 拼接两个子查询结果，只输出第一列——此查询中为 ws ,是查询导致的，不要使用 .scalars().all() 。使用 .all()
            # eg: [(9.1, 11, 'BYQ', 1708347600), (8.7, 15, 'BYQ', 1708351200), ..]
            combined_res_surge = session.execute(combined_surge_stmt).all()
            temp_ts_list: List[int] = []
            temp_surge_list: List[float] = []

            """
                [(Decimal('277.0000000000'), 'WFG', 1708387200), ]
            """
            list_res_ts: List[int] = [i[2] for i in combined_res_surge]
            """基于combined_res_surge的 issue_ts 生成的时间戳集合"""
            list_res_surge: List[float] = [i[0] for i in combined_res_surge]
            """基于combined_res_surge的 surge 生成的增水集合"""
            combined_res_df = pd.DataFrame({"ts": list_res_ts, 'surge': list_res_surge})
            # 设置ts为index
            combined_res_df.set_index("ts", inplace=True)
            # 以生成的时间戳集合 list_ts 为基准，进行缺失值填充
            # aligned_res_df = combined_res_df.reindex(list_ts_standard, fill_value=np.NaN)
            # aligned_res_df = combined_res_df.reindex(list_ts_standard, fill_value=-999.9)
            # TODO:[*] 24-06-19 若对于缺省值使用 np.NaN 或 None 填充，填充后为 nan ，序列化时会出现 ValueError: Out of range float values are not JSON compliant 错误
            # 序列化出现的错误，建议不使用通过默认值进行填充
            aligned_res_df = combined_res_df.reindex(list_ts_standard, fill_value=NAN_VAL)
            # list 结果为: ['nan', 'nan', Decimal('157.0000000000')
            temp_surge_list = aligned_res_df['surge'].tolist()
            """索引采用时间戳集合作为index(list_ts)，存在nan的情况"""

            # for temp_surge_ in combined_res_surge:
            #     """
            #         query_model_.c.surge,           0
            #         query_model_.c.station_code,    1
            #         query_model_.c.issue_ts         2
            #     """
            #     # fub_realdata_list: List[DistStationListMidModel] = []
            #     temp_ts_: int = temp_surge_[2]
            #     """时间戳"""
            #     temp_surge_: float = temp_surge_[0]
            #     """风速"""
            #     temp_ts_list.append(temp_ts_)
            #     temp_surge_list.append(temp_surge_)

            """根据 start_ts , end_ts 生成的时间戳"""
            temp_station_wd_: StationInstanceMidModel = StationInstanceMidModel(code=temp_code,
                                                                                element_type=ElementTypeEnum.WD,
                                                                                ts_list=list_ts_standard,
                                                                                val_list=wd_standard_list)
            temp_station_ws_: StationInstanceMidModel = StationInstanceMidModel(code=temp_code,
                                                                                element_type=ElementTypeEnum.WS,
                                                                                ts_list=list_ts_standard,
                                                                                val_list=ws_standard_lis)
            temp_station_surge_: StationInstanceMidModel = StationInstanceMidModel(code=temp_code,
                                                                                   element_type=ElementTypeEnum.WL,
                                                                                   ts_list=list_ts_standard,
                                                                                   val_list=temp_surge_list)
            temp_station_: DistStationListMidModel = DistStationListMidModel(code=temp_code,
                                                                             obs_type=ObservationTypeEnum.STATION,
                                                                             observation_list=[temp_station_ws_,
                                                                                               temp_station_wd_,
                                                                                               temp_station_surge_])
            station_obserivation_list.append(temp_station_)

        return station_obserivation_list

    def get_stations_realdata_list_backup(self, codes: List[str], start_ts: int, end_ts: int) -> List[
        DistStationListMidModel]:
        """
            + 24-05-30
            不使用 union_all 拼接——不使用此方法
        :param codes:
        :param start_ts:
        :param end_ts:
        :return:
        """
        session = self.db.session
        elements: List[ElementTypeEnum] = [ElementTypeEnum.WD, ElementTypeEnum.WS, ElementTypeEnum.WL]
        """要素枚举集合"""
        tab_name: str = WindPerclockDataModel.get_split_tab_name(start_ts)
        station_obserivation_list: List[DistStationListMidModel] = []
        """被查询海洋站的观测数据集合"""

        for temp_code in codes:
            # step 2-1: 获取海洋站风要素实况
            # 获取指定站点的风要素(ws,wd)

            # # TODO:[*] 24-05-28 注意使用 stmt.union_all 的方式拼接另一个 stmt表达式
            # res_wind_next_ = session.execute(wind_stmt_next_).scalars().all()
            query_model_ = get_wind_instance_model(session, start_ts)
            wind_stmt_ = select(query_model_.c.ws, query_model_.c.wd, query_model_.c.station_code,
                                query_model_.c.issue_ts).where(query_model_.station_code == temp_code,
                                                               query_model_.issue_ts >= start_ts,
                                                               query_model_.issue_ts <= end_ts).order_by(
                query_model_.issue_ts)
            query_model_next_ = get_wind_instance_model(session, end_ts)
            wind_stmt_next_ = select(query_model_next_.c.ws, query_model_next_.c.wd, query_model_next_.c.station_code,
                                     query_model_next_.c.issue_ts).where(query_model_next_.station_code == temp_code,
                                                                         query_model_next_.issue_ts >= start_ts,
                                                                         query_model_next_.issue_ts <= end_ts).order_by(
                query_model_next_.issue_ts)
            # TODO:[*] 24-05-30 若使用 union 拼接两个子查询结果，只输出第一列——此查询中为 ws ,是查询导致的，不要使用 .scalars().all() 。使用 .all()
            combined_stmt = union_all(wind_stmt_, wind_stmt_next_)
            combined_res_wind = session.execute(combined_stmt).all()
            res_wind_next_ = []
            if arrow.get(start_ts).date().year != arrow.get(end_ts).date().year:
                query_model_next_ = get_wind_instance_model(end_ts)
                wind_stmt_next_ = select(query_model_next_).where(query_model_next_.station_code == temp_code,
                                                                  query_model_next_.issue_ts >= start_ts,
                                                                  query_model_next_.issue_ts <= end_ts).order_by(
                    query_model_next_.issue_ts)
                res_wind_next_ = session.execute(wind_stmt_next_).scalars().all()
            # TODO:[*] 24-05-29 将 res 与 res_next_ 拼接
            wind_res_combined = res_wind_ + res_wind_next_

            temp_ts_list: List[int] = []
            temp_wd_list: List[int] = []
            temp_ws_list: List[float] = []
            for temp_wind_ in wind_res_combined:
                # fub_realdata_list: List[DistStationListMidModel] = []
                temp_ts_: int = temp_wind_.issue_ts
                """时间戳"""
                temp_wd_: int = temp_wind_.wd
                """风向"""
                temp_ws_: float = temp_wind_.ws
                """风速"""
                temp_ts_list.append(temp_ts_)
                temp_wd_list.append(temp_wd_)
                temp_ws_list.append(temp_ws_)

            # step 2-2: 获取海洋站潮位要素实况
            SurgePerclockDataModel.set_split_tab_name(start_ts)
            surge_stmt = select(SurgePerclockDataModel).where(SurgePerclockDataModel.station_code == temp_code,
                                                              SurgePerclockDataModel.issue_ts >= start_ts,
                                                              SurgePerclockDataModel.issue_ts <= end_ts).order_by(
                SurgePerclockDataModel.issue_ts)
            res_surge = session.execute(surge_stmt).scalars().all()
            res_surge_next_ = []
            if arrow.get(start_ts).date().year != arrow.get(end_ts).date().year:
                SurgePerclockDataModel.set_split_tab_name(end_ts)
                surge_stmt_next_ = select(SurgePerclockDataModel).where(
                    SurgePerclockDataModel.station_code == temp_code,
                    SurgePerclockDataModel.issue_ts >= start_ts,
                    SurgePerclockDataModel.issue_ts <= end_ts).order_by(
                    SurgePerclockDataModel.issue_ts)
                res_surge_next_ = session.execute(surge_stmt_next_).scalars().all()
                pass
            # res = session.execute(wind_sql_str)
            res_surge_combined = res_surge + res_surge_next_
            temp_surge_ts_list: List[int] = []
            temp_surge_list: List[float] = []
            for temp_surge_ in res_surge_combined:
                # fub_realdata_list: List[DistStationListMidModel] = []
                temp_ts_: int = temp_surge_.issue_ts
                """时间戳"""
                temp_ws_: float = temp_surge_.surge
                """风速"""
                temp_surge_ts_list.append(temp_ts_)
                temp_surge_list.append(temp_ws_)

            temp_station_wd_: StationInstanceMidModel = StationInstanceMidModel(code=temp_code,
                                                                                element_type=ElementTypeEnum.WD,
                                                                                ts_list=temp_ts_list,
                                                                                val_list=temp_wd_list)
            temp_station_ws_: StationInstanceMidModel = StationInstanceMidModel(code=temp_code,
                                                                                element_type=ElementTypeEnum.WS,
                                                                                ts_list=temp_ts_list,
                                                                                val_list=temp_ws_list)
            temp_station_surge_: StationInstanceMidModel = StationInstanceMidModel(code=temp_code,
                                                                                   element_type=ElementTypeEnum.WL,
                                                                                   ts_list=temp_ts_list,
                                                                                   val_list=temp_surge_list)
            temp_station_: DistStationListMidModel = DistStationListMidModel(code=temp_code,
                                                                             obs_type=ObservationTypeEnum.STATION,
                                                                             observation_list=[temp_station_ws_,
                                                                                               temp_station_wd_,
                                                                                               temp_station_surge_])
            station_obserivation_list.append(temp_station_)

        return station_obserivation_list
