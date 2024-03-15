from typing import List, Optional, Any, Dict
import json

import arrow

from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel
from schema.station import StationRegionSchema
from schema.station_status import StationStatusAndGeoInfoSchema
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from dao.base import BaseDao
from schema.station_surge import AstronomicTideSchema, DistStationTideListSchema

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
