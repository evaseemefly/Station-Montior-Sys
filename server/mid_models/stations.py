from typing import List, Union

from common.enums import ElementTypeEnum, ObservationTypeEnum


class StationElementMidModel:
    """
        + 24-01-17 海洋站及对应的要素集合中间model
    """

    def __init__(self, code, num, name, elements: List[ElementTypeEnum]):
        self.station_code = code
        """站点 code"""
        self.station_num = num
        """站点 代号"""
        self.station_name = name
        """站点 中文名"""
        self.elements = elements
        """该站点的要素种类集合"""


class DistStationSurgeListMidModel:
    """
        + 24-03-15 不同站点潮位集合
        对应 DistStationSurgeListSchema
    """

    def __init__(self, code: str, surge_list: List[float], ts_list: List[int]):
        self.station_code = code
        self.surge_list = surge_list
        self.ts_list = ts_list


class DistStationWindListMidModel:
    """
        + 24-03-15 不同站点潮位集合
        对应 DistStationSurgeListSchema
    """

    def __init__(self, code: str, ws_list: List[float], dir_list: List[int], ts_list: List[int]):
        self.station_code = code
        self.ws_list = ws_list
        self.dir_list = dir_list
        self.ts_list = ts_list


class StationInstanceMidModel:
    """
        + 24-05-28
        参考 mid_modlers -> FubListMidModel
    """

    def __init__(self, code: str, element_type: ElementTypeEnum, ts_list: List[int],
                 val_list: List[Union[float, int]]):
        self.station_code = code
        self.element_type = element_type
        """要素枚举类型"""
        self.ts_list = ts_list
        """对应的时间戳"""
        self.val_list = val_list
        """时间戳对应的观测要素值"""


class DistStationListMidModel:
    """
        + 24-05-28
        参考 mid_modlers -> DistFubListMidModel
    """

    def __init__(self, code: str, obs_type: ObservationTypeEnum, observation_list: List[StationInstanceMidModel]):
        self.code = code
        self.obs_type = obs_type
        self.observation_list = observation_list
