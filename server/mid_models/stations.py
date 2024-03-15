from typing import List

from common.enums import ElementTypeEnum


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
