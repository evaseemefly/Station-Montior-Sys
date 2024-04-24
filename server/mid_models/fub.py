from typing import List, Optional, Union

from common.enums import ElementTypeEnum


class FubListMidModel:
    """
        + 24-04-22 不同浮标站点的 mid model
        对应 DistStationSurgeListSchema
    """

    def __init__(self, code: str, element_type: ElementTypeEnum, ts_list: List[int],
                 val_list: List[Union[float, int]]):
        # def __init__(self, code: str, element_type: ElementTypeEnum, ts_list: List[int],
        #              val_list: List[float]):
        self.station_code = code
        self.elemtn_type = element_type
        """要素枚举类型"""
        self.ts_list = ts_list
        """对应的时间戳"""
        self.val_list = val_list
        """时间戳对应的观测要素值"""


class DistFubListMidModel:
    def __init__(self, code: str, observation_list: List[FubListMidModel]):
        self.code = code
        self.observation_list = observation_list
