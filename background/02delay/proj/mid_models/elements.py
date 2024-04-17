from typing import Union, List

from common.enums import ElementTypeEnum


class WindExtremum:
    """
        风要素极值
    """

    def __init__(self, val: float, dir: float, ts: int):
        """

        @param val:
        @param dir:
        @param ts:
        """
        self.val = val
        """风要素极值"""
        self.dir = dir
        """极值风向"""
        self.ts = ts
        """极值出现时间"""


class FubElementMidModel:

    def __init__(self, element_type: ElementTypeEnum, val: Union[float, int]):
        """
            浮标各观测要素
        @param element_type:
        @param val:
        """
        self.element_type: ElementTypeEnum = element_type
        """观测要素种类"""
        self.val: Union[float, int] = val
        """对应的观测值"""


class FubMidModel:
    """
        浮标middle model
    """

    def __init__(self, code: str, ts: int, list_vals: List[FubElementMidModel]):
        self.code = code
        self.ts = ts
        self.list_vals = list_vals
