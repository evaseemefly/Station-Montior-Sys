from enum import Enum, unique


@unique
class ElementTypeEnum(Enum):
    """
        观测要素种类枚举
        501-风
        502-潮
    """
    WIND = 501
    """风(风向|风速)"""
    SURGE = 502
    """潮位(潮位)"""
    AT = 503
    """气温"""


@unique
class ExtremumType(Enum):
    """
        极值美剧
    """
    WIND_EXTREMUM = 801
    """风速极值"""

    WIND_MAX = 802
    """风速最大值"""
