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
    WS = 504
    """风速"""
    WD = 505
    """风向"""
    WSM = 506
    """最大风速"""
    BP = 507
    """气压"""
    BG = 508
    """平均波高"""
    YBG = 509
    """有效波高"""

    FUB = 601
    """浮标"""


@unique
class ExtremumType(Enum):
    """
        极值美剧
    """
    WIND_EXTREMUM = 801
    """风速极值"""

    WIND_MAX = 802
    """风速最大值"""
