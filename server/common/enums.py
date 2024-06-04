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

    WL = 510
    """潮位"""

    FUB = 601
    """浮标"""
    MooringBuoy = 602
    """锚系浮标"""

    LargeBuoy = 611
    """大浮标"""


@unique
class ObservationTypeEnum(Enum):
    """
        观测站点类型
    """
    """海洋站"""
    STATION = 600

    """水利部海洋站"""
    STATION_IRRIGATION = 603

    """浮标"""
    FUB = 601
