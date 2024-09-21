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


@unique
class RegionGroupEnum(Enum):
    """
        站点归属(海洋局|水利部)
    """
    HAIYANG = 1001
    """海洋局"""

    SHUILI = 1002
    """水利部"""


@unique
class RunTypeEnmum(Enum):
    """
        执行 task 类型
    """

    DELATY_TASK = 101
    """延时任务"""
    #
    DATAENTRY_STATION_RANGE = 102
    """录入海洋站(时间段)实况"""

    DATAENTRY_SLB_RANGE = 103
    """录入水利部(时间段)实况"""

    DATAENTRY_FUB_RANGE = 104
    """录入浮标(时间段)实况"""

    DELATY_FUB_TASK = 105
    """延时任务"""

    DELATY_SLB_TASK = 106
    """定时处理水利部站点"""

    DELATY_STATIN_DAILY_TASK = 111
    """每日补录海洋站定时任务"""

    DELATY_FUB_DAILY_TASK = 115
    """每日补录FUB定时任务"""

    DELATY_SLB_DAILY_TASK = 116
    """每日补录SLB定时任务"""
