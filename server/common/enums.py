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
