"""
    + 24-01-12 要素相关包
"""

from abc import ABC, abstractmethod, abstractproperty
import pathlib
from typing import Optional, List, Dict

from common.enums import ElementTypeEnum


class IElement(ABC):

    def __init__(self, element_type: ElementTypeEnum, val: str, name: str):
        """
            + 观测要素接口
        :param val:
        :param name:
        """
        self.element_type = element_type
        """观测要素种类"""
        self.element_val: str = val
        self.element_name: str = name


class WindElement(IElement):
    """
        风要素
    """

    def __init__(self):
        super().__init__(ElementTypeEnum.WIND, 'ws', '风')


class SurgeElement(IElement):
    """
        潮位要素
    """

    def __init__(self):
        super().__init__(ElementTypeEnum.SURGE, 'wl', '风')
