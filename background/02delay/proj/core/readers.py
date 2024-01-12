"""
    + 24-01-12 各类阅读器 包
"""

from abc import ABC, abstractmethod, abstractproperty
import pathlib
from typing import Optional, List, Dict

from core.file import IFile
from util.ftp import FtpClient
from util.common import get_store_relative_path
from common.enums import ElementTypeEnum


class IReader(ABC):
    """
        阅读器接口
    """

    def __init__(self, file: IFile):
        self.file = file
        """被读取的文件"""

    @abstractmethod
    def read_file(self):
        pass


class WindReader(IReader):
    """
        风 读取器
    """

    def read_file(self):
        pass


class SurgeReader(IReader):
    """
        潮位 读取器
    """

    def read_file(self):
        pass
