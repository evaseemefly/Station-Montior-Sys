"""
    + 24-01-12 file相关包
"""

from abc import ABC, abstractmethod, abstractproperty
import pathlib
from typing import Optional, List, Dict
from util.ftp import FtpClient
from util.common import get_store_relative_path
from common.enums import ElementTypeEnum


class IFile(ABC):
    """
        文件接口
        已废弃
    """

    def __init__(self, ftp_client: FtpClient, local_root_path: str, element_type: ElementTypeEnum,
                 remote_root_path: str = None):
        """

        :param ftp_client: ftp客户端
        :param local_root_path: 本地存储的根目录(容器对应挂载volums根目录)——绝对路径
        :param element_type:  观测要素种类
        :param remote_root_path:    ftp下载的远端对应登录后的路径(相对路径)
        """
        self.ftp_client = ftp_client
        self.local_root_path = local_root_path
        self.remote_root_path = remote_root_path
        self.element_type = element_type

    @abstractmethod
    def get_remote_path(self, ts: int) -> str:
        """
            根据传入的时间戳获取对应的远程路径
        :param ts:
        :return:
        """
        pass

    @abstractmethod
    def get_local_path(self, ts: int) -> str:
        """
            根据传入的时间获取对应的本地存储路径
        :param ts:
        :return:
        """
        pass


class FubFile(IFile):
    """
        浮标 file
        + 暂时不实现
    """

    def get_local_path(self, ts: int) -> str:
        pass

    def get_remote_path(self, ts: int) -> str:
        pass


class StationFile(IFile):
    """
        海洋站 file 实现类
    """

    def __init__(self, ftp_client: FtpClient, local_root_path: str, element_type: ElementTypeEnum, code: str,
                 remote_root_path: str = None):
        super().__init__(ftp_client, local_root_path, element_type, remote_root_path)
        self.code: str = code
        """站点code"""

    def get_remote_path(self, ts: int) -> str:
        relative_path: str = get_store_relative_path(ts)
        """存储的相对路径(yyyy/mm/dd)"""
        path = pathlib.Path(self.remote_root_path) / self.code / relative_path
        return str(path)

    def get_local_path(self, ts: int) -> str:
        relative_path: str = get_store_relative_path(ts)
        """存储的相对路径(yyyy/mm/dd)"""
        path = pathlib.Path(self.local_root_path) / self.code / relative_path
        return str(path)
