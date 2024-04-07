"""
    + 24-01-12 file相关包
"""

from abc import ABC, abstractmethod, abstractproperty
import pathlib
from typing import Optional, List, Dict

import arrow

from util.ftp import FtpClient
from util.common import get_store_relative_path, get_filestamp, get_calendarday_filestamp
from common.enums import ElementTypeEnum
from util.decorators import decorator_timer_consuming, decorator_exception_logging


class IFile(ABC):
    """
        文件接口
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
    def get_remote_path(self) -> str:
        """
            根据传入的时间戳获取对应的远程路径
        :param ts:
        :return:
        """
        pass

    @abstractmethod
    def get_relative_path(self) -> str:
        """本地与远端的存储相对路径"""
        pass

    @abstractmethod
    def get_local_path(self) -> str:
        """
            根据传入的时间获取对应的本地存储路径
        :param ts:
        :return:
        """
        pass

    @abstractmethod
    def get_file_name(self) -> str:
        pass

    @property
    def local_full_path(self) -> str:
        path = pathlib.Path(self.get_local_path()) / self.get_file_name()
        return str(path)

    @decorator_timer_consuming
    def download(self) -> bool:
        """
            TODO:[*] 24-01-17 需要实现，通过ftp下载指定文件
            注意下载至本地需要考虑若存在本地文件需要覆盖
        :return:
        """
        is_ok: bool = False
        # step1: 判断本地路径是否存在，若不存在则创建指定目录
        if pathlib.Path(self.local_full_path).parent.exists():
            pass
        else:
            pathlib.Path(self.local_full_path).parent.mkdir(parents=True, exist_ok=True)
            # step2: 将ftp远端文件 在相对路径下 下载至本地 local_full_path 中
        is_ok = self.ftp_client.down_load_file_bycwd(self.local_full_path, self.get_remote_path(),
                                                     self.get_file_name())
        return is_ok


class FubFile(IFile):
    """
        浮标 file
        + 暂时不实现
    """

    def get_local_path(self, ts: int) -> str:
        pass

    def get_remote_path(self, ts: int) -> str:
        pass


class IStationFile(IFile):
    """
        海洋站 file 实现类
    """

    def __init__(self, ftp_client: FtpClient, local_root_path: str, element_type: ElementTypeEnum, station_code: str,
                 station_name: str,
                 station_num: str,
                 ts: int,
                 remote_root_path: str = None):
        super().__init__(ftp_client, local_root_path, element_type, remote_root_path)
        self.station_code: str = station_code
        """站代码 XQS"""
        self.station_name: str = station_name
        """站名称(ch)"""
        self.station_num: str = station_num
        """站代号 08442"""
        self.ts = ts
        """当前时间戳 Int"""

    def get_remote_path(self) -> str:
        """
            相对于 self.remote_root_path 的相对路径
            remote_path:              '/test/ObsData/SHW/2024/02/20'
            实际路径: /home/nmefc/share/test/ObsData/汕尾/perclock/2024/02
        @return: '/test/ObsData/SHW/2024/02/20'
        """
        relative_path: str = get_store_relative_path(self.ts)
        """存储的相对路径(yyyy/mm/dd)"""
        path = pathlib.Path(self.remote_root_path) / self.station_name / 'perclock' / relative_path
        # TODO:[-] 24-03-27 此处若运行在win下需要手动将其转换为 linux 路径格式
        path = path.as_posix()
        return str(path)

    def get_local_path(self) -> str:
        relative_path: str = get_store_relative_path(self.ts)
        """存储的相对路径(yyyy/mm/dd)"""
        path = pathlib.Path(self.local_root_path) / self.station_name / relative_path
        return str(path)

    def get_relative_path(self) -> str:
        """
            相对于 self.remote_root_path 的相对路径
            remote_path:              '/test/ObsData/SHW/2024/02/20'
            实际路径: /home/nmefc/share/test/ObsData/汕尾/perclock/2024/02
        @return: eg: /test/ObsData/SHW/2024/02/20
        """
        relative_path: str = get_store_relative_path(self.ts)
        # TODO:[-] 24-02-26 注意实际的路径(包含:perclock)
        path = pathlib.Path(self.station_name) / 'perclock' / relative_path
        return str(path)

    @abstractmethod
    def get_file_name(self) -> str:
        """

            eg: ws0115.08442
                ws0115_DAT.08442
        :param ts:
        :return:
        """
        pass


class SurgeFile(IStationFile):
    def get_file_name(self) -> str:
        """

                    eg: WL0115.08442
                        WL0115_DAT.08442
                :param ts:
                :return:
                """
        mmdd = get_calendarday_filestamp(self.ts)
        file_name: str = f'WL{mmdd}_DAT.{self.station_num}'
        """WL0115_DAT.08442"""
        return file_name


class WindFile(IStationFile):
    def get_file_name(self) -> str:
        """

                    eg: ws0115.08442
                        ws0115_DAT.08442
                :param ts:
                :return:
                """
        mmdd = get_filestamp(self.ts)
        file_name: str = f'WS{mmdd}_DAT.{self.station_code}'
        """WS0115_DAT.08442"""
        return file_name
