"""
    + 24-01-17 各类操作类，操作 reader,file,store等类
"""

from abc import ABC, abstractmethod, abstractproperty
import pathlib
from typing import Optional, List, Dict, Any, Tuple
import arrow
import pandas as pd

from common.exceptions import FtpDownLoadError
from core.files import IFile, IStationFile
from core.readers import SurgeReader
from core.storers import SurgeStore
from util.decorators import decorator_timer_consuming


class IOperater(ABC):
    def __init__(self, file: IStationFile):
        self.file = file

    @abstractmethod
    def todo(self, **kwargs):
        pass


class WindOperate(IOperater):
    def todo(self, **kwargs):
        pass


class SurgeOperate(IOperater):
    """
        潮位操作类 完成 文件下载 -> 读取 -> 写入数据库的操作
    """

    @decorator_timer_consuming
    def todo(self, **kwargs):
        ts = kwargs.get('ts')
        # step1: 下载文件
        if self.file.download():
            # step2: 读取文件(实际不需要)
            surge_reader = SurgeReader(self.file)
            realdata_list: List[dict] = surge_reader.realdata_list
            """站点实况集合"""
            extremum_list: List[dict] = surge_reader.extremum_list
            """站点极值集合"""
            # step3: 写入数据库持久化保存(reader在此步骤中完成)
            SurgeStore(self.file).to_db(realdata_list=realdata_list, extremum_list=extremum_list, ts=ts)
        else:
            # TODO:[-] 24-01-19 抛出异常
            raise FtpDownLoadError()
