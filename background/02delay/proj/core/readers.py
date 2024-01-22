"""
    + 24-01-12 各类阅读器 包
"""

from abc import ABC, abstractmethod, abstractproperty
import pathlib
from typing import Optional, List, Dict, Any, Tuple
import arrow
import pandas as pd

from common.exceptions import FileReadError, FileFormatError
from core.files import IFile
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
    def read_file(self, full_path: str) -> Optional[Any]:
        """
            根据全路径读取文件并返回 list[dict]
        :param full_path:
        :return:
        """
        pass


class WindReader(IReader):
    """
        风 读取器
    """

    def get_wind_data_list(self):
        """
            获取风要素的数据集合
        :return:
        """
        list_wind: List[dict] = self.read_file(self.file.local_full_path)
        return list_wind

    # 读取整点数据(风场为例)
    def read_file(self, full_path: str) -> List[dict]:
        """
            读取指定路径的风要素文件
            [*] 24-01-15 需要加入异常处理
        :param full_path: 读取文件全路径
        :return:
        """
        list_wind: List[dict] = []
        # step-1: 判断指定文件是否存在
        if pathlib.Path(full_path).exists():
            # step-2: 以 gbk 格式打开指定文件
            with open(full_path, 'rb') as f:
                data = pd.read_csv(f, encoding='gbk', sep='\s+', header=None,
                                   infer_datetime_format=False)
                # step-3:获取文件的shape
                shape = data.shape
                # 总行数
                rows = data.shape[0]
                # 总列数
                columns = data.shape[1]
                if rows > 0:
                    # 日期
                    # eg: 20231217
                    #     yyyy-mm-dd
                    dt_str: str = str(int(data.iloc[0][0]))
                    # 设置起始时间(utc)
                    # xxxx 12:00(utc)
                    dt_start_utc: arrow.Arrow = arrow.Arrow(dt_str, 'yyyymmdd').add(hour=-12)
                    # 站点起始时间为昨天的20点(local)
                    realdata_series: pd.Series = data.iloc[0][1:]

                    step = 2
                    for index in range(24):
                        # print(index)
                        temp_dt_arrow: arrow.Arrow = dt_start_utc.add(hour=1)
                        temp_ts: int = temp_dt_arrow.int_timestamp
                        temp_wd = realdata_series[index * step + 1]
                        temp_ws = realdata_series[index * step + 2]
                        temp_dict = {'index': index, 'ts': temp_ts, 'wd': temp_wd, 'ws': temp_ws}
                        list_wind.append(temp_dict)
        return list_wind


class SurgeReader(IReader):
    """
        潮位 读取器
    """

    def __init__(self, file: IFile):

        super().__init__(file)
        self.realdata_list, self.extremum_list = self.get_surge_data_list()

    def get_realdata_list(self):
        pass

    def get_surge_data_list(self) -> Tuple[List[dict], List[dict]]:
        """

        :return:
        """
        res_dict = self.read_file(self.file.local_full_path)
        list_realdata_surge: List[dict] = res_dict['extremum']
        """站点实况集合(整点潮位值)"""
        list_extremum_surge: List[dict] = res_dict['realdata']
        """站点每日极值集合"""
        return list_realdata_surge, list_extremum_surge

    def read_file(self, full_path: str) -> Optional[dict]:
        """
            获取 00-23 H 的实况潮位，以及该日的所有极值
        :param full_path:
        :return:{’extremum‘:list[dict],'realdata':list[dcit]}
        """
        res_dict: Optional[dict] = None
        list_surge: List[dict] = []
        """实况潮位集合 {ts:int,surge:float}"""
        list_surge_extremum: List[dict] = []
        """潮位极值集合 {ts:int,surge:float}"""
        # step-1: 判断指定文件是否存在
        if pathlib.Path(full_path).exists():
            # step-2: 以 gbk 格式打开指定文件
            try:
                with open(full_path, 'rb') as f:
                    data = pd.read_csv(f, encoding='gbk', sep='\s+', header=None,
                                       infer_datetime_format=False)
                    # step-3:获取文件的shape
                    shape = data.shape
                    """tuple 读取 dataframe 形状"""

                    rows = shape[0]
                    """总行数"""
                    # 总列数
                    columns = data.shape[1]
                    if rows > 0:
                        # 日期
                        # eg: 20231217
                        #     yyyy-mm-dd
                        dt_str: str = str(int(data.iloc[0][0]))
                        date_local = arrow.Arrow(dt_str, 'yyyymmdd')
                        # 设置起始时间(utc)
                        # xxxx 12:00(utc)
                        dt_start_utc: arrow.Arrow = arrow.Arrow(dt_str, 'yyyymmdd').shift(hours=-8)
                        # 站点起始时间为昨天的20点(local)
                        series_surge: pd.Series = data.iloc[0][1:25]
                        for index in range(24):
                            # print(index)
                            # TODO:[*] 24-01-15 此处需要加入是否为缺省值的判断
                            temp_dt_arrow: arrow.Arrow = dt_start_utc.shift(hours=1)
                            temp_ts: int = temp_dt_arrow.int_timestamp
                            temp_surge = series_surge[index + 1]
                            temp_dict = {'ts': temp_ts, 'surge': temp_surge}
                            list_surge.append(temp_dict)
                        res_dict['realdata'] = list_surge
                        # 获取当日极值情况
                        series_extremum: pd.Series = data.iloc[0][25:]
                        """ 当日的潮位极值 series """
                        count_extremum = series_extremum.count()

                        """ 当日潮位极值对应series 的长度 """
                        """
                           0 608
                           1 148
                           2 115
                           3 906
                           4 619
                           5 1517
                           6 248
                           7 2140
                           8 9999
                           9 9999
                        """
                        for index, val in enumerate(series_extremum):
                            temp_str: str = val
                            # 能被2整除(包含0)说明为时间位
                            # 缺省值会是 9999
                            if index % 2 == 0 and val != 9999:
                                temp_time_str: str = temp_str
                                # '608'.rjust(4,'0')
                                # '0608'
                                temp_dt_format: str = f'{date_local.format("yyyymmdd")}{temp_time_str.rjust(4, "0")}'
                                # 转换为utc
                                temp_dt: arrow.Arrow = arrow.Arrow(temp_dt_format).shift(hours=-8)
                                temp_ts: int = temp_dt.int_timestamp
                                temp_dict: dict = {'ts': temp_ts, 'surge': val}
                                list_surge_extremum.append(temp_dict)
                        res_dict['extremum'] = list_surge_extremum
                    else:
                        raise FileFormatError(f"文件:{full_path}shape异常")

            except Exception:
                raise FileReadError(f"读取:{full_path}错误")
        else:
            # 容易不存在抛出文件不存在异常
            raise FileExistsError(f"{full_path}文件不存在")
        return res_dict
