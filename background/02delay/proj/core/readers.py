"""
    + 24-01-12 各类阅读器 包
"""
import logging
from abc import ABC, abstractmethod, abstractproperty
import pathlib
from typing import Optional, List, Dict, Any, Tuple
import arrow
import pandas as pd

from common.exceptions import FileReadError, FileFormatError
from core.files import IFile
from mid_models.elements import WindExtremum
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

    def __init__(self, file: IFile):

        super().__init__(file)
        self.list_wind: List[dict] = []
        """风要素实况集合 :List[{'index': index, 'ts': temp_ts, 'wd': temp_wd, 'ws': temp_ws}]"""
        self.extremum: WindExtremum = None
        """风要素极值"""
        self.max: WindExtremum = None
        """风要素最大值"""
        self.assign_wind_data_list()
        pass

    def assign_wind_data_list(self):
        """
            获取风要素的数据集合
            并为当前reader中的变量赋值
        :return:
        """
        list_wind, extremum_dict = self.read_file(self.file.local_full_path)
        self.list_wind = list_wind
        self.extremum = extremum_dict['extremum']
        self.max = extremum_dict['max']

    # 读取整点数据(风场为例)
    def read_file(self, full_path: str) -> Tuple[List[dict], dict]:
        """
            读取指定路径的风要素文件
            [*] 24-01-15 需要加入异常处理
        :param full_path: 读取文件全路径
        :return: list_wind: list[{'index': index, 'ts': temp_ts, 'wd': temp_wd, 'ws': temp_ws}]
                 extremum_val_dict: 'extremum':WindExtremum , 'max':WindExtremum
        """
        list_wind: List[dict] = []
        extremum_val_dict: dict = {}
        omit_list: List[float, int] = [999, 9998, 9999]
        # step-1: 判断指定文件是否存在
        if pathlib.Path(full_path).exists():
            # step-2: 以 gbk 格式打开指定文件
            try:
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
                        # eg: 2023 12 17
                        #     yyyy-mm-dd
                        dt_str: str = str(int(data.iloc[0][0]))

                        """风向极值字典 extremum-极值;max-最大值"""
                        # 设置起始时间(utc)
                        # xxxx 12:00(utc)
                        # 风要素起始时间为前一日21点
                        # local:21-8=utc:13
                        dt_start_utc: arrow.Arrow = arrow.get(dt_str, 'YYYYMMDD').shift(hours=-11)
                        # 站点起始时间为昨天的20点(local)
                        realdata_series: pd.Series = data.iloc[0][1:]

                        step = 2
                        for index in range(24):
                            # print(index)
                            temp_dt_arrow: arrow.Arrow = dt_start_utc.shift(hours=index)
                            temp_ts: int = temp_dt_arrow.int_timestamp
                            temp_wd = realdata_series[index * step + 1]
                            temp_ws = realdata_series[index * step + 2]
                            if temp_ws in omit_list or temp_wd in omit_list:
                                pass
                            else:
                                temp_dict = {'index': index, 'ts': temp_ts, 'wd': temp_wd, 'ws': temp_ws}
                                list_wind.append(temp_dict)
                        if rows > 3:
                            extremum_row = data.iloc[3]
                            """极值出现的行"""
                            extremum_val: float = extremum_row.iloc[0]
                            """极值"""
                            extremum_dir: float = extremum_row.iloc[1]
                            """极值对应的风向"""
                            extremum_hhdd: float = extremum_row.iloc[2]
                            extremum_hhdd_str: str = str(int(extremum_hhdd)).rjust(4, '0')
                            if extremum_val in omit_list or extremum_dir in omit_list:
                                pass
                            else:
                                extremum_dt_str = f'{dt_str}{extremum_hhdd_str}'
                                """极值对应的出现时间"""
                                extremum_dt_utc: arrow.Arrow = arrow.get(extremum_dt_str, 'YYYYMMDDhhmm').shift(
                                    hours=-8)
                                extremum_ts: int = extremum_dt_utc.int_timestamp

                                extremum_val_dict['extremum'] = WindExtremum(extremum_val, extremum_dir, extremum_ts)
                            # -----------------
                            max_row = data.iloc[2]
                            """最大值出现的行"""
                            max_val: float = max_row.iloc[0]
                            """最大值"""
                            max_dir: float = max_row.iloc[1]
                            """最大值对应的风向"""

                            max_hhdd: float = max_row.iloc[2]
                            """最大之出现时间"""
                            # TODO:[*] 24-04-09 由于 使用 serise.iloc[index] 读取后的某列数值为 float，需要转换为 str
                            # 例如原始数据为: 0747 -> 747.0 -> 0747 需要向左侧填充4位0
                            max_hhdd_str: str = str(int(max_hhdd)).rjust(4, '0')
                            if max_val in omit_list or max_dir in omit_list:
                                pass
                            else:
                                max_dt_str = f'{dt_str}{max_hhdd_str}'
                                """最大值对应的出现时间"""
                                max_dt_utc: arrow.Arrow = arrow.get(max_dt_str, 'YYYYMMDDhhmm').shift(hours=-8)
                                max_ts: int = max_dt_utc.int_timestamp
                                extremum_val_dict['max'] = WindExtremum(max_val, max_dir, max_ts)
                    else:
                        raise FileFormatError(f"文件:{full_path}shape异常")
            except Exception:
                raise FileReadError(f"读取:{full_path}错误")
        return list_wind, extremum_val_dict

    def get_extremum_list(self):
        pass


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
        # TODO:[*] 24-02-28 对于远端文件名小写，本地下载后统一为大写的此处会出错
        # '/Users/evaseemefly/03data/02station/南澳/2024/02/20/WL0220_DAT.09710'
        res_dict = self.read_file(self.file.local_full_path)
        list_realdata_surge: List[dict] = res_dict['realdata']
        """站点实况集合(整点潮位值)"""
        list_extremum_surge: List[dict] = res_dict['extremum']
        """站点每日极值集合"""
        return list_realdata_surge, list_extremum_surge

    def read_file(self, full_path: str) -> Optional[dict]:
        """
            获取 00-23 H 的实况潮位，以及该日的所有极值
        :param full_path:
        :return:{’extremum‘:list[dict],'realdata':list[dcit]}
        """
        res_dict: Optional[dict] = {}
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
                        date_local = arrow.get(dt_str, 'YYYYMMDD')
                        # TODO:[*] 24-04-03 整点潮位数据起始时间为 Localtime 00时，此处需要修改
                        hh_utc_str: str = '16'
                        dt_utc_str: str = f'{dt_str}{hh_utc_str}'
                        # 设置起始时间(utc)
                        # xxxx 12:00(utc)
                        # 读取时间有误，应为 24-8=16
                        dt_start_utc: arrow.Arrow = arrow.get(dt_utc_str, 'YYYYMMDDHH').shift(days=-1)
                        # 站点起始时间为昨天的20点(local)
                        series_surge: pd.Series = data.iloc[0][1:25]
                        """获取当日实况情况(series)"""
                        list_surge_realdata: List[dict] = self._read_realdata(dt_start_utc, series_surge)
                        """站点实况集合"""
                        series_extremum: pd.Series = data.iloc[0][25:]
                        """获取当日极值情况(series)"""
                        list_surge_extremum: List[dict] = self._read_extremum(dt_start_utc, series_extremum)
                        """站点极值集合"""
                        res_dict['realdata'] = list_surge_realdata
                        res_dict['extremum'] = list_surge_extremum
                    else:
                        raise FileFormatError(f"文件:{full_path}shape异常")

            except Exception:
                raise FileReadError(f"读取:{full_path}错误")
        else:
            # 容易不存在抛出文件不存在异常
            raise FileExistsError(f"{full_path}文件不存在")
        return res_dict

    def read_file_back(self, full_path: str) -> Optional[dict]:
        """
            获取 00-23 H 的实况潮位，以及该日的所有极值
            备份: 不再使用!
        :param full_path:
        :return:{’extremum‘:list[dict],'realdata':list[dcit]}
        """
        res_dict: Optional[dict] = {}
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
                        date_local = arrow.get(dt_str, 'YYYYMMDD')
                        # 设置起始时间(utc)
                        # xxxx 12:00(utc)
                        dt_start_utc: arrow.Arrow = arrow.get(dt_str, 'YYYYMMDD').shift(hours=-8)
                        # 站点起始时间为昨天的20点(local)
                        series_surge: pd.Series = data.iloc[0][1:25]
                        for index in range(24):
                            # print(index)
                            # TODO:[*] 24-01-15 此处需要加入是否为缺省值的判断
                            temp_dt_arrow: arrow.Arrow = dt_start_utc.shift(hours=index)
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
                           0 608    值
                           1 148    时间
                           2 115    值
                           3 906    时间
                           4 619
                           5 1517
                           6 248
                           7 2140
                           8 9999
                           9 9999
                        """
                        """

                        """
                        for index, val in enumerate(series_extremum):
                            temp_str: str = str(val)
                            # 能被2整除(包含0)说明为时间位
                            # 缺省值会是 9999
                            # 0,2,4,6
                            if index % 2 == 0 and val != 9999:
                                # 注意时间在对应数值的后面
                                temp_time_str: str = str(series_extremum.iloc[index + 1])
                                # '608'.rjust(4,'0')
                                # '0608'
                                temp_dt_format: str = f'{date_local.format("YYYYMMDD")}{temp_time_str.rjust(4, "0")}'
                                # 转换为utc
                                temp_dt: arrow.Arrow = arrow.get(temp_dt_format, "YYYYMMDDhhmm").shift(hours=-8)
                                temp_ts: int = temp_dt.int_timestamp
                                """对应的极值的时间戳"""
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

    def _read_realdata(self, dt: arrow.Arrow, series_list: pd.Series) -> List[dict]:
        """
            读取实况 serise
        @param dt: 起始的世界时
        @param series_list:
        @return:
        """
        list_dict: List[dict] = []
        dt_start_utc: arrow.Arrow = dt
        for index in range(24):
            try:
                # TODO:[*] 24-01-15 此处需要加入是否为缺省值的判断
                temp_dt_arrow: arrow.Arrow = dt_start_utc.shift(hours=index)
                temp_ts: int = temp_dt_arrow.int_timestamp
                temp_surge = series_list[index + 1]
                temp_dict = {'ts': temp_ts, 'surge': temp_surge}
                list_dict.append(temp_dict)
            except Exception:
                logging.error(f'读取实况集合出错')
        return list_dict

    def _read_extremum(self, dt: arrow.Arrow, series_list: pd.Series) -> List[dict]:
        """
            读取极值 series
            TODO:[*] 24-02-29 读取极值会将时间往前推一天
        @param dt: 起始的世界时
        @param series_list:
        @return:
        """
        list_dict: List[dict] = []
        dt_start_utc: arrow.Arrow = dt
        """文件起始时间utc"""
        dt_start_local: arrow.Arrow = dt_start_utc.shift(hours=8)
        """文件起始时间localtime"""
        # 获取当日极值情况
        series_extremum: pd.Series = series_list
        """ 当日的潮位极值 series """
        count_extremum = series_extremum.count()

        """ 当日潮位极值对应series 的长度 """
        """
           0 608    值
           1 148    时间
           2 115    值
           3 906    时间
           4 619
           5 1517
           6 248
           7 2140
           8 9999
           9 9999
        """
        """

        """
        for index, val in enumerate(series_extremum):
            temp_str: str = str(val)
            # 能被2整除(包含0)说明为时间位
            # 缺省值会是 9999
            # 0,2,4,6
            try:
                if index % 2 == 0 and val != 9999:
                    # 注意时间在对应数值的后面
                    temp_time_str: str = str(series_extremum.iloc[index + 1])
                    # '608'.rjust(4,'0')
                    # '0608'
                    # TODO:[*] 24-02-29 注意此处有一个隐藏的问题:由于整点报文为 20H -> 次日19H(localtime),此处需要将 utc -> localtime
                    temp_dt_format: str = ''
                    """当前极值对应的时间字符串 YYYYMMDDHH"""

                    # TODO:[*] 24-02-29 localtime: [20H,23H][2000,2359] 直接拼接即可; localtime:[ 0000,1959] day+1 再拼接
                    if int(temp_time_str) >= 2000 and int(temp_time_str) <= 2359:
                        temp_dt_format: str = f'{dt_start_local.format("YYYYMMDD")}{temp_time_str.rjust(4, "0")}'
                    elif int(temp_time_str) >= 0 and int(temp_time_str) <= 1959:
                        temp_dt_format: str = f'{dt_start_local.shift(days=1).format("YYYYMMDD")}{temp_time_str.rjust(4, "0")}'

                    # 转换为utc
                    temp_dt_utc: arrow.Arrow = arrow.get(temp_dt_format, "YYYYMMDDhhmm").shift(hours=-8)
                    temp_ts: int = temp_dt_utc.int_timestamp
                    """对应的极值的时间戳"""
                    temp_dict: dict = {'ts': temp_ts, 'surge': val}
                    list_dict.append(temp_dict)
            except Exception as ex:
                logging.error(f'读取极值集合出错!')
        return list_dict
