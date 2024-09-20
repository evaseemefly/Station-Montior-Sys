"""
    + 24-01-17 各类 作业案例 case 集合
"""

from abc import ABC, abstractmethod, abstractproperty
import pathlib
from typing import Optional, List, Dict, Any, Tuple
import arrow
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger

from common.const import LIST_STATIONS, LIST_SLB_STATIONS, LIST_FUBS
from common.enums import ElementTypeEnum
from common.exceptions import FtpDownLoadError, FileReadError, ReadataStoreError
from conf._privacy import FTP_LIST
from conf.settings import DOWNLOAD_OPTIONS
from core.files import IFile, IStationFile
from core.operaters import IOperater

from mid_models.stations import StationElementMidModel
from util.common import get_station_start_ts, get_fub_start_ts
from util.factory import factory_get_station_file, factory_get_operater
from util.ftp import FtpClient
from util.decorators import decorator_timer_consuming, decorator_exception_logging


class ICase(ABC):

    def __init__(self):
        self.ftp_client: FtpClient = self._init_ftp_client()
        pass

    @abstractmethod
    def _init_ftp_client(self) -> FtpClient:
        """
            初始化 ftp client
        :return:
        """
        pass

    @abstractmethod
    def todo(self, **kwargs):
        """
            kwargs: ts : 起始时间戳
            kwargs: local_root_path : 本地存储根目录
            kwargs: remote_root_path : ftp远程根目录
        @param kwargs:
        @return:
        """
        pass


class StationRealdataDownloadCase(ICase):
    # def __init__(self):
    #     self.ftp_client: FtpClient = self._init_ftp_client()

    def _init_ftp_client(self) -> FtpClient:
        """
            初始化 ftp client
        :return:
        """
        ftp_opt = FTP_LIST.get('STATION_REALDATA')
        host = ftp_opt.get('HOST')
        port = ftp_opt.get('PORT')
        user_name: str = ftp_opt.get('USER')
        pwd: str = ftp_opt.get('PWD')
        ftp_client = FtpClient(host, port)
        ftp_client.login(user_name, pwd)
        return ftp_client

    # @decorator_exception_logging
    def todo(self, **kwargs):
        """

        @param kwargs: ts : 起始时间戳sds
                      local_root_path : 本地存储根目录
                      remote_root_path : ftp远程根目录
        @return:
        """
        # step1: 根据当前的触发时间，以及海洋站集合获取对应的站点集合|每个站点对应的要素
        """
            {'code':[要素a,要素b],,,}
        """
        # TODO:[*] 24-08-05 此部分放在 ./common/const.py 中
        list_station = LIST_STATIONS
        ftp = self.ftp_client
        ts = kwargs.get('ts')

        """世界时"""
        local_root_path: str = kwargs.get('local_root_path')
        remote_root_path: str = kwargs.get('remote_root_path')
        current_dt_str: str = arrow.get(ts).format('YYYY-MM-DD HH:mm:ssZ')

        # step2:根据集合遍历执行批量 下载 -> 读取 -> to db -> 删除操作
        for val_station in list_station:
            # logger.info(f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}')
            for val_element in val_station.elements:
                logger.info(
                    f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{current_dt_str}|要素:{val_element.value}')
                # TODO"[*] 24-07-29 在此处加入异常处理
                try:
                    # TODO:[-] 24-02-29 修改为根据传入时间获取对应的统一时间戳(起止时间)
                    stand_start_ts: int = get_station_start_ts(ts, val_element)
                    # step2-1:根据工厂方法获取当前要素对应的文件
                    cls = factory_get_station_file(val_element)
                    """ 文件类"""
                    # TODO:[*] 24-07-24 可将 file 实例同意push至数组中，并加入判断是否存在指定数组的操作，
                    file_instance: IStationFile = cls(ftp, local_root_path, val_element, val_station.station_code,
                                                      val_station.station_name,
                                                      val_station.station_num, stand_start_ts,
                                                      remote_root_path)
                    """ 文件类实例化对象"""
                    # step2-2:文件下载
                    # step2-3:文件读取
                    # step2-4:文件写入db
                    # 以上均在操作类中实现
                    cls_operate = factory_get_operater(val_element)
                    """操作类"""
                    instance_operate: IOperater = cls_operate(file_instance)
                    """ 操作类实例化对象"""
                    instance_operate.todo(ts=stand_start_ts)
                # TODO:[*] 24-07-29 加入异常集
                except FtpDownLoadError as ftpEx:
                    logger.info(
                        f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}|要素:{val_element.value}出现:下载异常')
                except FileReadError as readerEx:
                    logger.info(
                        f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}|要素:{val_element.value}出现:数据读取异常')
                except ReadataStoreError as storeEx:
                    logger.info(
                        f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}|要素:{val_element.value}出现:数据写入db异常')
                except Exception as ex:
                    logger.info(
                        f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}|要素:{val_element.value}出现:未知异常——f{ex.args}')

            pass
        pass


class SLBStationRealdataDownloadCase(ICase):
    """
        TODO:[-] 24-08-02 加入的处理水利部站点的case
    """

    # def __init__(self):
    #     self.ftp_client: FtpClient = self._init_ftp_client()

    def _init_ftp_client(self) -> FtpClient:
        """
            初始化 ftp client
        :return:
        """
        ftp_opt = FTP_LIST.get('SLB_REALDATA')
        host = ftp_opt.get('HOST')
        port = ftp_opt.get('PORT')
        user_name: str = ftp_opt.get('USER')
        pwd: str = ftp_opt.get('PWD')
        ftp_client = FtpClient(host, port)
        ftp_client.login(user_name, pwd)
        return ftp_client

    # @decorator_exception_logging
    def todo(self, **kwargs):
        """

        @param kwargs: kwargs:ts : 起始时间戳sds
                      local_root_path : 本地存储根目录
                      remote_root_path : ftp远程根目录
        @return:
        """
        # step1: 根据当前的触发时间，以及海洋站集合获取对应的站点集合|每个站点对应的要素
        """
            {'code':[要素a,要素b],,,}
        """
        # TODO:[*] 24-02-27 注意例如站点不存在某个要素的整点数据，例如潮位数据WL 莆田不存在，则会报错
        # 水利部的站点目前只获取 潮位数据(总潮位)
        list_station: List[StationElementMidModel] = LIST_SLB_STATIONS
        ftp = self.ftp_client
        ts = kwargs.get('ts')
        # TODO:[-] 24-02-29 修改为根据传入时间获取对应的统一时间戳(起止时间)
        stand_start_ts: int = get_station_start_ts(ts)
        """世界时"""
        local_root_path: str = kwargs.get('local_root_path')
        remote_root_path: str = kwargs.get('remote_root_path')

        # step2:根据集合遍历执行批量 下载 -> 读取 -> to db -> 删除操作
        for val_station in list_station:
            # logger.info(f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}')
            for val_element in val_station.elements:
                logger.info(
                    f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}|要素:{val_element.value}')
                # TODO"[*] 24-07-29 在此处加入异常处理
                try:
                    # step2-1:根据工厂方法获取当前要素对应的文件
                    cls = factory_get_station_file(val_element)
                    """ 文件类"""
                    # TODO:[*] 24-07-24 可将 file 实例同意push至数组中，并加入判断是否存在指定数组的操作，
                    file_instance: IStationFile = cls(ftp, local_root_path, val_element, val_station.station_code,
                                                      val_station.station_name,
                                                      val_station.station_num, stand_start_ts,
                                                      remote_root_path)
                    """ 文件类实例化对象"""
                    # step2-2:文件下载
                    # step2-3:文件读取
                    # step2-4:文件写入db
                    # 以上均在操作类中实现
                    cls_operate = factory_get_operater(val_element)
                    """操作类"""
                    instance_operate: IOperater = cls_operate(file_instance)
                    """ 操作类实例化对象"""
                    instance_operate.todo(ts=stand_start_ts)
                # TODO:[*] 24-07-29 加入异常集
                except FtpDownLoadError as ftpEx:
                    logger.info(
                        f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}|要素:{val_element.value}出现:下载异常')
                except FileReadError as readerEx:
                    logger.info(
                        f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}|要素:{val_element.value}出现:数据读取异常')
                except ReadataStoreError as storeEx:
                    logger.info(
                        f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}|要素:{val_element.value}出现:数据写入db异常')
                except Exception as ex:
                    logger.info(
                        f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}|要素:{val_element.value}出现:未知异常——f{ex.args}')

            pass
        pass


class FubRealdataDownloadCase(ICase):
    # def __init__(self):
    #     self.ftp_client: FtpClient = self._init_ftp_client()

    def _init_ftp_client(self) -> FtpClient:
        """
            初始化 ftp client
        :return:
        """
        ftp_opt = FTP_LIST.get('STATION_REALDATA')
        host = ftp_opt.get('HOST')
        port = ftp_opt.get('PORT')
        user_name: str = ftp_opt.get('USER')
        pwd: str = ftp_opt.get('PWD')
        ftp_client = FtpClient(host, port)
        ftp_client.login(user_name, pwd)
        return ftp_client

    def todo(self, **kwargs):
        """
                24-07-24: 浮标数据是每个小时一个观测数据集
                * 注意与海洋站不同
                kwargs: ts : 起始时间戳sds
                kwargs: local_root_path : 本地存储根目录
                kwargs: remote_root_path : ftp远程根目录
        @param kwargs:ts : 起始时间戳sds
                      local_root_path : 本地存储根目录
                      remote_root_path : ftp远程根目录
        @return:
        """
        # TODO:[*] 24-02-27 注意例如站点不存在某个要素的整点数据，例如潮位数据WL 莆田不存在，则会报错
        list_station: List[StationElementMidModel] = LIST_FUBS
        ftp = self.ftp_client
        ts = kwargs.get('ts')
        stand_start_ts = ts
        # TODO:[-] 24-08-21 浮标case根据 当前时间->整点时刻->时间戳
        # stand_start_ts: int = get_station_start_ts(ts)
        # stand_start_ts: int = get_fub_start_ts(ts)
        """世界时"""
        local_root_path: str = kwargs.get('local_root_path')
        remote_root_path: str = kwargs.get('remote_root_path')

        # TODO:[*] 24-07-29 处理浮标数据只需要根据浮标中心统一处理即可(不需要再按要素进行分类处理——下载不同的要素对应的文件)
        val_element: ElementTypeEnum = ElementTypeEnum.FUB
        # step2:根据集合遍历执行批量 下载 -> 读取 -> to db -> 删除操作
        for val_station in list_station:
            logger.info(f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}')
            try:
                # step2-1:根据工厂方法获取当前要素对应的文件
                cls = factory_get_station_file(val_element)
                """ 文件类"""
                file_instance: IStationFile = cls(ftp, local_root_path, val_element, val_station.station_code,
                                                  val_station.station_name,
                                                  val_station.station_num, stand_start_ts,
                                                  remote_root_path)
                """ 文件类实例化对象"""
                # step2-2:文件下载
                # step2-3:文件读取
                # step2-4:文件写入db
                # 以上均在操作类中实现
                cls_operate = factory_get_operater(val_element)
                """操作类"""
                instance_operate: IOperater = cls_operate(file_instance)
                # TODO:[-] 24-08-22 加入查找日志记录
                downloadfile_remote_fullpath: str = f'{remote_root_path}/{file_instance.get_file_name()}'
                downloadfile_local_fullpath: str = f'{local_root_path}/{file_instance.get_file_name()}'
                logger.info(f'[-]下载地址:{downloadfile_remote_fullpath}——本地地址:{downloadfile_local_fullpath}')
                """ 操作类实例化对象"""
                instance_operate.todo(ts=stand_start_ts)
            # TODO:[*] 24-07-29 加入异常集
            except FtpDownLoadError as ftpEx:
                logger.info(
                    f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts} 出现:下载异常')
            except FileReadError as readerEx:
                logger.info(
                    f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts} 出现:数据读取异常')
            except ReadataStoreError as storeEx:
                logger.info(
                    f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts} 出现:数据写入db异常')
            except Exception as ex:
                logger.info(
                    f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts} 出现:未知异常——f{ex.args}')
        pass

        pass


def timer_download_station_realdata():
    """
        站点下载定时器
    :return:
    """
    # target_dt = arrow.Arrow(2024, 2, 20, 0, 0)
    # now_ts: int = ts
    now_ts: int = arrow.utcnow().int_timestamp
    # TODO:[*] 24-09-19 测试需要注释
    # target_dt = arrow.Arrow(2024, 9, 19, 13, 12)
    # now_ts: int = target_dt.int_timestamp
    # local_root_path: str = '/Users/evaseemefly/03data/02station'
    # TODO:[-] 24-03-27 采用 docker 容器内绝对路径为 /data/remote
    # local_root_path: str = r'D:\05data\05station_data'
    local_root_path: str = DOWNLOAD_OPTIONS.get('local_root_path')
    # TODO:[-] 24-02-26 此处修改为remote的全路径
    # remote_root_path: str = r'/home/nmefc/share/test/ObsData/'
    # v2:不使用全路径，需改为 相对路径 /home/nmefc/share/test/ObsData/' -> test/ObsData/
    # remote_root_path: str = r'test/ObsData/'
    # v3: 修改为使用远端绝对路径
    remote_root_path: str = DOWNLOAD_OPTIONS.get('remote_station_root_path')

    """当前时间的时间戳"""
    logger.info(f"触发timer_download_station_realdata|ts:{now_ts}")

    case = StationRealdataDownloadCase()
    case.todo(ts=now_ts, local_root_path=local_root_path, remote_root_path=remote_root_path)


def timer_download_slb_realdata():
    """
        站点下载定时器
    :return:
    """
    now_ts: int = arrow.utcnow().int_timestamp
    # TODO:[-] 24-03-27 采用 docker 容器内绝对路径为 /data/remote
    local_root_path: str = DOWNLOAD_OPTIONS.get('local_root_path')
    # TODO:[-] 24-02-26 此处修改为remote的全路径
    # v3: 修改为使用远端绝对路径
    remote_root_path: str = DOWNLOAD_OPTIONS.get('remote_slb_root_path')

    """当前时间的时间戳"""
    logger.info(f"触发timer_download_SLB_realdata|ts:{now_ts}")
    case = SLBStationRealdataDownloadCase()
    case.todo(ts=now_ts, local_root_path=local_root_path, remote_root_path=remote_root_path)


def timer_download_fub_realdata():
    """
        TODO:[-] 24-04-17 处理浮标数据浮标数据为每小时一个文件，需要单独处理每个整点的数据
    @return:
    """
    # target_dt = arrow.Arrow(2024, 2, 20, 0, 0)
    """utc时"""
    # now_ts: int = ts
    now_ts: int = arrow.utcnow().int_timestamp
    # TODO:[-] 24-08-22 将标准化时间放在触发器中
    stand_ts: int = get_fub_start_ts(now_ts)
    stand_dt_str: str = arrow.get(stand_ts).format('YYYY-MM-DD HH:ss')
    # TODO:[-] 24-03-27 采用 docker 容器内绝对路径为 /data/remote
    local_root_path: str = DOWNLOAD_OPTIONS.get('local_root_path')
    # TODO:[-] 24-02-26 此处修改为remote的全路径
    # v3: 修改为使用远端绝对路径 r'/home/nmefc/share/test/ObsData/数据'
    remote_root_path: str = DOWNLOAD_OPTIONS.get('remote_fub_root_path')

    """当前时间的时间戳"""
    logger.info(f"触发timer_download_FUB_realdata|ts:{stand_ts}|dt:{stand_dt_str}")

    case = FubRealdataDownloadCase()
    case.todo(ts=now_ts, local_root_path=local_root_path, remote_root_path=remote_root_path)


def task_downloads_fub_byrange(start_ts: int, end_ts: int, split_hours=1):
    """
        根据 起止时间以及切分 hours批量下载并写入浮标数据
        延时补录
    @param start_ts:
    @param end_ts: 为结束的 YYYY-MM-DD 00:00
    @param split_hours:
    @return:
    """
    timestamps = []
    current_time = arrow.get(start_ts)
    local_root_path: str = DOWNLOAD_OPTIONS.get('local_root_path')
    # TODO:[-] 24-02-26 此处修改为remote的全路径
    # v3: 修改为使用远端绝对路径
    remote_root_path: str = DOWNLOAD_OPTIONS.get('remote_fub_root_path')
    while current_time.int_timestamp <= end_ts:
        timestamps.append(current_time.int_timestamp)
        current_time = current_time.shift(hours=1)

    '''
        - 24-07-24 此处逻辑:
            循环 起止时间生成的时间戳数组 ,按照每个时间戳下载所需文件
            * 此部分存在逻辑bug,若起止时间戳24小时,则需要反复下载24次
    '''
    for temp_ts in timestamps:
        """当前时间的时间戳"""
        logger.info(f"触发timer_download_station_realdata|ts:{temp_ts}")
        # TODO:[*] 24-07-16 修改为录入站点数据
        # case = StationRealdataDownloadCase()
        case = FubRealdataDownloadCase()
        case.todo(ts=temp_ts, local_root_path=local_root_path, remote_root_path=remote_root_path)


def task_downloads_station_byrange(start_ts: int, end_ts: int, split_hours=1):
    """
        根据 时间范围 下载站点实况任务
        延时补录
    @param start_ts:
    @param end_ts:
    @param split_hours:
    @return:
    """
    timestamps = []
    current_time = arrow.get(start_ts)
    local_root_path: str = DOWNLOAD_OPTIONS.get('local_root_path')
    # TODO:[-] 24-02-26 此处修改为remote的全路径
    # v3: 修改为使用远端绝对路径
    remote_root_path: str = DOWNLOAD_OPTIONS.get('remote_station_root_path')

    while current_time.int_timestamp <= end_ts:
        timestamps.append(current_time.int_timestamp)
        # TODO:[*] 24-07-24 每天生成一个时间戳即可
        current_time = current_time.shift(days=1)

    '''
        - 24-07-24 此处逻辑:
            循环 起止时间生成的时间戳数组 ,按照每个时间戳下载所需文件
            * 此部分存在逻辑bug,若起止时间戳24小时,则需要反复下载24次
    '''
    for temp_ts in timestamps:
        """当前时间的时间戳"""
        logger.info(f"触发timer_download_station_realdata|ts:{temp_ts}")
        # TODO:[*] 24-07-16 修改为录入站点数据
        case = StationRealdataDownloadCase()
        case.todo(ts=temp_ts, local_root_path=local_root_path, remote_root_path=remote_root_path)


def task_downloads_slb_byrange(start_ts: int, end_ts: int, split_hours=1):
    """
        根据 时间范围 下载水利部站点实况任务
        延时补录
    @param start_ts:
    @param end_ts:
    @param split_hours:
    @return:
    """
    timestamps = []
    current_time = arrow.get(start_ts)
    local_root_path: str = DOWNLOAD_OPTIONS.get('local_root_path')
    # TODO:[-] 24-02-26 此处修改为remote的全路径
    # v3: 修改为使用远端绝对路径
    remote_root_path: str = DOWNLOAD_OPTIONS.get('remote_slb_root_path')

    while current_time.int_timestamp <= end_ts:
        timestamps.append(current_time.int_timestamp)
        # TODO:[*] 24-07-24 每天生成一个时间戳即可
        current_time = current_time.shift(days=1)

    '''
        - 24-07-24 此处逻辑:
            循环 起止时间生成的时间戳数组 ,按照每个时间戳下载所需文件
            * 此部分存在逻辑bug,若起止时间戳24小时,则需要反复下载24次
    '''
    for temp_ts in timestamps:
        """当前时间的时间戳"""
        logger.info(f"触发timer_download_station_realdata|ts:{temp_ts}")
        # TODO:[*] 24-07-16 修改为录入站点数据
        case = SLBStationRealdataDownloadCase()
        case.todo(ts=temp_ts, local_root_path=local_root_path, remote_root_path=remote_root_path)


def delay_task(start_ts: int, end_ts: int):
    """
        执行定时延时作业:
            定时下载站点数据
    :return:
    """
    scheduler = BackgroundScheduler(timezone='UTC')
    # 添加调度任务
    # 调度方法为 timedTask，触发器选择 interval(间隔性)，间隔时长为 2 秒
    logger.info('[-]启动定时任务触发事件:')
    # 十分钟/次 的定时下载站点任务
    # scheduler.add_job(timer_download_station_realdata, 'interval', minutes=10)
    # TODO:[-] 24-07-29 执行定时下载任务
    scheduler.add_job(timer_download_station_realdata, 'interval', minutes=10)
    # TODO:[*] 24-08-19
    # scheduler.add_job(timer_download_fub_realdata, 'interval', minutes=10)
    # # 启动调度任务
    scheduler.start()


def delay_fub_task(start_ts: int, end_ts: int):
    """
            执行定时延时作业:
                定时下载浮标数据
        :return:
        """
    scheduler = BackgroundScheduler(timezone='UTC')
    # 添加调度任务
    # 调度方法为 timedTask，触发器选择 interval(间隔性)，间隔时长为 2 秒
    logger.info('[-]启动定时任务触发事件:')
    # TODO:[*] 24-08-19
    scheduler.add_job(timer_download_fub_realdata, 'interval', minutes=1)
    # # 启动调度任务
    scheduler.start()


def delay_slb_task(start_ts: int, end_ts: int):
    """
        执行定时延时作业:
            定时下载水利部站点数据
    :return:
    """
    scheduler = BackgroundScheduler(timezone='UTC')
    # 添加调度任务
    # 调度方法为 timedTask，触发器选择 interval(间隔性)，间隔时长为 2 秒
    logger.info('[-]启动定时任务触发事件:')
    # 执行定时下载任务
    scheduler.add_job(timer_download_slb_realdata, 'interval', minutes=15)
    # scheduler.add_job(timer_download_fub_realdata, 'interval', minutes=10)
    # # 启动调度任务
    scheduler.start()
