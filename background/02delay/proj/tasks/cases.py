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

from common.enums import ElementTypeEnum
from common.exceptions import FtpDownLoadError, FileReadError, ReadataStoreError
from conf._privacy import FTP_LIST
from conf.settings import DOWNLOAD_OPTIONS
from core.files import IFile, IStationFile
from core.operaters import IOperater

from mid_models.stations import StationElementMidModel
from util.common import get_station_start_ts
from util.factory import factory_get_station_file, factory_get_operater
from util.ftp import FtpClient
from util.decorators import decorator_timer_consuming, decorator_exception_logging


class ICase(ABC):

    def __init__(self):
        self.ftp_client: FtpClient = self.__init_ftp_client()

    def __init_ftp_client(self) -> FtpClient:
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

    @abstractmethod
    def todo(self, **kwargs):
        pass


class StationRealdataDownloadCase(ICase):
    # @decorator_exception_logging
    def todo(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        # step1: 根据当前的触发时间，以及海洋站集合获取对应的站点集合|每个站点对应的要素
        """
            {'code':[要素a,要素b],,,}
        """
        # TODO:[*] 24-02-27 注意例如站点不存在某个要素的整点数据，例如潮位数据WL 莆田不存在，则会报错
        list_station: List[StationElementMidModel] = [
            # StationElementMidModel('SHW', '08522', '莆田', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('YAO', '09710', '南澳', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # 存在大小写的问题
            # StationElementMidModel('PTN', '08440', '平潭', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('QLN', '11742', '清澜', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('SPU', '07421', '石浦', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('DTO', '07450', '温州', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('SHW', '09711', '汕尾', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('HZO', '09740', '惠州', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND])
            # part1: 北海
            # StationElementMidModel('QHD', '03122', '秦皇岛', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('RZH', '04144', '日照', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('TGU', '02123', '塘沽', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('WFG', '04163', '潍坊', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('ZFD', '04152', '芝罘岛', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('BYQ', '01111', '鲅鱼圈', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('HZO', '04130', '北隍城', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('CFD', '03126', '曹妃甸', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('CST', '04133', '成山头', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('GUD', '04166', '孤东', [ElementTypeEnum.SURGE]),
            # --- 24-08-01
            # StationElementMidModel('GUD', '04166', '东港', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('GUD', '04166', '小长山', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('GUD', '04166', '皮口', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('LHT', '01146', '老虎滩', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('GUD', '04166', '长兴岛', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('BYQ', '01111', '鲅鱼圈', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('GUD', '04166', '营口', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('GUD', '04166', '锦州', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('GUD', '04166', '盘锦', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('HLD', '01120', '葫芦岛', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('GUD', '04166', '芷锚湾', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('GUD', '04166', '京唐港', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('GUD', '04166', '唐山三岛', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('GUD', '04166', '黄骅', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('GUD', '04166', '滨州港', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('GUD', '04166', '黄河海港', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('GUD', '04166', '东营港', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('GUD', '04166', '垦东', [ElementTypeEnum.SURGE]),
            # ----
            # StationElementMidModel('QHD', '03122', '秦皇岛', [ElementTypeEnum.WIND]),
            # StationElementMidModel('RZH', '04144', '日照', [ElementTypeEnum.WIND]),
            # StationElementMidModel('TGU', '02123', '塘沽', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('WFG', '04163', '潍坊', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('ZFD', '04152', '芝罘岛', [ElementTypeEnum.WIND]),
            # StationElementMidModel('BYQ', '01111', '鲅鱼圈', [ElementTypeEnum.WIND]),
            # StationElementMidModel('HZO', '04130', '北隍城', [ElementTypeEnum.WIND]),
            # StationElementMidModel('CFD', '03126', '曹妃甸', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('CST', '04133', '成山头', [ElementTypeEnum.WIND]),
            # StationElementMidModel('GUD', '04166', '孤东', [ElementTypeEnum.WIND]),
            # TODO:[*] 24-07-16 加入了两个站
            # StationElementMidModel('BZG', '04223', '滨州港', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('HHA', '03125', '黄骅', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('BJA', '08433', '北茭', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('CGM', '08444', '长门', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('PTN', '08440', '平潭', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            # StationElementMidModel('SHA', '08430', '三沙', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
            StationElementMidModel('RAS', '70600800', '瑞安S', [ElementTypeEnum.SURGE]),
            StationElementMidModel('AJS', '70610600', '鳌江S', [ElementTypeEnum.SURGE]),
            StationElementMidModel('WZS', '70503400', '温州S', [ElementTypeEnum.SURGE]),
            # StationElementMidModel('PTN', '08440', '平潭', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
        ]
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


class SLStationRealdataDownloadCase(ICase):
    """
        TODO:[-] 24-08-02 加入的处理水利部站点的case
    """

    # @decorator_exception_logging
    def todo(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        # step1: 根据当前的触发时间，以及海洋站集合获取对应的站点集合|每个站点对应的要素
        """
            {'code':[要素a,要素b],,,}
        """
        # TODO:[*] 24-02-27 注意例如站点不存在某个要素的整点数据，例如潮位数据WL 莆田不存在，则会报错
        list_station: List[StationElementMidModel] = [
            StationElementMidModel('RAS', '70600800', '瑞安S', [ElementTypeEnum.SURGE]),
            StationElementMidModel('AJS', '70610600', '鳌江S', [ElementTypeEnum.SURGE]),
            StationElementMidModel('WZS', '70503400', '温州S', [ElementTypeEnum.SURGE]), ]
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

    def todo(self, **kwargs):
        """
                24-07-24: 浮标数据是每个小时一个观测数据集
                * 注意与海洋站不同
        @param kwargs:
        @return:
        """
        # TODO:[*] 24-02-27 注意例如站点不存在某个要素的整点数据，例如潮位数据WL 莆田不存在，则会报错
        list_station: List[StationElementMidModel] = [
            StationElementMidModel('02001', '02001', 'MF02001', [ElementTypeEnum.FUB]),
            StationElementMidModel('02004', '02004', 'MF02004', [ElementTypeEnum.FUB]),
        ]
        ftp = self.ftp_client
        ts = kwargs.get('ts')
        # TODO:[*] 24-04-19 浮标case不需要对ts进行标准化，此处后续需要修改
        # stand_start_ts: int = get_station_start_ts(ts)
        stand_start_ts: int = ts
        """世界时"""
        local_root_path: str = kwargs.get('local_root_path')
        remote_root_path: str = kwargs.get('remote_root_path')
        # TODO:[*] 24-07-29 处理浮标数据只需要根据浮标中心统一处理即可(不需要再按要素进行分类处理——下载不同的要素对应的文件)
        val_element: ElementTypeEnum = ElementTypeEnum.FUB
        # step2:根据集合遍历执行批量 下载 -> 读取 -> to db -> 删除操作
        for val_station in list_station:
            logger.info(f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}')
            logger.info(
                f'[-]处理:{val_station.station_name}-{val_station.station_code}站点|ts:{stand_start_ts}')
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
    # local_root_path: str = '/Users/evaseemefly/03data/02station'
    # TODO:[-] 24-03-27 采用 docker 容器内绝对路径为 /data/remote
    # local_root_path: str = r'D:\05data\05station_data'
    local_root_path: str = DOWNLOAD_OPTIONS.get('local_root_path')
    # TODO:[-] 24-02-26 此处修改为remote的全路径
    # remote_root_path: str = r'/home/nmefc/share/test/ObsData/'
    # v2:不使用全路径，需改为 相对路径 /home/nmefc/share/test/ObsData/' -> test/ObsData/
    # remote_root_path: str = r'test/ObsData/'
    # v3: 修改为使用远端绝对路径
    remote_root_path: str = DOWNLOAD_OPTIONS.get('remote_fub_root_path')

    """当前时间的时间戳"""
    logger.info(f"触发timer_download_station_realdata|ts:{now_ts}")

    case = StationRealdataDownloadCase()
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
    # TODO:[-] 24-03-27 采用 docker 容器内绝对路径为 /data/remote
    local_root_path: str = DOWNLOAD_OPTIONS.get('local_root_path')
    # TODO:[-] 24-02-26 此处修改为remote的全路径
    # v3: 修改为使用远端绝对路径 r'/home/nmefc/share/test/ObsData/数据'
    remote_root_path: str = DOWNLOAD_OPTIONS.get('remote_fub_root_path')

    """当前时间的时间戳"""
    logger.info(f"触发timer_download_station_realdata|ts:{now_ts}")

    case = FubRealdataDownloadCase()
    case.todo(ts=now_ts, local_root_path=local_root_path, remote_root_path=remote_root_path)


def task_downloads_fub_byrange(start_ts: int, end_ts: int, split_hours=1):
    """
        根据 起止时间以及切分 hours批量下载并写入浮标数据
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


def delay_task():
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
    scheduler.add_job(timer_download_fub_realdata, 'interval', minutes=10)
    # # 启动调度任务
    scheduler.start()
