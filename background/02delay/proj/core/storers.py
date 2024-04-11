"""
    + 24-01-16 各类存储器(写入db)
"""

from abc import ABC, abstractmethod, abstractproperty
import pathlib
from typing import Optional, List, Dict, Any
import arrow
import pandas as pd
from sqlalchemy import distinct, select, update
from datetime import datetime

from common.default import DEFAULT_SURGE, DEFAULT_WINDSPEED, DEFAULT_DIR
from core.files import IFile, IStationFile
from mid_models.elements import WindExtremum
from util.decorators import decorator_timer_consuming
from util.ftp import FtpClient
from util.common import get_store_relative_path, get_standard_datestamp
from common.enums import ElementTypeEnum, ExtremumType

from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel, WindPerclockDataModel, \
    WindPerclockExtremumDataModel
from core.readers import SurgeReader
from db.db import DbFactory, check_exist_tab
from util.qc import is_standard_ws, DEFAULT_VAL_LIST


class IStore(ABC):

    @abstractmethod
    def to_db(self, **kwargs):
        """
            写入数据库
        :return:
        """
        pass


class SurgeStore(IStore):
    def __init__(self, file: IStationFile):
        self.file = file
        """当前要素文件"""
        self.session = DbFactory().Session
        """数据库session"""

    @decorator_timer_consuming
    def to_db(self, **kwargs):
        """
            根据传入的 ts,realdata_list,extremum_list 循环写入 db
        :param kwargs: ts: 时间戳
        :param kwargs: realdata_list: 潮位实况集合
        :param kwargs: extremum_list: 潮位极值集合
        :return:
        """
        ts: int = kwargs.get('ts')
        realdata_list: List[dict] = kwargs.get('realdata_list')
        """站点实况集合"""
        extremum_list: List[dict] = kwargs.get('extremum_list')
        """站点极值集合"""

        # step2: 根据实况 与 极值 集合写入db
        self._loop_realdata_2_db(realdata_list, ts)
        self._loop_extremum_2_db(extremum_list, ts)

    def _loop_realdata_2_db(self, realdata_list: List[dict], ts: int):
        """
            TODO:[-] 24-01-16
            循环将实况数据写入 db
        :param realdata_list: {'ts','surge'}[]
        :return:
        """
        station_code = self.file.station_code
        for val in realdata_list:
            temp_ts = val['ts']
            temp_dt: datetime = arrow.get(temp_ts).datetime

            # TODO:[*] 24-01-16 此处应加入是否为默认值 9999 的判断
            # TODO:[*] 24-04-07 跨年可能有问题,加入判断是否存在指定表，若不存在则创建该表的逻辑
            self.check_tab(arrow.get(temp_ts))
            SurgePerclockDataModel.set_split_tab_name(arrow.get(ts))
            temp_surge = val['surge']
            temp_standard_val = temp_surge
            """当前实况的标准化后数值(若为缺省值则统一赋值为标准缺省值"""
            # TODO:[-] 24-04-11 新加入若存在缺省值，进行标准化后将标准化后的缺省值录入db
            if temp_surge in DEFAULT_VAL_LIST:
                temp_standard_val = DEFAULT_SURGE
            # TODO:[*] 24-04-07 加入指定表是否存在的判断，若不存在则创建
            stmt = select(SurgePerclockDataModel).where(SurgePerclockDataModel.station_code == station_code,
                                                        SurgePerclockDataModel.issue_ts == temp_ts)
            filter_res = self.session.execute(stmt).fetchall()
            # 当前时间
            utc_now: datetime = arrow.Arrow.utcnow().datetime
            # 若已经存在则直接更新
            if len(filter_res) > 0:
                update_stmt = (update(SurgePerclockDataModel).where(
                    SurgePerclockDataModel.station_code == station_code,
                    SurgePerclockDataModel.issue_ts == temp_ts).values(
                    surge=temp_standard_val,
                    station_code=station_code,
                    issue_ts=temp_ts,
                    gmt_modify_time=utc_now
                ))
                self.session.execute(update_stmt)
            # 若不存在则insert
            else:
                temp_model = SurgePerclockDataModel(surge=temp_standard_val,
                                                    station_code=station_code,
                                                    issue_ts=temp_ts,
                                                    issue_dt=temp_dt,
                                                    gmt_modify_time=utc_now, gmt_create_time=utc_now)
                self.session.add(temp_model)
        self.session.commit()
        self.session.close()
        pass

    def _loop_extremum_2_db(self, extremum_list: List[dict], ts: int):
        """
            循环将极值写入db
        :param extremum_list: {'ts','surge'}[]
        :return:
        """
        station_code = self.file.station_code
        for val in extremum_list:
            temp_ts = val['ts']
            temp_dt: datetime = arrow.get(temp_ts).datetime

            # TODO:[*] 24-01-16 此处应加入是否为默认值 9999 的判断
            temp_surge = val['surge']
            stmt = select(SurgePerclockExtremumDataModel).where(
                SurgePerclockExtremumDataModel.station_code == station_code,
                SurgePerclockExtremumDataModel.issue_ts == temp_ts)
            filter_res = self.session.execute(stmt).fetchall()
            # 当前时间
            utc_now: datetime = arrow.Arrow.utcnow().datetime
            # 若已经存在则直接更新
            if len(filter_res) > 0:
                update_stmt = (update(SurgePerclockExtremumDataModel).where(
                    SurgePerclockExtremumDataModel.station_code == station_code,
                    SurgePerclockExtremumDataModel.issue_ts == temp_ts).values(
                    surge=temp_surge,
                    station_code=station_code,
                    issue_ts=temp_ts,
                    gmt_modify_time=utc_now
                ))
                self.session.execute(update_stmt)
            # 若不存在则insert
            else:
                temp_model = SurgePerclockExtremumDataModel(surge=temp_surge,
                                                            station_code=station_code,
                                                            issue_ts=temp_ts,
                                                            issue_dt=temp_dt,
                                                            gmt_modify_time=utc_now, gmt_create_time=utc_now)
                self.session.add(temp_model)
        self.session.commit()
        self.session.close()
        pass

    def check_tab(self, dt_arrow: arrow):
        """
            根据当前时间判断指定表是否存在，若不存在则创建该表
        @param dt_arrow:
        @param tab_name:
        @return:
        """

        tab_name: str = SurgePerclockDataModel.get_split_tab_name(dt_arrow)
        if check_exist_tab(tab_name) is False:
            # TODO:[-] 24-04-08 注意此处缺少对极值表创建操作(极值表手动创建，不需要自动创建——不需要分表存储)
            SurgePerclockDataModel.create_tab(dt_arrow)
        pass


class PerclockWindStore(IStore):
    """
        整点风存储
    """

    def __init__(self, file: IStationFile):
        self.file = file
        """当前要素文件"""
        self.session = DbFactory().Session
        """数据库session"""

    @decorator_timer_consuming
    def to_db(self, **kwargs):
        """
            将 风要素 实况与极值等写入 db
        @param kwargs:
        @return:
        """
        ts: int = kwargs.get('ts')
        realdata_list: List[dict] = kwargs.get('realdata_list')
        max: WindExtremum = kwargs.get('max')
        extremum: WindExtremum = kwargs.get('extremum')
        # 分别将文件中的逐时风及极值风写入db
        self._loop_realdata_2_db(realdata_list, ts)
        self._loop_extremum_2_db(extremum, max, ts)
        pass

    def check_tab(self, dt_arrow: arrow):
        """
            根据当前时间判断指定表是否存在，若不存在则创建该表
        @param dt_arrow:
        @param tab_name:
        @return:
        """

        tab_name: str = WindPerclockDataModel.get_split_tab_name(dt_arrow)
        if check_exist_tab(tab_name) is False:
            WindPerclockDataModel.create_tab(dt_arrow)
        pass

    def _loop_realdata_2_db(self, realdata_list: List[dict], ts: int):
        """
            写入 整点逐时风要素
        @param realdata_list:逐时风集合
        @param ts:标准化后的时间戳
        @return:
        """
        station_code = self.file.station_code
        for val in realdata_list:
            temp_ts = val['ts']
            temp_dt: datetime = arrow.get(temp_ts).datetime

            # TODO:[*] 24-01-16 此处应加入是否为默认值 9999 的判断
            # TODO:[*] 24-04-07 跨年可能有问题,加入判断是否存在指定表，若不存在则创建该表的逻辑
            self.check_tab(arrow.get(temp_ts))
            WindPerclockDataModel.set_split_tab_name(arrow.get(ts))
            temp_ws = val['ws']
            temp_standard_ws = temp_ws
            """当前实况的标准化后数值(若为缺省值则统一赋值为标准缺省值"""
            temp_wd = val['wd']
            temp_standard_wd = temp_ws
            if temp_ws in DEFAULT_VAL_LIST:
                temp_standard_ws = DEFAULT_WINDSPEED
            if temp_wd in DEFAULT_VAL_LIST:
                temp_standard_wd = DEFAULT_DIR
            """当前实况的标准化后数值(若为缺省值则统一赋值为标准缺省值"""
            # TODO:[*] 24-04-07 加入指定表是否存在的判断，若不存在则创建
            stmt = select(WindPerclockDataModel).where(WindPerclockDataModel.station_code == station_code,
                                                       WindPerclockDataModel.issue_ts == temp_ts)
            filter_res = self.session.execute(stmt).fetchall()
            # 当前时间
            utc_now: datetime = arrow.Arrow.utcnow().datetime
            # 若已经存在则直接更新
            if len(filter_res) > 0:
                update_stmt = (update(WindPerclockDataModel).where(
                    WindPerclockDataModel.station_code == station_code,
                    WindPerclockDataModel.issue_ts == temp_ts).values(
                    ws=temp_standard_ws,
                    wd=temp_standard_wd,
                    station_code=station_code,
                    issue_ts=temp_ts,
                    issue_dt=temp_dt,
                    gmt_modify_time=utc_now
                ))
                self.session.execute(update_stmt)
            # 若不存在则insert
            else:
                temp_model = WindPerclockDataModel(ws=temp_standard_ws,
                                                   wd=temp_standard_wd,
                                                   station_code=station_code,
                                                   issue_ts=temp_ts,
                                                   issue_dt=temp_dt,
                                                   gmt_modify_time=utc_now, gmt_create_time=utc_now)
                self.session.add(temp_model)
        self.session.commit()
        self.session.close()
        pass

    def _loop_extremum_2_db(self, extremum: WindExtremum, max: WindExtremum, ts: int):
        """
            将当前日期的风要素 极值与最大值 写入db
            TODO:[*] 24-04-09 此方法内判断方法较为冗余
        @param extremum:当前文件的极值风
        @param max:当前文件的最大风
        @param ts:当前标准化后的时间戳
        @return:
        """
        station_code: str = self.file.station_code
        # dt_local_stamp: str = arrow.get(ts).format('YYYYMMDD')
        # 根据日期戳与站点code获取指定日期的唯一极值(max或extremum)
        # TODO:[-] 24-04-09 分别更新 风要素 极值 与 最大值
        # stepS1: 判断 风要素 极值与最大值 是否是缺省值
        is_standard_extremum: bool = is_standard_ws(extremum.val) and is_standard_ws(extremum.dir)
        """极值是否标准化"""
        is_standard_max: bool = is_standard_ws(max.val) and is_standard_ws(max.dir)
        """最大值是否标准化"""

        date_standard_str: str = f'{get_standard_datestamp(ts, ElementTypeEnum.WIND)}'

        utc_now: datetime = arrow.Arrow.utcnow().datetime
        """当前时间(utc)"""
        # setp1: 更新 风要素 最大值
        if is_standard_max:
            stmt_max = select(WindPerclockExtremumDataModel).where(
                WindPerclockExtremumDataModel.station_code == station_code,
                WindPerclockExtremumDataModel.dt_local_stamp == date_standard_str,
                WindPerclockExtremumDataModel.extremum_type == ExtremumType.WIND_MAX.value, )
            filter_res_max = self.session.execute(stmt_max).fetchall()
            test_filter = self.session.execute(stmt_max).first()

            if len(filter_res_max) > 0:
                # 更新
                # 更新 max
                update_stmt_max = (update(WindPerclockExtremumDataModel).where(
                    WindPerclockExtremumDataModel.station_code == station_code,
                    WindPerclockExtremumDataModel.dt_local_stamp == date_standard_str,
                    WindPerclockExtremumDataModel.extremum_type == ExtremumType.WIND_MAX.value, ).values(
                    ws=max.val,
                    wd=max.dir,
                    station_code=station_code,
                    issue_ts=max.ts,
                    issue_dt=arrow.get(max.ts).datetime,
                    gmt_modify_time=utc_now
                ))
                # TODO:[*] 24-04-09 尝试使用以下方式进行update
                # test_filter.ws = max.val
                # test_filter.wd = max.dir
                # test_filter.station_code = station_code
                # test_filter.issue_ts = max.ts
                # test_filter.issue_dt = arrow.get(max.ts).datetime
                # test_filter.gmt_modify_time = utc_now
                self.session.execute(update_stmt_max)
                pass
            else:
                temp_max_model = WindPerclockExtremumDataModel(ws=max.val,
                                                               wd=max.dir,
                                                               station_code=station_code,
                                                               issue_ts=max.ts,
                                                               issue_dt=arrow.get(max.ts).datetime,
                                                               gmt_modify_time=utc_now,
                                                               dt_local_stamp=date_standard_str,
                                                               extremum_type=ExtremumType.WIND_MAX.value
                                                               )
                self.session.add(temp_max_model)
                pass
        if is_standard_extremum:
            # step2: 更新 风要素 极值
            stmt_extremum = select(WindPerclockExtremumDataModel).where(
                WindPerclockExtremumDataModel.station_code == station_code,
                WindPerclockExtremumDataModel.dt_local_stamp == date_standard_str,
                WindPerclockExtremumDataModel.extremum_type == ExtremumType.WIND_EXTREMUM.value, )
            filter_res_extremum = self.session.execute(stmt_extremum).fetchall()
            if len(filter_res_extremum) > 0:
                update_stmt_extremum = (update(WindPerclockExtremumDataModel).where(
                    WindPerclockExtremumDataModel.station_code == station_code,
                    WindPerclockExtremumDataModel.dt_local_stamp == date_standard_str,
                    WindPerclockExtremumDataModel.extremum_type == ExtremumType.WIND_EXTREMUM.value, ).values(
                    ws=extremum.val,
                    wd=extremum.dir,
                    station_code=station_code,
                    issue_ts=extremum.ts,
                    issue_dt=arrow.get(extremum.ts).datetime,
                    gmt_modify_time=utc_now
                ))
                self.session.execute(update_stmt_extremum)
                pass
            else:
                # 新增
                temp_extremum_model = WindPerclockExtremumDataModel(ws=extremum.val,
                                                                    wd=extremum.dir,
                                                                    station_code=station_code,
                                                                    issue_ts=extremum.ts,
                                                                    issue_dt=arrow.get(extremum.ts).datetime,
                                                                    gmt_modify_time=utc_now,
                                                                    dt_local_stamp=date_standard_str,
                                                                    extremum_type=ExtremumType.WIND_EXTREMUM.value
                                                                    )
                self.session.add(temp_extremum_model)
                pass
        self.session.commit()
        self.session.close()
        pass
