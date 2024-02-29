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

from core.files import IFile, IStationFile
from util.decorators import decorator_timer_consuming
from util.ftp import FtpClient
from util.common import get_store_relative_path
from common.enums import ElementTypeEnum

from models.station import SurgePerclockDataModel, SurgePerclockExtremumDataModel
from core.readers import SurgeReader
from db.db import DbFactory


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
            temp_surge = val['surge']
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
                    surge=temp_surge,
                    station_code=station_code,
                    issue_ts=temp_ts,
                    gmt_modify_time=utc_now
                ))
                self.session.execute(update_stmt)
            # 若不存在则insert
            else:
                temp_model = SurgePerclockDataModel(surge=temp_surge,
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
