from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, DeclarativeBase
from sqlalchemy import String
from datetime import datetime
from arrow import Arrow

from common.default import DEFAULT_SURGE, DEFAULT_CODE
from models.base_model import BaseMeta, IModel, IIdIntModel, ITimestampModel
from util.common import get_utc_year


class IStationSurge(IModel, IIdIntModel):
    __abstract__ = True
    station_code: Mapped[str] = mapped_column(String(10), default=DEFAULT_CODE)


class SurgePerclockDataModel(IStationSurge, ITimestampModel):
    table_name_base = 'surge_perclock_data_'

    # error:sqlalchemy.exc.InvalidRequestError: Class <class 'models.station.SurgePerclockDataModel'> does not have a __table__ or __tablename__ specified and does not inherit from an existing table-mapped class.
    __tablename__ = 'surge_perclock_data_realtime_template'

    surge: Mapped[float] = mapped_column(default=DEFAULT_SURGE)

    @classmethod
    def get_split_tab_name(cls, dt_arrow: Arrow) -> str:
        """
            + 获取动态分表后的表名
            按照 dt_arrow 按年进行分表
        @param dt_arrow: 时间 产品 issue_dt 时间
        @return:
        """

        tab_dt_name: str = get_utc_year(dt_arrow.int_timestamp)
        tab_name: str = f'{cls.table_name_base}_{tab_dt_name}'
        return tab_name

    @classmethod
    def set_split_tab_name(cls, dt_arrow: Arrow):
        """
            + 根据动态分表规则动态分表
            按照 issue_dt 进行分表
        @param dt_arrow: 时间 产品 issue_dt 时间
        @return:
        """
        tab_name: str = cls.get_split_tab_name(dt_arrow)
        cls.__table__.name = tab_name


class SurgePerclockExtremumDataModel(IStationSurge, ITimestampModel):
    table_name_base = 'surge_perclock_data_'

    # error:sqlalchemy.exc.InvalidRequestError: Class <class 'models.station.SurgePerclockDataModel'> does not have a __table__ or __tablename__ specified and does not inherit from an existing table-mapped class.
    __tablename__ = 'surge_perclock_data_extremum_template'

    surge: Mapped[float] = mapped_column(default=DEFAULT_SURGE)

    @classmethod
    def get_split_tab_name(cls, dt_arrow: Arrow) -> str:
        """
            + 获取动态分表后的表名
            按照 dt_arrow 按年进行分表
        @param dt_arrow: 时间 产品 issue_dt 时间
        @return:
        """

        tab_dt_name: str = get_utc_year(dt_arrow.int_timestamp)
        tab_name: str = f'{cls.table_name_base}_{tab_dt_name}'
        return tab_name

    @classmethod
    def set_split_tab_name(cls, dt_arrow: Arrow):
        """
            + 根据动态分表规则动态分表
            按照 issue_dt 进行分表
        @param dt_arrow: 时间 产品 issue_dt 时间
        @return:
        """
        tab_name: str = cls.get_split_tab_name(dt_arrow)
        cls.__table__.name = tab_name

    @classmethod
    def get_tab_name(cls):
        return cls.__tablename__
