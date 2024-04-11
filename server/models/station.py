import arrow
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, DeclarativeBase
from sqlalchemy import String, MetaData, Table
from datetime import datetime
from arrow import Arrow

from common.default import DEFAULT_SURGE, DEFAULT_CODE, DEFAULT_WINDSPEED, DEFAULT_DIR
from db.db import DBFactory
from models.base_model import BaseMeta, IModel, IIdIntModel, ITimestampModel
from util.common import get_utc_year


class IStation(IModel, IIdIntModel):
    __abstract__ = True
    station_code: Mapped[str] = mapped_column(String(10), default=DEFAULT_CODE)

    @classmethod
    def get_split_tab_name(cls, ts: int) -> str:
        """
            + 获取动态分表后的表名
            按照 dt_arrow 按年进行分表
        @param ts: 时间 产品 时间戳
        @return:
        """

        tab_dt_name: str = get_utc_year(ts)
        tab_name: str = f'{cls.table_name_base}_{tab_dt_name}'
        return tab_name

    @classmethod
    def set_split_tab_name(cls, ts: int):
        """
            + 根据动态分表规则动态分表
            按照 issue_dt 进行分表
        @param dt_arrow: 时间 产品 issue_dt 时间
        @return:
        """
        tab_name: str = cls.get_split_tab_name(ts)
        cls.__table__.name = tab_name


class SurgePerclockDataModel(IStation, ITimestampModel):
    table_name_base = 'surge_perclock_data_realtime'

    # error:sqlalchemy.exc.InvalidRequestError: Class <class 'models.station.SurgePerclockDataModel'> does not have a __table__ or __tablename__ specified and does not inherit from an existing table-mapped class.
    __tablename__ = 'surge_perclock_data_realtime_template'

    surge: Mapped[float] = mapped_column(default=DEFAULT_SURGE)

    @classmethod
    def get_split_tab_name(cls, ts: int) -> str:
        """
            + 获取动态分表后的表名
            按照 dt_arrow 按年进行分表
        @param ts: 时间 产品 issue_dt 时间
        @return:
        """

        tab_dt_name: str = get_utc_year(ts)
        tab_name: str = f'{cls.table_name_base}_{tab_dt_name}'
        return tab_name

    @classmethod
    def set_split_tab_name(cls, ts: int):
        """
            + 根据动态分表规则动态分表
            按照 issue_dt 进行分表
        @param ts: 时间 产品 issue_dt 时间
        @return:
        """
        tab_name: str = cls.get_split_tab_name(ts)
        cls.__table__.name = tab_name

    @classmethod
    def check_needsplittab(cls, start_ts: int, end_ts: int) -> bool:
        """
            判断起止时间是否需要分表
            判断逻辑:
                start utc 时间的year== end utc 年份
        :param start_ts:
        :param end_ts:
        :return:
        """
        is_need: bool = False
        start_arrow: arrow.Arrow = arrow.get(start_ts)
        end_arrow: arrow.Arrow = arrow.get(end_ts)
        is_need: bool = start_arrow.date().year != end_arrow.date().year
        return is_need


class WindPerclockDataModel(IStation, ITimestampModel):
    """
        + 24-04-08
         整点风要素- 按年分表
    """

    table_name_base = 'wind_perclock_data_realtime'

    # error:sqlalchemy.exc.InvalidRequestError: Class <class 'models.station.SurgePerclockDataModel'> does not have a __table__ or __tablename__ specified and does not inherit from an existing table-mapped class.
    __tablename__ = 'wind_perclock_data_realtime_template'

    ws: Mapped[float] = mapped_column(default=DEFAULT_WINDSPEED)
    """风速"""

    wd: Mapped[int] = mapped_column(default=DEFAULT_DIR)
    """风向"""


class SurgePerclockExtremumDataModel(IStation, ITimestampModel):
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


def get_table(tab_name: str):
    db = DBFactory()
    engine = db.get_engine()
    # 创建元数据对象
    metadata = MetaData()
    # TODO:[*] 24-04-07 TypeError: Additional arguments should be named <dialectname>_<argument>, got 'autoload'
    # 创建 table 时去掉  autoload=True 参数
    table = Table(tab_name, metadata, autoload_with=engine)
    return table
