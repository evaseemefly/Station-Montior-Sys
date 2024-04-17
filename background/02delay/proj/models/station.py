from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, DeclarativeBase
from sqlalchemy import String, MetaData
from sqlalchemy import ForeignKey, Sequence, MetaData, Table
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, text
from sqlalchemy.dialects.mysql import DATETIME, INTEGER, TINYINT, VARCHAR
from datetime import datetime
from arrow import Arrow

from common.default import DEFAULT_SURGE, DEFAULT_CODE, DEFAULT_WINDSPEED, DEFAULT_DIR, DEFAULT_DT_STAMP
from common.enums import ExtremumType, ElementTypeEnum
from db.db import DbFactory
from models.base_model import BaseMeta, IModel, IIdIntModel, ITimestampModel
from util.common import get_utc_year


class IStation(IModel, IIdIntModel):
    __abstract__ = True
    table_name_base = 'element_perclock_data_'

    station_code: Mapped[str] = mapped_column(String(10), default=DEFAULT_CODE)

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


class SurgePerclockDataModel(IStation, ITimestampModel):
    """
        整点潮位 —— 按年动态分表
    """
    table_name_base = 'surge_perclock_data_realtime'

    # error:sqlalchemy.exc.InvalidRequestError: Class <class 'models.station.SurgePerclockDataModel'> does not have a __table__ or __tablename__ specified and does not inherit from an existing table-mapped class.
    __tablename__ = 'surge_perclock_data_realtime_template'

    surge: Mapped[float] = mapped_column(default=DEFAULT_SURGE)

    @classmethod
    def create_tab(cls, dt_arrow: Arrow):
        """
            由于采用了分表，加入了分表方法
        """
        is_ok = False
        meta_data = MetaData()
        tab_name: str = cls.get_split_tab_name(dt_arrow)
        # TODO:[*] 24-04-07 缺少索引
        Table(tab_name, meta_data, Column('id', Integer, primary_key=True),
              Column('is_del', TINYINT(1), nullable=False, server_default=text("'0'"), default=0),
              Column('station_code', VARCHAR(200), nullable=False, index=True), Column('tid', Integer, nullable=False),
              Column('surge', Float, nullable=False),
              Column('issue_ts', Integer, nullable=False),
              Column('issue_dt', DATETIME(fsp=6), default=datetime.utcnow, index=True),
              Column('gmt_create_time', DATETIME(fsp=6), default=datetime.utcnow),
              Column('gmt_modify_time', DATETIME(fsp=6), default=datetime.utcnow))
        db_factory = DbFactory()
        session = db_factory.Session
        engine = db_factory.engine
        with engine.connect() as conn:
            try:
                meta_data.create_all(engine)
                is_ok = True
            except Exception as ex:
                print(ex.args)
        session.commit()


class SurgePerclockExtremumDataModel(IStation, ITimestampModel):
    table_name_base = 'surge_perclock_data_'

    # error:sqlalchemy.exc.InvalidRequestError: Class <class 'models.station.SurgePerclockDataModel'> does not have a __table__ or __tablename__ specified and does not inherit from an existing table-mapped class.
    __tablename__ = 'surge_perclock_data_extremum_template'

    surge: Mapped[float] = mapped_column(default=DEFAULT_SURGE)


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

    @classmethod
    def create_tab(cls, dt_arrow: Arrow):
        """
            由于采用了分表，加入了分表方法
        """
        is_ok = False
        meta_data = MetaData()
        tab_name: str = cls.get_split_tab_name(dt_arrow)
        # TODO:[*] 24-04-07 缺少索引
        Table(tab_name, meta_data, Column('id', Integer, primary_key=True),
              Column('is_del', TINYINT(1), nullable=False, server_default=text("'0'"), default=0),
              Column('station_code', VARCHAR(200), nullable=False, index=True),
              Column('ws', Float, nullable=False),
              Column('wd', Integer, nullable=False),
              Column('issue_ts', Integer, nullable=False),
              Column('issue_dt', DATETIME(fsp=6), default=datetime.utcnow, index=True),
              Column('gmt_create_time', DATETIME(fsp=6), default=datetime.utcnow),
              Column('gmt_modify_time', DATETIME(fsp=6), default=datetime.utcnow))
        db_factory = DbFactory()
        session = db_factory.Session
        engine = db_factory.engine
        with engine.connect() as conn:
            try:
                meta_data.create_all(engine)
                is_ok = True
            except Exception as ex:
                print(ex.args)
        session.commit()


class WindPerclockExtremumDataModel(IStation, ITimestampModel):
    """
        风要素极值
        整点中提取的极值
    """
    table_name_base = 'surge_perclock_data_'

    # error:sqlalchemy.exc.InvalidRequestError: Class <class 'models.station.SurgePerclockDataModel'> does not have a __table__ or __tablename__ specified and does not inherit from an existing table-mapped class.
    __tablename__ = 'wind_perclock_data_extremum_template'

    ws: Mapped[float] = mapped_column(default=DEFAULT_WINDSPEED)
    """风速"""

    wd: Mapped[int] = mapped_column(default=DEFAULT_DIR)
    """风向"""

    dt_local_stamp: Mapped[str] = mapped_column(default=DEFAULT_DT_STAMP)
    """ TODO:[-] 24-04-08 日期戳(确定极值唯一性)"""

    extremum_type: Mapped[int] = mapped_column(default=ExtremumType.WIND_EXTREMUM.value)
    """极值种类: 极值|最大值"""


class FubPerclockDataModel(ITimestampModel):
    """
        + 24-04-17 对应的浮标整点数据
        采用了方式二的实现方式，采取多冗余设计，减少耦合
    """
    table_name_base = 'surge_perclock_data_'

    # error:sqlalchemy.exc.InvalidRequestError: Class <class 'models.station.SurgePerclockDataModel'> does not have a __table__ or __tablename__ specified and does not inherit from an existing table-mapped class.
    __tablename__ = 'fub_perclock_data'

    element_type: Mapped[int] = mapped_column(default=ElementTypeEnum.WS.value)
    value: Mapped[float] = mapped_column(default=DEFAULT_WINDSPEED)
