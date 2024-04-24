import arrow
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column, DeclarativeBase
from sqlalchemy import String, MetaData, Table
from datetime import datetime
from arrow import Arrow

from common.default import DEFAULT_SURGE, DEFAULT_CODE, DEFAULT_WINDSPEED, DEFAULT_DIR
from common.enums import ElementTypeEnum
from models.base_model import ITimestampModel
from models.station import IStation


class FubPerclockDataModel(IStation, ITimestampModel):
    """
        + 24-04-17 对应的浮标整点数据
        采用了方式二的实现方式，采取多冗余设计，减少耦合
    """
    table_name_base = 'surge_perclock_data_'

    # error:sqlalchemy.exc.InvalidRequestError: Class <class 'models.station.SurgePerclockDataModel'> does not have a __table__ or __tablename__ specified and does not inherit from an existing table-mapped class.
    __tablename__ = 'fub_perclock_data'

    element_type: Mapped[int] = mapped_column(default=ElementTypeEnum.WS.value)
    value: Mapped[float] = mapped_column(default=DEFAULT_WINDSPEED)

    @classmethod
    def get_tab_name(cls):
        return cls.__tablename__
