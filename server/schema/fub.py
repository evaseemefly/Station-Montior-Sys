from datetime import datetime
from typing import List, Union, Optional

from pydantic import BaseModel, Field

from common.enums import ElementTypeEnum, ObservationTypeEnum


class FubObservationSchema(BaseModel):
    """
        对应 tb: station_info 共享潮位站基础数据
    """
    station_code: str
    element_type: ElementTypeEnum
    ts_list: List[int]
    # TODO:[*] 24-06-19 观测值有可能为NAN,此处加入允许为空的声明
    val_list: Optional[List[Optional[Union[float, int]]]] = None

    # val_list: List[float]

    class Config:
        orm_mode = True


class DistFubObservationSchema(BaseModel):
    code: str
    obs_type: ObservationTypeEnum
    observation_list: List[FubObservationSchema]

    class Config:
        orm_mode = True
