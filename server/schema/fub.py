from datetime import datetime
from typing import List, Union

from pydantic import BaseModel, Field

from common.enums import ElementTypeEnum, ObservationTypeEnum


class FubObservationSchema(BaseModel):
    """
        对应 tb: station_info 共享潮位站基础数据
    """
    station_code: str
    element_type: ElementTypeEnum
    ts_list: List[int]
    val_list: List[Union[float, int]]

    # val_list: List[float]

    class Config:
        orm_mode = True


class DistFubObservationSchema(BaseModel):
    code: str
    obs_type: ObservationTypeEnum
    observation_list: List[FubObservationSchema]

    class Config:
        orm_mode = True
