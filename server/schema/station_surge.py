from datetime import datetime
from typing import List

from pydantic import BaseModel, Field


class SurgeRealDataSchema(BaseModel):
    """
        潮位实况
        station_code: 站点编号
        surge: 潮位
        tid: 对应所属行政区划
    """
    station_code: str
    issue_ts: int
    issue_dt: datetime
    surge: float

    class Config:
        orm_mode = True


class TideRealDataSchema(BaseModel):
    """
        天文潮 data
        station_code: 站点编号
        surge: 潮位
        tid: 对应所属行政区划
    """
    station_code: str
    gmt_realtime: datetime
    surge: float
    ts: int


class SurgeRealDataJoinStationSchema(BaseModel):
    """
        潮位实况(含station经纬度信息)
        station_code: 站点编号
        surge: 潮位
        tid: 对应所属行政区划
        lat: 经纬度
        lon: 经纬度
    """
    station_code: str
    gmt_realtime: datetime
    surge: float
    tid: int
    lat: float
    lon: float


class AstronomicTideSchema(BaseModel):
    """
        天文潮
    """
    station_code: str
    forecast_dt: str
    surge: float


class StationSurgeListSchema(BaseModel):
    forecast_ts_list: List[int]
    surge_list: List[float]


class DistStationTideListSchema(BaseModel):
    station_code: str
    forecast_ts_list: List[int]
    tide_list: List[float]


class DistStationRealdataListSchema(BaseModel):
    """
        对应 mid model DistStationSurgeListMidModel
    """
    station_code: str
    surge_list: List[float]
    ts_list: List[int]

    class Config:
        # 对于自定义model也需要设置此配置
        orm_mode = True


class DistStationSurgeListSchema(BaseModel):
    station_code: str
    issue_ts: int
    surge_list_schema: StationSurgeListSchema


class DistStationWindListSchema(BaseModel):
    """
        + 24-04-09 风要素实况集合
    """
    station_code: str
    ws_list: List[float]
    ts_list: List[int]
    dir_list: List[int]

    class Config:
        # 对于自定义model也需要设置此配置
        orm_mode = True


class DistStationTotalSurgeSchema(BaseModel):
    """
        按照不同站点嵌套 总潮位 schema
    """
    station_code: str
    sort: int
    # station_total_schema: List[StationTotalSurgeSchema]
    forecast_ts_list: List[int]
    tide_list: List[float]
    surge_list: List[float]


class DistStationAlertLevel(BaseModel):
    """
        不同站点的警戒潮位
    """
    station_code: str
    alert_tide_list: List[float]
    alert_level_list: List[int]
