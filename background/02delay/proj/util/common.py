import arrow
import pathlib

from common.enums import ElementTypeEnum


def get_utc_year(ts: int) -> str:
    """
        根据时间戳获取 utc 年
    :param ts:
    :return:
    """

    current_arrow: arrow.Arrow = arrow.get(ts)
    year_str: str = current_arrow.format('YYYY')
    return year_str


def get_utc_month(ts: int) -> str:
    """
        根据时间戳获取 utc 月
    :param ts:
    :return:
    """
    current_arrow: arrow.Arrow = arrow.get(ts)
    month_str: str = current_arrow.format('MM')
    return month_str


def get_localtime_year(ts: int) -> str:
    """
            本日 00->23 H
            对应utc时间 前一日:16H -> 本日:15H
    """
    format_str: str = 'YYYY'
    dt_arrow: arrow.Arrow = arrow.get(ts)
    yyyy = dt_arrow.shift(days=1).format(format_str)
    return yyyy


def get_localtime_month(ts: int) -> str:
    """
            本日 00->23 H
            对应utc时间 前一日:16H -> 本日:15H
    """
    format_str: str = 'MM'
    dt_arrow: arrow.Arrow = arrow.get(ts)
    # 0-15
    mm = dt_arrow.shift(days=1).format(format_str)
    return mm


def get_localtime_day(ts: int) -> str:
    """
            本日 00->23 H
            对应utc时间 前一日:16H -> 本日:15H
    """
    format_str: str = 'DD'
    dt_arrow: arrow.Arrow = arrow.get(ts)
    # 0-15
    dd = dt_arrow.shift(days=1).format(format_str)
    return dd


def get_utc_day(ts: int) -> str:
    """
        根据时间戳获取 utc 日
    :param ts:
    :return:
    """
    current_arrow: arrow.Arrow = arrow.get(ts)
    day_str: str = current_arrow.format('DD')
    return day_str


def get_store_relative_exclude_day(ts: int) -> str:
    """
        根据当前时间获取 存储相对路径 (fub为例)
        /2024/04
    @param ts:
    @return:
    """
    year: str = get_localtime_year(ts)
    month: str = get_localtime_month(ts)
    path = pathlib.Path(year) / month
    return str(path)


def get_store_relative_path(ts: int) -> str:
    """
        获取存储的相对路径 yyyy/mm/dd
    :param ts:
    :return:
    """
    # TODO:[*] 24-02-26 注意此处修改为根据本地时间获取存储路径
    year: str = get_localtime_year(ts)
    month: str = get_localtime_month(ts)
    day: str = get_localtime_day(ts)
    path = pathlib.Path(year) / month / day
    return str(path)


def get_station_start_ts(ts: int) -> int:
    """
        + 24-02-29
          将当前传入的时间戳提取对应的站点起始时间戳
          local [前一日 20-23],[次日 0-19]
          utc   [前一日 12-15],[前一日 16- 次日11]
          utc   [前一日 12-23],[次日 0-11]
          返回的时间为 前一日 local 20,utc,12
    @param ts:
    @return:
    """
    current_arrow: arrow.Arrow = arrow.get(ts)
    hour_str: str = current_arrow.format('HH')
    hour_int: int = int(hour_str)
    stand_start_ts: int = 0
    """传入时间戳对应的小时(utc)"""
    if hour_int < 12:
        # 前一日
        stand_date_format: str = current_arrow.shift(days=-1).format('YYYYMMDD')
        stand_dt: arrow.Arrow = arrow.get(f'{stand_date_format}12', 'YYYYMMDDHH')
        stand_start_ts = stand_dt.int_timestamp
    else:
        # 当日
        stand_date_format: str = current_arrow.format('YYYYMMDD')
        # TODO:[*] 24-07-17 ERROR: arrow.parser.ParserError: Could not match input '2024-07-0112'
        stand_dt: arrow.Arrow = arrow.get(f'{stand_date_format}12', 'YYYYMMDDHH')
        stand_start_ts = stand_dt.int_timestamp
    return stand_start_ts


def get_filestamp(ts: int) -> str:
    """
        根据时间戳获取当前时间戳对应的时间日期(mmdd)
        对应要素:风 20-19(local)
    :param ts:
    :return:
    """
    mmdd: str = ''
    dt_arrow: arrow.Arrow = arrow.get(ts)
    """mmdd 字符串戳"""
    format_str: str = 'MMDD'
    # 传入的时间对应的 dt
    dt_arrow: arrow.Arrow = arrow.get(ts)
    """
        保存前一日 20H- 本日 19H
        对应utc时间 前一日:12H -> 本日:11H
    """
    if dt_arrow.datetime.hour <= 11:
        mmdd = dt_arrow.format(format_str)
    else:
        mmdd = dt_arrow.shift(days=-1).format(format_str)
    return mmdd


def get_standard_datestamp(ts: int, element_type: ElementTypeEnum = ElementTypeEnum.SURGE):
    """
        + 24-04-09
        潮位要素整点数据的起止时间为本地时:00,风要素起始时间为前一日:21
        根据标准化后的时间戳获取对应的日期戳(标准化)
    @param ts:
    @param element_type:
    @return:
    """
    format_str: str = 'YYYYMMDD'
    # 传入的时间对应的 dt
    dt_arrow: arrow.Arrow = arrow.get(ts)
    date_str: str = ''
    if element_type in [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]:
        date_str = dt_arrow.shift(days=1).format(format_str)
    return date_str


def get_calendarday_filestamp(ts: int) -> str:
    """
        根据传入时间获取对应的自然日的 日期 stamp (mmdd)
        对应要素:潮位 00-23(local)
    :param ts:
    :return:
    """
    mmdd: str = ''
    dt_arrow: arrow.Arrow = arrow.get(ts)
    """mmdd 字符串戳"""
    format_str: str = 'MMDD'
    # 传入的时间对应的 dt
    dt_arrow: arrow.Arrow = arrow.get(ts)
    mmdd = dt_arrow.shift(days=1).format(format_str)
    return mmdd


def get_fulltime_stamp(ts: int) -> str:
    """
        根据当前传入的时间戳获取对应的时间 str
        YYYYMMDDHHmm
        返回的日期戳为世界时(utc)
    @param ts: 当前时间戳
    @return:
    """
    format_str: str = 'YYYYMMDDHHmm'
    # 传入的时间对应的 dt
    dt_arrow: arrow.Arrow = arrow.get(ts)
    date_str: str = ''
    '''时间str戳'''
    date_str = dt_arrow.format(format_str)
    return date_str


def get_local_fulltime_stamp(ts: int) -> str:
    """
        根据当前传入的时间戳获取对应的时间 str
        YYYYMMDDHHmm
        返回的日期戳为世界时(本地时)
    @param ts: 当前时间戳
    @return:
    """
    format_str: str = 'YYYYMMDDHHmm'
    # 传入的时间对应的 dt
    dt_arrow: arrow.Arrow = arrow.get(ts).shift(hours=8)
    date_str: str = ''
    '''时间str戳'''
    date_str = dt_arrow.format(format_str)
    return date_str
