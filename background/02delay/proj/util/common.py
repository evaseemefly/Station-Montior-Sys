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


def get_localtime_year_bydt(dt_utc: arrow.Arrow) -> str:
    """
        TODO:[-] 24-09-15 统一修改为在外侧处理
        获取当前传入的 utc dt 对应的本地时 yyyy
    @param dt_utc:
    @return:
    """
    format_str: str = 'YYYY'
    yyyy = dt_utc.shift(hours=8).format(format_str)
    return yyyy


def get_localtime_year(ts: int) -> str:
    """
            本日 00->23 H
            对应utc时间 前一日:16H -> 本日:15H
    """
    format_str: str = 'YYYY'
    dt_arrow: arrow.Arrow = arrow.get(ts)
    yyyy = dt_arrow.shift(days=1).format(format_str)
    return yyyy


def get_localtime_month_bydt(dt_utc: arrow.Arrow) -> str:
    """
        TODO:[-] 24-09-15 统一修改为在外侧处理
        获取当前传入的 utc dt 对应的本地时 mm
    @param dt_utc:
    @return:
    """
    format_str: str = 'MM'
    mm = dt_utc.shift(hours=8).format(format_str)
    return mm


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


def get_localtime_day_bydt(dt_utc: arrow.Arrow) -> str:
    """
        TODO:[-] 24-09-15 统一修改为在外侧处理
        获取当前传入的 utc dt 对应的本地时 mm
    @param dt_utc:
    @return:
    """
    format_str: str = 'DD'
    dd = dt_utc.shift(hours=8).format(format_str)
    return dd


def get_localtime_day(ts: int) -> str:
    """
            本日 00->23 H
            对应utc时间 前一日:16H -> 本日:15H
    """
    format_str: str = 'DD'
    dt_arrow: arrow.Arrow = arrow.get(ts)
    # 0-15
    # TODO:[*] 24-09-15 此处的意义是？
    dd = dt_arrow.shift(days=1).format(format_str)
    return dd


def get_tomorrow_local_day(ts: int) -> str:
    format_str: str = 'DD'
    dt_arrow: arrow.Arrow = arrow.get(ts)
    # 0-15
    # TODO:[*] 24-09-15 此处的意义是？
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


def get_store_relative_path(ts: int, element_type: ElementTypeEnum = ElementTypeEnum.SURGE) -> str:
    """
        获取存储的相对路径 yyyy/mm/dd
    :param ts:
    :return:
    """
    # TODO:[*] 24-02-26 注意此处修改为根据本地时间获取存储路径
    # TODO:[-] 24-09-15 修改存储路径 此次修改引发了bug
    # year: str = get_localtime_year(ts)
    # month: str = get_localtime_month(ts)
    # day: str = get_localtime_day(ts)

    dt_utc: arrow.Arrow = arrow.get(ts)
    '''当前传入的 ts 的utc时间 dt'''
    dt_local: arrow.Arrow = dt_utc.shift(hours=8)
    '''当前传入的 ts 的本地时间 dt'''
    hour_local: int = dt_local.datetime.hour
    '''本地时间的小时 h'''
    if element_type == ElementTypeEnum.WIND and hour_local >= 21:
        # TODO:[-] 24-09-15 对当前时间 + 4h 由于21时起为下一日 此处引发了bug，重新修改
        # TODO:[-] 24-09-19 只有风要素的本地时间 >=20H 文件存储于 day+1 日目录下
        dt_local: arrow.Arrow = dt_local.shift(days=1)

    # year: str = get_localtime_year_bydt(dt_utc)
    # month: str = get_localtime_month_bydt(dt_utc)
    # day: str = get_localtime_day_bydt(dt_utc)
    year: str = dt_local.format('YYYY')
    month: str = dt_local.format('MM')
    day: str = dt_local.format('DD')
    path = pathlib.Path(year) / month / day
    return str(path)


def get_station_start_ts(ts: int, element_type: ElementTypeEnum = ElementTypeEnum.SURGE) -> int:
    """
        + 24-02-29
          将当前传入的时间戳提取对应的站点起始时间戳
          local [前一日 20-23],[次日 0-19]
          utc   [前一日 12-15],[前一日 16- 次日11]
          utc   [前一日 12-23],[次日 0-11]
          返回的时间为 前一日 local 20,utc,12

        TODO:[-] 24-09-19 此处根据 surge 与 wind 进行了修改
    @param ts:
    @return:
    """
    current_arrow: arrow.Arrow = arrow.get(ts)
    hour_str: str = current_arrow.format('HH')
    hour_int: int = int(hour_str)
    stand_start_ts: int = 0
    """传入时间戳对应的小时(utc)"""
    '''
        TODO:[-] 24-09-19 
            判断 WL 与 WS:
                WL: localtime [0,23]
                    utc       [16,23] 前一日 ; [0,15] 当日
                WS: localtime [21,19] 
                    utc       [13,23] 前一日 ; [0,12] 当日
    '''
    # if hour_int < 12:
    #     # 前一日
    #     stand_date_format: str = current_arrow.shift(days=-1).format('YYYYMMDD')
    #     stand_dt: arrow.Arrow = arrow.get(f'{stand_date_format}12', 'YYYYMMDDHH')
    #     stand_start_ts = stand_dt.int_timestamp
    # else:
    #     # 当日
    #     stand_date_format: str = current_arrow.format('YYYYMMDD')
    #     # TODO:[*] 24-07-17 ERROR: arrow.parser.ParserError: Could not match input '2024-07-0112'
    #     stand_dt: arrow.Arrow = arrow.get(f'{stand_date_format}12', 'YYYYMMDDHH')
    #     stand_start_ts = stand_dt.int_timestamp
    if element_type == ElementTypeEnum.SURGE:
        if hour_int >= 16:
            # 当前日期(local)
            stand_date_format: str = current_arrow.shift(hours=8).format('YYYYMMDD')
            stand_dt: arrow.Arrow = arrow.get(f'{stand_date_format}12', 'YYYYMMDDHH')
            stand_start_ts = stand_dt.int_timestamp
        else:
            # [0,15]
            stand_date_format: str = current_arrow.format('YYYYMMDD')
            stand_dt: arrow.Arrow = arrow.get(f'{stand_date_format}12', 'YYYYMMDDHH')
            stand_start_ts = stand_dt.int_timestamp
    elif element_type == ElementTypeEnum.WIND:
        if hour_int >= 13:
            # 当前日期(local)
            stand_date_format: str = current_arrow.shift(days=1).format('YYYYMMDD')
            stand_dt: arrow.Arrow = arrow.get(f'{stand_date_format}12', 'YYYYMMDDHH')
            stand_start_ts = stand_dt.int_timestamp
        else:
            # [0,12]
            stand_date_format: str = current_arrow.format('YYYYMMDD')
            stand_dt: arrow.Arrow = arrow.get(f'{stand_date_format}12', 'YYYYMMDDHH')
            stand_start_ts = stand_dt.int_timestamp
    return stand_start_ts


def get_fub_start_ts(ts: int) -> int:
    """
        TODO:[*] 24-08-21 根据当前时间获取对应的本地整点时间戳
    @param ts:
    @return:
    """
    current_arrow: arrow.Arrow = arrow.get(ts)
    return current_arrow.floor('hour').int_timestamp


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


def get_calendarday_filestamp(ts: int, element_type: ElementTypeEnum = ElementTypeEnum.SURGE) -> str:
    """
        根据传入时间获取对应的自然日的 日期 stamp (mmdd)
        对应要素:潮位 00-23(local)
        TODO:[-] 24-09-19 与 get_store_relative_path 的获取时间戳逻辑一致
    :param ts:
    :return:
    """
    mmdd: str = ''
    dt_arrow: arrow.Arrow = arrow.get(ts)
    """mmdd 字符串戳"""
    format_str: str = 'MMDD'
    # 传入的时间对应的 dt
    dt_utc: arrow.Arrow = arrow.get(ts)
    '''当前传入的 ts 的utc时间 dt'''
    dt_local: arrow.Arrow = dt_utc.shift(hours=8)
    '''当前传入的 ts 的本地时间 dt'''
    hour_local: int = dt_local.datetime.hour
    '''本地时间的小时 h'''
    # TODO:[-] 24-09-16 此处修改为本地时
    # mmdd = dt_arrow.shift(days=1).format(format_str)
    # TODO:[*] 24-09-19 此处引发了新的bug，统一与get_store_relative_path相同的处理方式
    # if element_type == ElementTypeEnum.SURGE:
    #     mmdd = dt_arrow.shift(hours=8).format(format_str)
    # elif element_type == ElementTypeEnum.WIND:
    #     # TODO:[-] 24-09-15 风要素每日21时起是在明日的文件夹中
    #     dt_local: arrow.Arrow = dt_arrow.shift(hours=8)
    #     if int(dt_local.format('HH')) >= 21:
    #         mmdd = dt_local.shift(days=1).format(format_str)
    #     else:
    #         mmdd = dt_local.format(format_str)
    if element_type == ElementTypeEnum.WIND and hour_local >= 21:
        # TODO:[-] 24-09-19 只有风要素的本地时间 >=20H 文件存储于 day+1 日目录下
        dt_local: arrow.Arrow = dt_local.shift(days=1)
    mmdd = dt_local.format('MMDD')
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


def get_local_fulltime_stamp(ts: int, is2rounding: bool = True) -> str:
    """
        根据当前传入的时间戳获取对应的时间 str
        YYYYMMDDHHmm
        返回的日期戳为世界时(本地时)
    @param ts: 当前时间戳
    @return:
    """
    # 传入的时间对应的 dt
    dt_arrow: arrow.Arrow = arrow.get(ts).shift(hours=8)
    date_str: str = ''
    '''时间str戳'''
    if is2rounding:
        format_str: str = 'YYYYMMDDHH'
        date_str = f'{dt_arrow.format(format_str)}00'
    else:
        format_str: str = 'YYYYMMDDHHmm'
        date_str = f'{dt_arrow.format(format_str)}'
    return date_str
