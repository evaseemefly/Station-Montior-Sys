import arrow
import pathlib


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
    if dt_arrow.datetime.hour >= 16:
        yyyy = dt_arrow.format(format_str)
    else:
        # 0-15
        yyyy = dt_arrow.shift(days=-1).format(format_str)
    return yyyy


def get_localtime_month(ts: int) -> str:
    """
            本日 00->23 H
            对应utc时间 前一日:16H -> 本日:15H
    """
    format_str: str = 'MM'
    dt_arrow: arrow.Arrow = arrow.get(ts)
    if dt_arrow.datetime.hour >= 16:
        mm = dt_arrow.format(format_str)
    else:
        # 0-15
        mm = dt_arrow.shift(days=-1).format(format_str)
    return mm


def get_localtime_day(ts: int) -> str:
    """
            本日 00->23 H
            对应utc时间 前一日:16H -> 本日:15H
    """
    format_str: str = 'DD'
    dt_arrow: arrow.Arrow = arrow.get(ts)
    if dt_arrow.datetime.hour >= 16:
        dd = dt_arrow.format(format_str)
    else:
        # 0-15
        dd = dt_arrow.shift(days=-1).format(format_str)
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
    format_str: str = 'mmdd'
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
    """
        本日 00->23 H
        对应utc时间 前一日:16H -> 本日:15H
    """
    if dt_arrow.datetime.hour >= 16:
        mmdd = dt_arrow.format(format_str)
    else:
        # 0-15
        mmdd = dt_arrow.shift(days=-1).format(format_str)
    return mmdd
