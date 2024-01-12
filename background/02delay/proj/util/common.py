import arrow
import pathlib


def get_utc_year(ts: int) -> str:
    """
        根据时间戳获取 utc 年
    :param ts:
    :return:
    """

    current_arrow: arrow.Arrow = arrow.get(ts)
    year_str: str = current_arrow.format('yyyy')
    return year_str


def get_utc_month(ts: int) -> str:
    """
        根据时间戳获取 utc 月
    :param ts:
    :return:
    """
    current_arrow: arrow.Arrow = arrow.get(ts)
    month_str: str = current_arrow.format('mm')
    return month_str


def get_utc_day(ts: int) -> str:
    """
        根据时间戳获取 utc 日
    :param ts:
    :return:
    """
    current_arrow: arrow.Arrow = arrow.get(ts)
    day_str: str = current_arrow.format('dd')
    return day_str


def get_store_relative_path(ts: int) -> str:
    """
        获取存储的相对路径 yyyy/mm/dd
    :param ts:
    :return:
    """
    year: str = get_utc_year(ts)
    month: str = get_utc_month(ts)
    day: str = get_utc_day(ts)
    path = pathlib.Path(year) / month / day
    return str(path)
