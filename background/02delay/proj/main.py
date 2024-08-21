import argparse
import pathlib
import sys
import time

from loguru import logger
import arrow

from common.enums import RunTypeEnmum
from conf.settings import LOG_DIR, LOG_FILE
from tasks.cases import delay_task, timer_download_station_realdata, timer_download_fub_realdata, \
    task_downloads_fub_byrange, task_downloads_station_byrange, task_downloads_slb_byrange


def init_logging():
    """
        + 23-12-11
        完成对于 loguru logging 的配置
        1- 配置指定目录
        2- 配置每个日志文件的大小
    :return:
    """
    log_full_path: str = str(pathlib.Path(f'{LOG_DIR}/{LOG_FILE}'))
    """按照文件大小对日志进行切分"""
    # 判断日志目录是否存在不存在创建
    if not pathlib.Path(LOG_DIR).exists():
        pathlib.Path(LOG_DIR).mkdir()
    logger.add(log_full_path, rotation='200KB')
    logger.info('logger初始化完毕!')

    pass


def main(run_type: RunTypeEnmum = RunTypeEnmum.DELATY_TASK, start: str = '19700101', end: str = '19700101'):
    """
        执行 站点定时录入|站点立即录入时间段数据
    @param run_type:
    @return:
    """
    # TODO:[*] 24-07-31 加入了关于日志的配置——控制台输出带颜色+日志文件
    # 自定义格式字符串
    # format_string = "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {module:<15} | {function:<20} | {message}"
    format_string = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level:<8}</level> | "
        "<cyan>{module:<15}</cyan> | "
        "<cyan>{function:<20}</cyan> | "
        "<level>{message}</level>"
    )
    # 配置 logger
    logger.remove()  # 移除默认的日志配置
    """
        使用 logger.add() 添加了一个新的日志配置，
        指定了日志文件名为 logfile_{time:YYYY-MM-DD}.log，
        并设置了自定义的格式字符串 format_string，去除了颜色标签。    
        设置了 rotation="00:00" 参数，表示每天午夜分割日志文件。
    """
    logger.add("logfile.log",
               format=format_string.replace('<level>', '').replace('</level>', '').replace('<green>', '').replace(
                   '</green>', '').replace('<cyan>', '').replace('</cyan>', ''), level="DEBUG", rotation="00:00")
    """入口方法"""
    logger.info(f'启动观测检测系统:{arrow.Arrow.utcnow().format("YYYY-MM-dd HH:MM")}')
    """
        再次使用 logger.add() 添加了一个控制台输出配置，使用带有颜色标签的格式字符串 format_string。
    """
    logger.add(sys.stdout, format=format_string, level="DEBUG")
    # 执行定时延时作业
    # delay_task()
    # timer_downsa`1    load_station_realdata()
    # timer_download_fub_realdata()

    start_dt: arrow.Arrow = arrow.get(start, 'YYYYMMDD').shift(hours=-8)

    end_dt: arrow.Arrow = arrow.get(end, 'YYYYMMDD').shift(hours=-8)
    switch_task = {
        RunTypeEnmum.DELATY_TASK: delay_task,
        RunTypeEnmum.DATAENTRY_STATION_RANGE: task_downloads_station_byrange,
        RunTypeEnmum.DATAENTRY_SLB_RANGE: task_downloads_slb_byrange,
        RunTypeEnmum.DATAENTRY_FUB_RANGE: task_downloads_fub_byrange,
    }
    task_func = switch_task.get(run_type)
    # task_downloads_fub_byrange(start_dt.int_timestamp, end_dt.int_timestamp, 1)
    # task_downloads_station_byrange(start_dt.int_timestamp, end_dt.int_timestamp)
    # task_downloads_slb_byrange(start_dt.int_timestamp, end_dt.int_timestamp)
    task_func(start_dt.int_timestamp, end_dt.int_timestamp)


if __name__ == '__main__':
    """
        + 24-08-07 新加入了执行main.py时需提供输入参数
    """
    now_str = arrow.get().format('YYYYMMDD')
    parser = argparse.ArgumentParser(description=f'当前时间:{now_str},传入运行时参数')
    parser.add_argument('--type', help='运行类型', default=RunTypeEnmum.DELATY_TASK.value, type=int)
    parser.add_argument('--start', help='起始时间字符串(yyyymmdd)', default=now_str, type=int)
    parser.add_argument('--end', help='起始时间字符串(yyyymmdd)', default=now_str, type=int)
    args = parser.parse_args()
    run_type: RunTypeEnmum = RunTypeEnmum(args.type)
    main(run_type=run_type, start=str(args.start), end=str(args.end))
    while True:
        time.sleep(5)
    pass
