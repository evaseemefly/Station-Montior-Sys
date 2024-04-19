import pathlib
from loguru import logger
import arrow
from conf.settings import LOG_DIR, LOG_FILE
from tasks.cases import delay_task, timer_download_station_realdata, timer_download_fub_realdata, \
    task_downloads_fub_byrange


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


def main():
    """入口方法"""
    logger.info(f'启动观测检测系统:{arrow.Arrow.utcnow().format("YYYY-MM-dd HH:MM")}')
    # 执行定时延时作业
    # delay_task()
    # timer_download_station_realdata()
    # timer_download_fub_realdata()
    start_dt: arrow.Arrow = arrow.Arrow(2024, 2, 20, 0, 0)
    end_dt: arrow.Arrow = arrow.Arrow(2024, 2, 24, 0, 0)
    task_downloads_fub_byrange(start_dt.int_timestamp, end_dt.int_timestamp, 1)


if __name__ == '__main__':
    main()
