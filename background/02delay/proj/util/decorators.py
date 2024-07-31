# 23-12-11 用来记录请求的日志装饰器
import datetime
from functools import wraps
from typing import Any, Optional, Callable

import arrow
from loguru import logger

from common.exceptions import FtpDownLoadError, FileFormatError, FileReadError


def decorator_timer_consuming(func: Optional[Callable] = None):
    """
        方法执行耗时计时器
    :param func:
    :return:
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any):
        dt_str: str = arrow.Arrow.utcnow().format('yyyy-mm-dd HH:MM')
        # TODO:[-] 23-12-11 注意 old 不要放在 wrapper 方法外侧，装饰器方法在首次加载时会执行request_log_decorator ，但不会执行 wrapper 方法
        old: float = datetime.datetime.now().timestamp()
        # logger.debug(f'接收到请求体为:{url}')
        res = func(*args, **kwargs)
        now: float = datetime.datetime.now().timestamp()
        timer_consuming: float = now - old
        timer_consuming_str: str = '%.2f' % timer_consuming
        logger.debug(f'func:{func}|consuming time:{timer_consuming_str}')
        return res

    return wrapper


def decorator_exception_logging(func: Optional[Callable] = None):
    """
        异常捕捉并记录日志
    :param func:
    :return:
    """

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any):
        dt_str: str = arrow.Arrow.utcnow().format('yyyy-mm-dd HH:MM')
        res: Any = None
        try:
            # TODO:[-] 23-12-11 注意 old 不要放在 wrapper 方法外侧，装饰器方法在首次加载时会执行request_log_decorator ，但不会执行 wrapper 方法
            res = await func(*args, **kwargs)
        except FtpDownLoadError as ftp:
            logger.error(f'now:{dt_str}|func:{func}|error:{ftp.args}')
        except FileFormatError as file:
            logger.error(f'now:{dt_str}|func:{func}|error:{file.args}')
        except FileReadError as reader:
            logger.error(f'now:{dt_str}|func:{func}|error:{reader.args}')
        except Exception as ex:
            logger.error(f'now:{dt_str}|func:{func}|error:{ex.args}')
        return res

    return wrapper
