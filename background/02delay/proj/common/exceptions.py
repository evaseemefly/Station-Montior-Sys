"""
    + 24-01-19 自定义异常
"""


class FtpDownLoadError(Exception):
    """
        fpt 下载异常
    """
    pass


class FileReadError(Exception):
    """
        文件读取错误
    """
    pass


class FileFormatError(Exception):
    """
        文件格式错误
    """
    pass
