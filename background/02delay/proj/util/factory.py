"""
    + 24-01-17 各工厂类
"""
from common.enums import ElementTypeEnum
from core.files import IStationFile, SurgeFile, WindFile, FubFile
from core.operaters import WindOperate, SurgeOperate, FubOperate
from util.ftp import FtpClient


def factory_get_station_file(element: ElementTypeEnum):
    dict_file = {
        ElementTypeEnum.SURGE: SurgeFile,
        ElementTypeEnum.WIND: WindFile,
        ElementTypeEnum.FUB: FubFile
    }

    file_cls = dict_file.get(element)
    return file_cls
    # # 实例化
    # ftp = FtpClient()
    #
    # file_cls(ftp,)


def factory_get_operater(element: ElementTypeEnum):
    dict_operate = {
        ElementTypeEnum.SURGE: SurgeOperate,
        ElementTypeEnum.WIND: WindOperate,
        ElementTypeEnum.FUB: FubOperate
    }

    operate_cls = dict_operate.get(element)
    return operate_cls
