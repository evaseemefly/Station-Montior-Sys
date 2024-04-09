# 质控方法

DEFAULT_VAL_LIST = [999, 9999, 9998, 999.9, 9999.9]
"""缺省列表"""


def is_standard_ws(val: float):
    """
        对 风速 进行标准化判断
    @param val:
    @return:
    """
    is_standard: bool = False
    # 当前值不在缺省列表中
    if val in DEFAULT_VAL_LIST:
        pass
    else:
        is_standard = True
    return is_standard
