from common.enums import ElementTypeEnum


def factory_get_fubelements_val(element: ElementTypeEnum) -> str:
    """
        根据传入的要素获取对应的val
    @param element:
    @return:
    """
    dicts = {
        ElementTypeEnum.WS: 'WS',
        ElementTypeEnum.WD: 'WD',
        ElementTypeEnum.WSM: 'WSM',
        ElementTypeEnum.BP: 'BP',
        ElementTypeEnum.BG: 'BG',
        ElementTypeEnum.YBG: 'YBG',
    }
    val: str = dicts.get(element)
    return val
