import ast
import json
from typing import List, Optional, Any, Union
from datetime import datetime
import arrow
from sqlalchemy import select, update, func, and_, text, TextClause, union_all, MetaData, Table, Column, String, \
    Integer, Float, Date
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import func

from common.enums import ElementTypeEnum, ObservationTypeEnum
from dao.station import get_remote_service
from mid_models.fub import DistFubListMidModel, FubListMidModel
from models.fub import FubPerclockDataModel

from dao.base import BaseDao


class FubDao(BaseDao):
    def get_dist_fubs_realdata_list(self, start_ts: int, end_ts: int) -> List[DistFubListMidModel]:
        """
            根据起止时间获取所有浮标站点的实况集合
        :param start_ts:
        :param end_ts:
        :return:
        """
        return []

    def get_fubs_realdata_list(self, codes: List[str], start_ts: int, end_ts: int) -> List[DistFubListMidModel]:
        """
            根据传入的 codes 获取起止时间范围内的所有观测要素的集合(fub)
        :param codes:
        :param end_ts:
        :return:
        """
        session = self.db.session
        elements: List[ElementTypeEnum] = [ElementTypeEnum.WS, ElementTypeEnum.WD, ElementTypeEnum.BG,
                                           ElementTypeEnum.BP, ElementTypeEnum.YBG, ElementTypeEnum.WSM]
        """要素枚举集合"""

        tab_name: str = FubPerclockDataModel.get_tab_name()
        fubs_obserivation_list: List[DistFubListMidModel] = []

        for temp_code in codes:
            fub_realdata_list: List[FubListMidModel] = []
            sql_str: text = text(f"""
                        SELECT station_code,
                            GROUP_CONCAT(issue_ts ORDER BY issue_ts) as issue_ts_list,
                            GROUP_CONCAT(value ORDER BY issue_ts) as val_list,
                            element_type
                            FROM {tab_name}
                            WHERE {tab_name}.issue_ts >= {start_ts}
                              AND {tab_name}.issue_ts <= {end_ts}
                              AND {tab_name}.station_code='{temp_code}'
                            GROUP BY element_type
                    """)
            res = session.execute(sql_str)
            """
                返回结果 station_code,issue_ts_list,val_list,element_type
            """
            for row in res:
                temp_element = ElementTypeEnum(row.element_type)

                temp_ts_str_list: List[str] = row.issue_ts_list.split(',')
                temp_ts_list: List[int] = []
                temp_val_str_list: List[str] = row.val_list.split(',')
                temp_val_list: List[Union[float, int]] = []
                # TODO:[*] 24-04-23 此处可能出现的bug时缺少对时间戳的验证，可能会出现中断的问题
                if len(temp_ts_str_list) == len(temp_val_str_list):
                    for temp_ts_str in temp_ts_str_list:
                        if temp_ts_str != '' or temp_ts_str != ',':
                            temp_ts_list.append(int(temp_ts_str))
                    for temp_val_str in temp_val_str_list:
                        if temp_val_str != '' or temp_val_str != ',':
                            temp_val_list.append(ast.literal_eval(temp_val_str))
                temp_fub_realdata: FubListMidModel = FubListMidModel(temp_code, temp_element, temp_ts_list,
                                                                     temp_val_list)
                fub_realdata_list.append(temp_fub_realdata)
                pass
            fubs_obserivation_list.append(DistFubListMidModel(temp_code, ObservationTypeEnum.FUB, fub_realdata_list))
        return fubs_obserivation_list

    def get_all_fubs_codes(self) -> List[str]:
        """
            获取所有站点的浮标codes
        :return:
        """
        res_content = get_remote_service('/fub/dist/codes', {})
        return json.loads(res_content)

    def get_all_fubs(self) -> List[dict]:
        """
            获取所有fubs的信息
        :return:
        """
        res_content = get_remote_service('/fub/all/info', {})
        return json.loads(res_content)
