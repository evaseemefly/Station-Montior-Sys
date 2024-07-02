import ast
import json
from typing import List, Optional, Any, Union
from datetime import datetime
import arrow
import pandas as pd
from sqlalchemy import select, update, func, and_, text, TextClause, union_all, MetaData, Table, Column, String, \
    Integer, Float, Date
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import func

from common.default import NAN_VAL
from common.enums import ElementTypeEnum, ObservationTypeEnum
from dao.station import get_remote_service
from mid_models.fub import DistFubListMidModel, FubListMidModel
from models.fub import FubPerclockDataModel

from dao.base import BaseDao
from util.common import get_diff_timestamp_list


class FubDao(BaseDao):
    def get_dist_fubs_realdata_list(self, start_ts: int, end_ts: int) -> List[DistFubListMidModel]:
        """
            根据起止时间获取所有浮标站点的实况集合
        :param start_ts:
        :param end_ts:
        :return:
        """
        return []

    def get_fubs_realdata_list_backup(self, codes: List[str], start_ts: int, end_ts: int) -> List[DistFubListMidModel]:
        """
            根据传入的 codes 获取起止时间范围内的所有观测要素的集合(fub)
            TODO:[*] 24-06-24 此部分逻辑应与 dao/station.py -> get_stations_realdata_list 一致

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

    def get_fubs_realdata_list(self, codes: List[str], start_ts: int, end_ts: int) -> List[DistFubListMidModel]:
        """
            根据传入的 codes 获取起止时间范围内的所有观测要素的集合(fub)
            TODO:[*] 24-06-24 此部分逻辑应与 dao/station.py -> get_stations_realdata_list 一致

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
        list_ts_standard: List[int] = get_diff_timestamp_list(start_ts, end_ts)
        """根据起止时间戳生成的标准化后的时间戳数组"""

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

            # TODO:[*] 24-06-24 浮标数据量较小，不需要分表，不需要实现获取浮标orm instance的方法
            # fub_stmt = select(FubPerclockDataModel.element_type, FubPerclockDataModel.value,
            #                   FubPerclockDataModel.station_code, FubPerclockDataModel.issue_ts).where(
            #     FubPerclockDataModel.station_code == temp_code, FubPerclockDataModel.issue_ts <= end_ts,
            #     FubPerclockDataModel.issue_ts >= start_ts)
            # fub_res = session.execute(fub_stmt).all()
            # list_res_ts: List[int] = [i[3] for i in fub_res]
            # list_res_val: List[float] = [i[1] for i in fub_res]
            # res_df:pd.DataFrame=pd.DataFrame({'ts':list_res_ts,''})
            # TODO:[*] 24-06-24 参考 dao/station.py -> get_stations_realdata_list 进行 dataframe 拼接的操作逻辑

            """
                返回结果 station_code,issue_ts_list,val_list,element_type
            """
            res_df: pd.DataFrame = None
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
                name_element = row.element_type
                # TODO:[*] 24-06-24 首次需要向 dataframe 中按列赋值
                if res_df is None:
                    res_df = pd.DataFrame({'ts': temp_ts_list})
                res_df[name_element] = temp_val_list
                # temp_fub_realdata: FubListMidModel = FubListMidModel(temp_code, temp_element, temp_ts_list,
                #                                                      temp_val_list)
                # fub_realdata_list.append(temp_fub_realdata)
                pass
            # 循环结束后对dataframe设置对应索引
            res_df.set_index('ts', inplace=True)
            res_aligend_df = res_df.reindex(list_ts_standard, fill_value=NAN_VAL)
            # 循环元素数组获取标准化后的对应元素的集合
            for temp_element in elements:
                temp_element_vals = res_aligend_df[temp_element.value].tolist()
                temp_fub_realdata: FubListMidModel = FubListMidModel(temp_code, temp_element.value, list_ts_standard,
                                                                     temp_element_vals)
                fub_realdata_list.append(temp_fub_realdata)
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
