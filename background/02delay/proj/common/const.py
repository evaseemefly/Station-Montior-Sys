from typing import List

from common.enums import ElementTypeEnum
from mid_models.stations import StationElementMidModel

# TODO:[*] 24-02-27 注意例如站点不存在某个要素的整点数据，例如潮位数据WL 莆田不存在，则会报错
LIST_STATIONS: List[StationElementMidModel] = [
    # part1: 北海
    # ---
    StationElementMidModel('DGG', '01116', '东港', [ElementTypeEnum.SURGE]),
    StationElementMidModel('XCS', '01114', '小长山', [ElementTypeEnum.SURGE]),
    # StationElementMidModel('', '01217', '皮口', [ElementTypeEnum.SURGE]),
    StationElementMidModel('LHT', '01146', '老虎滩', [ElementTypeEnum.SURGE]),
    # StationElementMidModel('', '长兴岛', '0', [ElementTypeEnum.SURGE]),
    StationElementMidModel('BYQ', '01111', '鲅鱼圈', [ElementTypeEnum.SURGE]),
    # StationElementMidModel('', '营口', '0', [ElementTypeEnum.SURGE]),
    # StationElementMidModel('', '锦州', '0', [ElementTypeEnum.SURGE]),
    # StationElementMidModel('', '盘锦', '0', [ElementTypeEnum.SURGE]),
    StationElementMidModel('HLD', '01120', '葫芦岛', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('ZMW', '01121', '芷锚湾', [ElementTypeEnum.SURGE]),
    StationElementMidModel('QHD', '03122', '秦皇岛', [ElementTypeEnum.SURGE]),
    StationElementMidModel('JTG', '03124', '京唐港', [ElementTypeEnum.SURGE]),
    # StationElementMidModel('', '03211', '唐山三岛', [ElementTypeEnum.SURGE]),
    StationElementMidModel('CFD', '03126', '曹妃甸', [ElementTypeEnum.SURGE]),
    StationElementMidModel('CXQ', '03212', '曹妃甸新区', [ElementTypeEnum.SURGE]),  # TODO: 核对中文名
    StationElementMidModel('TGU', '02123', '塘沽', [ElementTypeEnum.SURGE]),
    StationElementMidModel('HHA', '03125', '黄骅', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('BZG', '04223', '滨州港', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('', '04167', '黄河海港', [ElementTypeEnum.SURGE]),
    StationElementMidModel('DYG', '04170', '东营港', [ElementTypeEnum.SURGE]),
    StationElementMidModel('GUD', '04166', '孤东', [ElementTypeEnum.SURGE]),
    # StationElementMidModel('', '04242', '垦东', [ElementTypeEnum.SURGE]),
    # StationElementMidModel('', '04215', '羊口港', [ElementTypeEnum.SURGE]),
    StationElementMidModel('WFG', '04163', '潍坊', [ElementTypeEnum.SURGE]),
    StationElementMidModel('LKO', '04131', '龙口', [ElementTypeEnum.SURGE]),
    StationElementMidModel('PLI', '04151', '蓬莱', [ElementTypeEnum.SURGE]),
    StationElementMidModel('ZFD', '04152', '芝罘岛', [ElementTypeEnum.SURGE]),
    # StationElementMidModel('', '04130', '北隍城', [ElementTypeEnum.SURGE]),
    StationElementMidModel('BHC', '04130', '北隍城', [ElementTypeEnum.SURGE]),
    # StationElementMidModel('', '04239', '小石岛', [ElementTypeEnum.SURGE]),
    StationElementMidModel('CST', '04133', '成山头', [ElementTypeEnum.SURGE]),
    StationElementMidModel('WDG', '04164', '文登', [ElementTypeEnum.SURGE]),
    StationElementMidModel('SID', '04134', '石岛', [ElementTypeEnum.SURGE]),
    # StationElementMidModel('', '04240', '南黄岛', [ElementTypeEnum.SURGE]),
    StationElementMidModel('TNH', '04241', '田横', [ElementTypeEnum.SURGE]),
    StationElementMidModel('QLY', '04141', '千里岩', [ElementTypeEnum.SURGE]),
    StationElementMidModel('XMD', '04142', '小麦岛', [ElementTypeEnum.SURGE]),
    StationElementMidModel('WMT', '04143', '五码头', [ElementTypeEnum.SURGE]),
    StationElementMidModel('RZH', '04144', '日照', [ElementTypeEnum.SURGE]),
    # ---
    # part2: 东海
    StationElementMidModel('LYG', '06452', '连云港', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '0'      , '燕尾'  ,    [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '06453'  , '滨海'  ,    [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '0'      , '射阳'  ,    [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW','06410'  ,  '新洋港' ,   [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW','06514'  ,  '大丰港' ,   [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('WKJ', '06415', '外磕角', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '06417', '竹根沙', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('YKG', '06454', '洋口港', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '06418', '火星沙', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('LSI', '06411', '吕泗', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '06523', '连兴港', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '0'      , '堡镇'  ,    [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),

    StationElementMidModel('CMG', '05454', '崇明', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '0', '新村沙', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '06414', '佘山', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '0'       '高桥', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '0'       '吴淞', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('HPG', '', '黄浦公园', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),   # TODO:
    StationElementMidModel('LCG', '05453', '芦潮港', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '0', '金山嘴', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '05452', '东大桥', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '0'       '滩浒', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('DJS', '07415', '大戢山', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '06416', '小衢山', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('SHS', '07417', '嵊山', [ElementTypeEnum.SURGE]),
    # StationElementMidModel('SHW', '0'       '乍浦', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHW', '0', '澉浦', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),

    StationElementMidModel('DAI', '07509', '岱山', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '0', '长白', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('DHI', '0', '定海', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('ZHI', '07447', '镇海', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('SJM', '07448', '沈家门', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '07452', '六横', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('BLN', '07428', '北仑', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '0', '松兰山', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),

    StationElementMidModel('WSH', '07453', '乌沙山', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('SPU', '07421', '石浦', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('JAT', '07454', '健跳', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '07454'   '三门', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '0', '三门核电', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '07455', '椒江', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('HMZ', '0', '海门Z', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO: 无
    StationElementMidModel('DCH', '07422', '大陈', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '07550' '石塘', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('KMN', '07424', '坎门', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '07607', '西门岛', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '07456', '沙港头', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),

    # StationElementMidModel('SHS', '07460', '大门', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('DTO', '07450', '洞头', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '07451', '瓯江口', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('LGW', '', '龙湾', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO: 无
    # StationElementMidModel('SHS', '07459', '瑞安', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '07457', '龙港', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '07425', '南麂', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # 24-08-05
    # StationElementMidModel('SHS', '07458', '石砰', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', 'F5101', '前岐', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', '08429', '沙埕', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('QYU', '08447', '秦屿', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('SHA', '08430', '三沙', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('SHS', 'F5103', '白马港', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('BSH', '08432', '北礵', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('CAO', 'F5114', '城澳', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:[*]
    # StationElementMidModel('SHS', 'F5102', '东冲', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('BJA', '08433', '北茭', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('QGY', 'F5104', '青屿', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:[*]
    # StationElementMidModel('QGY', '0', '长门', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('CGM', '08444', '长门', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '0', '琯头', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '0', '白岩潭', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '0', '梅花', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('TNT', 'F5105', '潭头', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:[*]
    StationElementMidModel('PTN', '08440', '平潭', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('FQH', 'F5106', '福清核电', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:[*]
    StationElementMidModel('SHC', 'F5107', '石城', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:[*]
    # StationElementMidModel('QGY', '08525', '秀屿', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', 'F5108', '湄州', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('FHW', 'F5109', '峰尾', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('CHW', '08441', '崇武', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '0', '崇武S', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('JJH', '08451', '晋江', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('SJH', 'F5110', '石井', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '08448', '龙海', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '0', '高崎', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('XMN', '08442', '厦门', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '08449', '翔安', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '0', '石码', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('JZH', '0', '旧镇', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO: 无
    # StationElementMidModel('QGY', 'F5111', '六鳌', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('GUL', 'F5112', '古雷', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('DSH', '08443', '东山', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('CSW', 'F5113', '赤石湾', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('RPG', '09863', '饶平', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('YAO', '09710', '云澳', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '0', '南澳岛', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),

    # ------
    # part3: 南海
    # StationElementMidModel('QGY', '0', '东溪口', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '0', '汕头', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('HMN', '0', '海门G', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),# TODO: 无
    StationElementMidModel('STO', '09735', '汕头S', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('HLA', '09866', '惠来', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('LFG', '09867', '陆丰', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('ZHL', '0', '遮浪', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]), # TODO
    StationElementMidModel('SHW', '09711', '汕尾', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '0', '港口', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('HZO', '09740', '惠州', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('SDS', '44A9', '深圳东山', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO
    # StationElementMidModel('NAO', '44A8', '深圳南澳', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('YAO', '09710', '南澳', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # 存在大小写的问题

    StationElementMidModel('SHK', '44A6', '蛇口', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('CWH', '09713', '赤湾', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '0', '赤湾S', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('QHW', '44A5', '前海湾', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:[*]
    StationElementMidModel('SZJ', '44A4', '深圳机场', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:[*]
    StationElementMidModel('DMS', '44A7', '大梅沙', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:[*]
    # StationElementMidModel('QGY', '0', '泗盛围', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('HPU', '0', '黄埔', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:无
    # StationElementMidModel('NSA', '0', '南沙', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:无
    StationElementMidModel('QGY', '09736', '广州', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # TODO:[-] 24-08-30 南海新加入的站点
    # StationElementMidModel('HGM', '0', '横门', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:无
    StationElementMidModel('ZHU', '09734', '珠海', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('MGE', '0', '妈阁', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:无
    # StationElementMidModel('QGY', '0', '青洲塘', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '0', '外港', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('DLS', '0', '灯笼山', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:无
    # StationElementMidModel('QGY', '09732', '大万山', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),   # TODO
    # StationElementMidModel('SZA', '0', '三灶', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO
    # StationElementMidModel('QGY', '0', '黄金', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '09873', '高栏', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('QGY', '0', '黄冲', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('TSH', '09737', '广东台山', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # TODO:[-] 24-08-30 更新了中文名

    # StationElementMidModel('TSH', '0', '烽火角', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('TSH', '0', '北津', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:无
    # StationElementMidModel('BJI', '0', '闸坡', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:无
    StationElementMidModel('SHD', '09739', '水东', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('TSH', '0', '湛江', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),           # TODO

    StationElementMidModel('NAZ', '09730', '硇洲', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('NAD', '0', '南渡', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:无
    StationElementMidModel('HAN', '09753', '海安', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('LZH', '09878', '雷州', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('TSH', '0', '海口S', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),

    StationElementMidModel('XYG', '11741', '秀英', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('TSH', '0', '铺前港', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('QLN', '11742', '清澜', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('TSH', '0', '清澜S', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('BAO', '11757', '博鳌', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('GBE', '0', '港北', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),    # TODO:无
    StationElementMidModel('WCH', '11753', '乌场', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('SYA', '11754', '三亚', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('YGH', '11744', '莺歌海', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('TSH', '0', '西沙', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('TSH', '0', '南沙岛', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('DFG', '11743', '东方', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('QZH', '10727', '钦州', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('TSH', '10728', '铁山港', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # StationElementMidModel('STP', '0', '石头埠', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),  # TODO:无
    StationElementMidModel('BHI', '10725', '北海', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('WZH', '10722', '涠洲', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('FCG', '10723', '防城港', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),

    # ------ end
    StationElementMidModel('SHW', '08522', '莆田', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('DTO', '07450', '温州', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),

    StationElementMidModel('LGS', '11755', '陵水', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    # TODO:[-] 24-08-30 南海新加入的站点

]
'''站点集合常量'''
#
LIST_STATIONS: List[StationElementMidModel] = [
    # StationElementMidModel('BJA', '08443', '北茭', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('KMN', '07424', '坎门', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
]

LIST_FUBS: List[StationElementMidModel] = [

    StationElementMidModel('01001', '01001', 'MF01001', [ElementTypeEnum.FUB]),
    StationElementMidModel('01002', '01002', 'MF01002', [ElementTypeEnum.FUB]),
    StationElementMidModel('02001', '02001', 'MF02001', [ElementTypeEnum.FUB]),
    StationElementMidModel('02004', '02004', 'MF02004', [ElementTypeEnum.FUB]),
    StationElementMidModel('03003', '03003', 'MF03003', [ElementTypeEnum.FUB]),
    StationElementMidModel('03004', '03004', 'MF03004', [ElementTypeEnum.FUB]),
    StationElementMidModel('03005', '03005', 'MF03005', [ElementTypeEnum.FUB]),
    StationElementMidModel('03006', '03006', 'MF03006', [ElementTypeEnum.FUB]),
    StationElementMidModel('03007', '03007', 'MF03007', [ElementTypeEnum.FUB]),
    StationElementMidModel('04001', '04001', 'MF04001', [ElementTypeEnum.FUB]),
    StationElementMidModel('04002', '04002', 'MF04002', [ElementTypeEnum.FUB]),
    StationElementMidModel('04004', '04004', 'MF04004', [ElementTypeEnum.FUB]),
    StationElementMidModel('04101', '04101', 'MF04101', [ElementTypeEnum.FUB]),
    StationElementMidModel('05003', '05003', 'MF05003', [ElementTypeEnum.FUB]),
    StationElementMidModel('06001', '06001', 'MF06001', [ElementTypeEnum.FUB]),
    StationElementMidModel('06002', '06002', 'MF06002', [ElementTypeEnum.FUB]),

    StationElementMidModel('06003', '06003', 'MF06003', [ElementTypeEnum.FUB]),
    StationElementMidModel('06004', '06004', 'MF06004', [ElementTypeEnum.FUB]),
    StationElementMidModel('06104', '06104', 'MF06104', [ElementTypeEnum.FUB]),
    StationElementMidModel('06106', '06106', 'MF06106', [ElementTypeEnum.FUB]),
    StationElementMidModel('06107', '06107', 'MF06107', [ElementTypeEnum.FUB]),
    StationElementMidModel('09119', '09119', 'MF09119', [ElementTypeEnum.FUB]),
    StationElementMidModel('12001', '12001', 'MF12001', [ElementTypeEnum.FUB]),
    StationElementMidModel('13001', '13001', 'MF13001', [ElementTypeEnum.FUB]),
    StationElementMidModel('13002', '13002', 'MF13002', [ElementTypeEnum.FUB]),
    StationElementMidModel('14001', '14001', 'MF14001', [ElementTypeEnum.FUB]),
    StationElementMidModel('14004', '14004', 'MF14004', [ElementTypeEnum.FUB]),
    StationElementMidModel('14005', '14005', 'MF14005', [ElementTypeEnum.FUB]),
    StationElementMidModel('15001', '15001', 'MF15001', [ElementTypeEnum.FUB]),
    StationElementMidModel('17001', '17001', 'MF17001', [ElementTypeEnum.FUB]),
    StationElementMidModel('18001', '18001', 'MF18001', [ElementTypeEnum.FUB]),
]
'''浮标站点集合'''

LIST_SLB_STATIONS: List[StationElementMidModel] = [
    StationElementMidModel('WZS', '0', '温州S', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('RAS', '70600800', '瑞安S', [ElementTypeEnum.SURGE]),
    StationElementMidModel('AJS', '70610600', '鳌江S', [ElementTypeEnum.SURGE]),
    StationElementMidModel('WZS', '70503400', '温州S', [ElementTypeEnum.SURGE]),
    StationElementMidModel('SCS', '0', '沙埕S', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]),
    StationElementMidModel('ZJS', '0', '湛江S', [ElementTypeEnum.SURGE, ElementTypeEnum.WIND]), ]
'''水利部潮位站'''
