from conf._privacy import DB

db_pwd = DB.get('DB_PWD')
# 数据库的配置，配置借鉴自 django 的 settings 的结构
DATABASES = {
    'default': {
        # mac 尝试使用
        'ENGINE': 'pymysql',  # 数据库引擎
        # 'ENGINE': 'mysqldb',  # 数据库引擎
        'NAME': 'sys_station_montior',  # 数据库名
        'USER': 'root',  # 账号
        'PASSWORD': db_pwd,
        # 'HOST': 'localhost',  # HOST
        'HOST': '128.5.9.79',  # HOST
        # 'HOST': '172.17.0.1',  # 9.79 docker 内部访问 mysql 地址
        # 'HOST': 'host.docker.internal',  # docker 宿主机
        'POST': 3306,  # 端口
        'OPTIONS': {
            "init_command": "SET foreign_key_checks = 0;",
        },
    },

}

# 下载配置文件
DOWNLOAD_OPTIONS = {
    # 挂载映射盘路径
    # 'remote_root_path': r'/data/remote',
    'remote_root_path': r'X:',
    # 线上环境
    # 'remote_root_path': r'/home/nmefc/data_remote/71_upload2surge_wd_surge/2023:',
    # 本地下载根目录
    'local_root_path': r'E:\05DATA\01nginx_data\nmefc_download\WD_RESULT'

}

TASK_OPTIONS = {
    'name_prefix': 'TASK_SPIDER_GLOBAL_',
    'interval': 10,  # 单位min
}

DB_TABLE_SPLIT_OPTIONS = {
    'station': {
        'tab_split_name': 'station_realdata_specific'
    }
}

LOGGING_OPTIONS = {
    # 将日志改为当前目录下
    # 'LOG_DIR': r'/opt/project/logs',
    'LOG_DIR': r'./logs',
    'LOG_FILE': 'logging_{time}.log'
}
LOG_DIR: str = LOGGING_OPTIONS.get('LOG_DIR')
"""log 存储目录"""
LOG_FILE: str = LOGGING_OPTIONS.get('LOG_FILE')
"""log 日志文件命名规范"""
