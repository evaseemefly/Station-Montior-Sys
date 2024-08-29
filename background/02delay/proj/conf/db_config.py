from conf.settings import db_pwd


class DBConfig:
    """
    DbConfig DB配置类
    :version: 1.4
    :date: 2020-02-11
    """

    driver = 'mysql+mysqldb'
    # host = '127.0.0.1'
    # 宿主机的mysql服务
    # host = 'host.docker.internal'
    # 线上环境由于 server 解释器为 docker，通过ifconfig 查看 docker0 的网络地址，通过该地址访问
    # host = '172.17.0.1'
    host = '128.5.9.79'
    port = '3306'
    username = 'root'
    password = db_pwd
    database = 'sys_station_montior'
    charset = 'utf8mb4'
    table_name_prefix = ''
    echo = False
    pool_size = 10  # 整数，连接池的大小，默认是 5。表示连接池中保持的连接数量。
    max_overflow = 20  # 整数，超过 pool_size 后可以额外创建的连接数。默认是 10。
    pool_timeout = 30  # 整数或浮点数，获取连接时的超时时间（秒），默认是 30 秒。
    pool_recycle = 180  # 整数，连接池中连接的回收时间（秒）。超过这个时间的连接会被自动断开并替换。默认是 -1，表示不使用回收。

    def get_url(self):
        config = [
            self.driver,
            '://',
            self.username,
            ':',
            self.password,
            '@',
            self.host,
            ':',
            self.port,
            '/',
            self.database,
            '?charset=',
            self.charset,
        ]

        return ''.join(config)
