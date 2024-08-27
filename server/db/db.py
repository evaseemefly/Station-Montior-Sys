from contextlib import contextmanager

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from conf.db_config import DBConfig


class DBFactory:
    """
        + 23-03-09 数据库工厂类
    """
    session: Session = None
    default_config: DBConfig = DBConfig()
    engine: Engine = None
    config: DBConfig = None

    def __init__(self, config: DBConfig = None):
        if not config:
            config = self.default_config
            self.config = config
        self.session = self._create_scoped_session(config)

    def __del__(self):
        """
            + 23-04-04 解决
            sqlalchemy.exc.OperationalError: (MySQLdb._exceptions.OperationalError)
             (1040, 'Too many connections')
        :return:
        """
        self.session.close()

    def get_engine(self):
        """
            + 24-04-07 获取 engine 实例
        :param config:
        :return:
        """
        config = self.config
        return create_engine(
            config.get_url(),
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
            pool_recycle=config.pool_recycle,
            echo=config.echo
        )

    @staticmethod
    def _create_scoped_session(config: DBConfig):
        engine = create_engine(
            config.get_url(),
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
            pool_recycle=config.pool_recycle,
            echo=config.echo
        )

        # TODO:[-] 23-03-10 sqlalchemy.exc.ArgumentError: autocommit=True is no longer supported
        session_factory = sessionmaker(autocommit=False, autoflush=True, bind=engine)

        # scoped_session封装了两个值 Session 和 registry,registry加括号就执行了ThreadLocalRegistry的__call__方法,
        # 如果当前本地线程中有session就返回session,没有就将session添加到了本地线程
        # 优点:支持线程安全,为每个线程都创建一个session
        # scoped_session 是一个支持多线程且线程安全的session
        return scoped_session(session_factory)


@contextmanager
def session_scope():
    """
        TODO:[-] 24-08-26 基于事物的Session会话管理
    """

    session = DBFactory().session
    """提供一个事务范围的会话"""
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
