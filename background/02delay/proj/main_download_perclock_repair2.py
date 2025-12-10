import uuid
from typing import List
import arrow
import ftplib
import os
from loguru import logger


class FtpClient:
    # 针对 FileZilla Server 0.9.x，必须使用 gbk 才能正确识别中文文件名
    encoding = 'gbk'

    def __init__(self, host: str, port=21):
        self.ftp = ftplib.FTP()
        self.ftp.connect(host, port)
        self.ftp.encoding = self.encoding

    def login(self, user, passwd):
        try:
            self.ftp.login(user, passwd)
            logger.info(f"FTP Login Successful: {self.ftp.welcome}")
        except Exception as e:
            logger.error(f"FTP Login Failed: {e}")
            raise e

    def close(self):
        try:
            self.ftp.quit()
        except:
            self.ftp.close()

    def _download_recursive_relative(self, local_dir: str):
        """
        [核心修复] 相对路径递归下载
        不再使用绝对路径检查，而是利用当前上下文进行相对跳转，
        彻底解决包含中文的绝对路径在老旧FTP服务器上识别错误的问题。
        """
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        try:
            # 获取当前目录下的所有项（只返回名字，不带路径）
            items = self.ftp.nlst()
        except ftplib.error_perm:
            # 可能是空目录或权限问题
            return

        for item in items:
            if item in ['.', '..']:
                continue

            # 构造本地路径
            local_path = os.path.join(local_dir, item)

            # --- 关键逻辑：尝试进入目录 ---
            try:
                # 尝试进入 item (使用相对路径，不含中文父路径，极大降低出错率)
                self.ftp.cwd(item)

                # [成功] 说明 item 是文件夹
                # logger.info(f"进入目录: {item}")

                # 递归处理子目录
                self._download_recursive_relative(local_path)

                # 递归完成后，必须退回上一级，保持目录层级一致
                self.ftp.cwd('..')

            except ftplib.error_perm:
                # [失败] 无法进入，说明 item 是文件 (或者无权限的目录)
                # 尝试下载
                try:
                    with open(local_path, 'wb') as f:
                        # 使用相对路径下载
                        self.ftp.retrbinary(f'RETR {item}', f.write, 262144)
                    logger.info(f"[-] 下载成功: {item}")
                except Exception as e:
                    logger.error(f"[x] 下载文件失败 {item}: {e}")
            except Exception as e:
                logger.error(f"[!] 处理 {item} 时发生未知错误: {e}")

    def download_by_stations(self, local_root_path: str, stations: List[str], remote_root: str = '/ObsData/'):
        """
        根据站点列表下载数据
        """
        logger.info(f"开始执行指定站点下载任务，当前编码: {self.encoding}")

        for station in stations:
            # 构造起始绝对路径: /ObsData/三亚/perclock
            # 这里必须使用绝对路径做第一次跳转
            safe_remote_root = remote_root.rstrip('/')
            remote_perclock_path = f"{safe_remote_root}/{station}/perclock"

            logger.info(f"正在检查站点 {station} -> {remote_perclock_path}")

            try:
                # 1. 尝试直接跳转到 perclock 目录
                self.ftp.cwd(remote_perclock_path)

                # 2. 如果跳转成功，说明目录存在
                logger.info(f"[-] 成功进入 {station} 的 perclock 目录")

                # 3. 构造本地起始目录
                local_save_path = os.path.join(local_root_path, station, "perclock")

                # 4. 开始相对路径递归下载 (无需再传入远程路径，因为FTP指针已经在这里了)
                self._download_recursive_relative(local_save_path)

            except ftplib.error_perm:
                logger.warning(f"[!] 无法进入 {station} 的 perclock 目录 (可能不存在或路径乱码)")
            except Exception as e:
                logger.error(f"[x] 处理站点 {station} 时发生错误: {e}")

        logger.info("所有指定站点下载任务完成。")


if __name__ == '__main__':
    """
        25-12-10 使用此脚本完成内网站点ftp批量下载并存储
    """
    # 配置信息
    FTP_HOST = '192.168.1.100'  # 请替换为实际IP
    FTP_USER = 'user'  # 请替换为实际账号
    FTP_PASS = 'pass'  # 请替换为实际密码

    LOCAL_SAVE_DIR = r'D:\Data\Downloads'

    # 这里的中文名称必须和 FTP 上完全一致
    STATIONS_LIST = ['三亚', '三沙', '三门健跳']

    client = FtpClient(FTP_HOST)
    try:
        client.login(FTP_USER, FTP_PASS)
        client.download_by_stations(LOCAL_SAVE_DIR, STATIONS_LIST)
    except Exception as e:
        logger.error(f"全局错误: {e}")
    finally:
        client.close()