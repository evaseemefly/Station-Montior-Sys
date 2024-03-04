import uuid
from typing import List

import arrow
import ftplib
import os


class FtpClient:
    """
        + 23-09-20 ftp下载工厂类
    """

    # encoding = 'gbk'
    # TODO:[-] 24-02-26 由于目录存在中文设置为 utf-8
    encoding = 'utf-8'
    ftp = ftplib.FTP()

    def __init__(self, host: str, port=21):
        self.ftp.connect(host, port)
        self.ftp.encoding = self.encoding

    def login(self, user, passwd):
        self.ftp.login(user, passwd)
        print(self.ftp.welcome)

    def down_load_file(self, local_full_path: str, remote_file_name: str):
        """


        @param local_full_path: 本地存储全路径(含文件名),
        @param remote_file_name: 远端文件名
        @return:
        """
        file_handler = open(local_full_path, 'wb')
        # print(file_handler)
        # self.ftp.retrbinary("RETR %s" % (RemoteFile), file_handler.write)#接收服务器上文件并写入本地文件
        self.ftp.retrbinary('RETR ' + remote_file_name, file_handler.write)
        file_handler.close()
        return True

    def down_load_file_bycwd(self, local_full_path: str, remote_path: str, file_name: str) -> bool:
        """
            - 24-02-27 此处需要修改 remote_path 为远端的绝对路径，由于下载不同的文件需要反复切换至不同的目录，使用相对路径较为不便
            改为使用绝对路径
        @param local_full_path:
        @param remote_path:
        @param file_name:
        @return:
        """
        is_ok: bool = False
        file_handler = open(local_full_path, 'wb')
        # print(file_handler)
        # self.ftp.retrbinary("RETR %s" % (RemoteFile), file_handler.write)#接收服务器上文件并写入本地文件
        # 进入到 remote_path 路径下
        # ftplib.error_perm: 550 Failed to change directory.
        # remote_path:              '/test/ObsData/SHW/2024/02/20'
        # 实际路径: /home/nmefc/share/test/ObsData/汕尾/perclock/2024/02
        # v2:                     '/test/ObsData/汕尾/perclock/2024/02/20'
        # v3:     '/home/nmefc/share/test/ObsData/汕尾/perclock/2024/02/20'
        # -- 修改为实际的绝对路径
        self.ftp.cwd(remote_path)
        files_list: List[str] = self.ftp.nlst()
        # TODO:[-] 23-09-26 修改 ftp 缓冲区为 250KB
        BUFSIZE: int = 262144
        if file_name in files_list:
            now_str: str = arrow.Arrow.utcnow().format('YYYY-MM-DD|HH:mm:ss')
            print(f'')
            print(f'{now_str}[*]下载指定文件:{file_name}ing')
            self.ftp.retrbinary('RETR ' + file_name, file_handler.write, BUFSIZE)
            is_ok = True
            load_over_time: str = arrow.Arrow.utcnow().format('YYYY-MM-DD|HH:mm:ss')
            print(f'{load_over_time}[-]下载指定文件:{file_name}成功')
            file_handler.close()
        # TODO:[*] 24-02-27 此处缺少对于小写的处理
        elif file_name.lower() in files_list:
            now_str: str = arrow.Arrow.utcnow().format('YYYY-MM-DD|HH:mm:ss')
            print(f'')
            print(f'{now_str}[*]下载指定文件:{file_name}ing')
            self.ftp.retrbinary('RETR ' + file_name.lower(), file_handler.write, BUFSIZE)
            is_ok = True
            load_over_time: str = arrow.Arrow.utcnow().format('YYYY-MM-DD|HH:mm:ss')
            print(f'{load_over_time}[-]下载指定文件:{file_name.lower()}成功')
            file_handler.close()
        return is_ok

    def down_load_file_tree(self, local_path: str, remote_path: str, cover_path: str = None):
        """
            下载整个目录下的文件
        @param local_path:
        @param remote_path:
        @param cover_path:
        @return:
        """
        print("远程文件夹remoteDir:", remote_path)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        # 进入到 remote_path 路径下
        self.ftp.cwd(remote_path)
        remote_name_list = self.ftp.nlst()
        print("远程文件目录：", remote_name_list)
        # ['EU_high_res_wind_2023082012.nc']
        for file in remote_name_list:
            # 'E:\\05DATA\\06wind\\2022_EU_high'
            Local = os.path.join(local_path, file)
            print("正在下载", self.ftp.nlst(file))
            if file.find(".") == -1:
                if not os.path.exists(Local):
                    os.makedirs(Local)
                self.down_load_file_tree(Local, file)
            else:
                self.down_load_file(Local, file)
        self.ftp.cwd("..")
        return

    def close(self):
        self.ftp.quit()
