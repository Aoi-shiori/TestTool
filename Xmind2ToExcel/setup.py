# -*- ecoding: utf-8 -*-
# @ModuleName: setup
# @Author: Rex
# @Time: 2024/4/8 18:31
from setuptools import setup

APP = ['main.py']
DATA_FILES = []
OPTIONS = {
    'iconfile': '/Users/rexren/Gitlab/Xmind2Excel/Pictures/icon.ico',  # 指定图标文件路径
    # 其他选项...
}
setup(
    name="XmindToExcel",
    app=APP,
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)