# -*- coding: utf-8 -*-
"""
# @Creation time: 2025/4/14 11:20
# @Author       : 郭军
# @Email        : 391350540@qq.com
# @FileName     : version_info.py
# @Software     : PyCharm
# @Project      : Xmind2ToExcel
# @PythonVersion: python 3.12
# @Version      : 
# @Description  : 
# @Update Time  : 
# @UpdateContent:  

"""
import datetime
# with open("version_info.txt", "r", encoding="utf-8") as f:
#     content = f.read()
# # 尝试解析
# try:
#     info = eval(content)
#     print("版本文件解析成功！")
# except SyntaxError as e:
#     print(f"语法错误：{e}")

version_1= (1, 1, 0, 0)
version_2='1.1.0.0'

with open("version_info.txt", "w",encoding='UTF-8') as f:
    f.write(f"""
# UTF-8
VSVersionInfo(
  ffi=FixedFileInfo(
    filevers={version_1},
    prodvers={version_1},
    mask=0x3f,
    flags=0x0,
    OS=0x40004,
    fileType=0x1,
    subtype=0x0,
    date=(0, 0)
  ),
  kids=[
    StringFileInfo(
      [
        StringTable(
          '040904B0',
          [
            StringStruct('CompanyName', 'Vivalink'),
            StringStruct('FileDescription', '用于转换公司特定格式的xmind用例文件为Excle,本转换提供内部使用,如有问题可联系我：391350540@qq.com(Jun),jun@vivalink.com.cn(Jun).'),
            StringStruct('FileVersion', '{version_2}'),
            StringStruct('InternalName', 'Xmind2ToExcel'),
            StringStruct('LegalCopyright', '© Vivalink. All rights reserved.'),
            StringStruct('OriginalFilename', 'Xmind2ToExcel.exe'),
            StringStruct('ProductName', 'Xmind2ToExcel'),
            StringStruct('ProductVersion', '{version_2}')
          ]
        )
      ]
    ),
    VarFileInfo([VarStruct('Translation', [0x804, 1200])]
  )]
)
""")