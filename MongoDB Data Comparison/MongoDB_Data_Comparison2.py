# -*- coding: utf-8 -*-
"""
# @Creation time: 2025/7/3 14:12
# @Author       : 郭军
# @Email        : 391350540@qq.com
# @FileName     : MongoDB_Data_Comparison.py
# @Software     : PyCharm
# @Project      : MongoDB Data Comparison
# @PythonVersion: python 3.12
# @Version      : 
# @Description  : 
# @Update Time  : 
# @UpdateContent:  

"""
from urllib.parse import quote_plus
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient
from datetime import datetime


def compare_collections_with_ssh(
        ssh_host, ssh_username, ssh_pkey, ssh_port,
        mongo_host, mongo_port, db_name,
        collection1, collection2, output_file,
        mongo_username=None, mongo_password=None
):
    """
    通过SSH隧道连接MongoDB并比较两个集合

    参数:
        ssh_host: SSH服务器地址
        ssh_username: SSH用户名
        ssh_pkey: SSH私钥路径
        ssh_port: SSH端口(默认22)
        mongo_host: MongoDB服务器地址(从SSH服务器角度看)
        mongo_port: MongoDB端口(从SSH服务器角度看)
        db_name: 数据库名称
        collection1: 第一个集合名称
        collection2: 第二个集合名称
        output_file: 输出的Excel文件名
        mongo_username: MongoDB用户名(可选)
        mongo_password: MongoDB密码(可选)
    """

    # 建立SSH隧道
    with SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_username,
            ssh_pkey=ssh_pkey,
            remote_bind_address=(mongo_host, mongo_port)
    ) as tunnel:

        # 连接MongoDB
        client = MongoClient(

            host=tunnel.remote_bind_address[0],
            port=tunnel.local_bind_port,
            username=mongo_username,
            password=mongo_password
        )

        db = client[db_name]

        # 获取两个集合
        coll1 = db[collection1]
        coll2 = db[collection2]

        # 获取集合中的所有文档
        docs1 = list(coll1.find())
        docs2 = list(coll2.find())

        # 将文档转换为DataFrame
        df1 = pd.DataFrame(docs1)
        df2 = pd.DataFrame(docs2)

        # 删除MongoDB的_id字段
        if '_id' in df1.columns:
            df1 = df1.drop('_id', axis=1)
        if '_id' in df2.columns:
            df2 = df2.drop('_id', axis=1)

        # 比较两个DataFrame
        comparison = df1.compare(df2)

        # 创建Excel写入对象
        writer = pd.ExcelWriter(output_file, engine='openpyxl')

        # 将原始数据和比较结果写入不同的工作表
        df1.to_excel(writer, sheet_name=collection1, index=False)
        df2.to_excel(writer, sheet_name=collection2, index=False)

        if not comparison.empty:
            comparison.to_excel(writer, sheet_name='Comparison', index=True)

            # 添加摘要信息
            summary = {
                'Comparison Date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                'Collection 1': [collection1],
                'Collection 1 Count': [len(docs1)],
                'Collection 2': [collection2],
                'Collection 2 Count': [len(docs2)],
                'Differences Found': ['Yes'],
                'Difference Count': [len(comparison)]
            }
        else:
            summary = {
                'Comparison Date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                'Collection 1': [collection1],
                'Collection 1 Count': [len(docs1)],
                'Collection 2': [collection2],
                'Collection 2 Count': [len(docs2)],
                'Differences Found': ['No']
            }

        pd.DataFrame(summary).to_excel(writer, sheet_name='Summary', index=False)

        # 保存Excel文件
        writer.close()
        print(f"Comparison completed. Results saved to {output_file}")


# 使用示例
if __name__ == "__main__":

    # SSH配置
    ssh_host = "3.15.229.48"  # SSH服务器地址
    ssh_username = "jun"  # SSH用户名
    ssh_pkey = "D:/01-WorkSpace/02-项目/03-Webportal/MongoDB秘钥/jun.pem"  # SSH私钥路径
    ssh_port = 22  # SSH端口

    # MongoDB配置(从SSH服务器角度看)
    mongo_host = f"mongodb://webportal-k8s-dev-mongodb-0-f511ed4cc11a5904.elb.us-east-2.amazonaws.com:27017/?authSource=webportal-dev" # MongoDB服务器地址
    mongo_port = 27017  # MongoDB端口

    # 数据库配置
    database_name = "webportal-dev"  # 替换为你的数据库名
    collection_a = "ecgEventTypes"  # 第一个集合名
    collection_b = "ecgEventTypes_copy1"  # 第二个集合名


    # MongoDB认证(可选)
    mongo_username = quote_plus("jun")  # MongoDB用户名
    mongo_password = quote_plus("xsd@d234F66lk77@44fx")  # MongoDB密码

    # 输出文件
    output_excel = "collection_comparison.xlsx"  # 输出文件名

    compare_collections_with_ssh(
        ssh_host=ssh_host,
        ssh_username=ssh_username,
        ssh_pkey=ssh_pkey,
        ssh_port=ssh_port,
        mongo_host=mongo_host,
        mongo_port=mongo_port,
        db_name=database_name,
        collection1=collection_a,
        collection2=collection_b,
        output_file=output_excel,
        mongo_username=mongo_username,
        mongo_password=mongo_password
    )