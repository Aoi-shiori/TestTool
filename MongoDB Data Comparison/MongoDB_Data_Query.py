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
import math
import os
from typing import Dict, Optional, Union, List, Any
# from bson import ObjectId
from pymongo import MongoClient, InsertOne, DeleteMany
import pandas as pd
from datetime import datetime
from urllib.parse import quote_plus
import time
from logger import logger
import gc
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter
import openpyxl
from math import ceil


def data_query_collections(uri, db_name, collection1, collection2, output_file, query_date):
    """
    比较两个 MongoDB 集合并将结果写入 Excel 文件

    参数:
        uri: MongoDB 连接字符串
        db_name: 数据库名称
        collection1: 第一个集合名称
        collection2: 第二个集合名称
        output_file: 输出的 Excel 文件名
    """

    # 连接 MongoDB
    client = MongoClient(uri)
    db = client[db_name]

    # 获取两个集合
    coll1 = db[collection1]
    coll2 = db[collection2]

    # 获取集合中的所有文档
    # docs1 = list(coll1.find(query_date))
    docs1=list(coll1.aggregate(query_date))
    # docs2 = list(coll2.find(query_date))

    # 将文档转换为 DataFrame
    df1 = pd.DataFrame(docs1)
    # df2 = pd.DataFrame(docs2)

    # 创建 Excel 写入对象
    writer = pd.ExcelWriter(output_file, engine='openpyxl')

    # 将原始数据和比较结果写入不同的工作表
    df1.to_excel(writer, sheet_name=collection1, index=False)
    # df2.to_excel(writer, sheet_name=collection2, index=False)

    # 保存 Excel 文件
    writer.close()
    logger.info(f"Query completed. Results saved to {output_file}")










# 使用示例
if __name__ == "__main__":
    # MongoDB 连接配置
    name=quote_plus("jun")
    # pwd=quote_plus("xsd@d234F66lk77@44fx") #dev
    pwd=quote_plus("Hsd3y5TPRt8jSOq4oF7d") #prod
    # mongodb_uri = f"mongodb://{name}:{pwd}@localhost:2989/?connectTimeoutMS=19000000&authSource=webportal-dev&directConnection=true"
    mongodb_uri = f"mongodb://{name}:{pwd}@localhost:2999/?connectTimeoutMS=19000000&authSource=webportal-prod&directConnection=true"

    # mongodb_uri = f"mongodb://{name}:{pwd}@webportal-k8s-dev-mongodb-0-f511ed4cc11a5904.elb.us-east-2.amazonaws.com:27017/?connectTimeoutMS=9000000&authSource=webportal-dev&directConnection=true"
    # mongodb_uri = f"mongodb://{name}:{pwd}@webportal-k8s-prod-mongodb-0-4ad4d750bb0ebaa9.elb.us-east-2.amazonaws.com:27017/?connectTimeoutMS=9000000&authSource=webportal-prod&directConnection=true"
    # mongodb_uri = f"mongodb://{name}:{pwd}@ webportal-k8s-prod-mongodb-0-4ad4d750bb0ebaa9.elb.us-east-2.amazonaws.com:27017/?connectTimeoutMS=9000000&authSource=webportal-prod&directConnection=true"

    database_name = "webportal-prod"  # 数据库名

    # 对比的集合名称
    # collection_a = "ecgEventChartData"  # 第一个集合名
    # collection_b = "ecgEventChartData_copy1"  # 第二个集合名

    # collection_a = "ecgTraitData"  # 第一个集合名
    # collection_b = "ecgTraitData_copy1"  # 第二个集合名

    # collection_a = "ecgBeatData"  # 第一个集合名
    # collection_b = "ecgBeatData_copy1"  # 第二个集合名

    # collection_a = "ecgEvents"  # 第一个集合名
    # collection_b = "ecgEvents_copy1"  # 第二个集合名

    collection_a = "ecgEventTypes"  # 第一个集合名
    collection_b = "ecgEventTypes_copy1"  # 第二个集合名

    # 查询参数
    clinic_ID="6848e8dd6b1fa7dec17a376e"

    output_excel = f"collection_query_{collection_a}_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"  # 输出文件名


    #开始时间
    start=time.time()
    logger.info(f"开始时间：{datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}")

    # 数据集对比
    data_query_collections(
        uri=mongodb_uri,
        db_name=database_name,
        collection1=collection_a,
        collection2=collection_b,
        output_file=output_excel,
        # query_date = { "$and": [ { "createdAt": { "$gte": datetime(2021, 5, 29) } }, { "clinic": ObjectId(clinic_ID) } ] }
        # query_date = {"$and": [{"start": {"$lt": datetime(2025, 7, 7)}}]}
        query_date=[{"$group": {"_id": "$name","count": { "$sum": 1 }}}]

        # 时间参数
        # ecgBeatData: recordTime
        # ecgEvents: start
        # ecgTraitData: recordTime
        # ecgEventChartData: recordTime
    )

    # 结束时间
    logger.info(f"结束时间：{datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"总计耗时：{time.time()-start:.2f}s")
