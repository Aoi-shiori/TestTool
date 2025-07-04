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
import pymongo
from bson import ObjectId
from pymongo import MongoClient, InsertOne, DeleteMany
import pandas as pd
from datetime import datetime
from urllib.parse import quote_plus
import time
from logger import logger


def compare_collections(uri, db_name, collection1, collection2, output_file,query_date):
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
    docs1 = list(coll1.find(query_date))
    docs2 = list(coll2.find(query_date))

    # 将文档转换为 DataFrame
    df1 = pd.DataFrame(docs1)
    df2 = pd.DataFrame(docs2)

    # 删除 MongoDB 的 _id 字段，因为它每次都会不同
    # if '_id' in df1.columns:
    #     df1 = df1.drop('_id', axis=1)
    # if '_id' in df2.columns:
    #     df2 = df2.drop('_id', axis=1)

    # 比较两个 DataFrame
    # comparison = df1.compare(df2,keep_shape=True,keep_equal=True,align_axis=1)
    # comparison = df1.compare(df2,keep_shape=True,keep_equal=True,align_axis=1).reset_index()
    # comparison = df1.compare(df2,keep_equal=True,keep_shape=False,align_axis=0)
    # comparison = df1.compare(df2,keep_equal=True,align_axis=0)
    # comparison = df1.compare(df2,align_axis=1).reset_index()
    # comparison = df1.compare(df2,align_axis=0).reset_index()
    # comparison = df1.compare(df2).reset_index()
    # comparison = df1.compare(df2).fillna("PASS")
    comparison = df1.compare(df2)

    # 创建 Excel 写入对象
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

    # 保存 Excel 文件
    writer.close()
    logger.info(f"Comparison completed. Results saved to {output_file}")



def collection_large_copy(mongodb_uri,source_db, source_col, target_db, target_col, batch_size=5000):
        """
        处理大型集合的复制，包含错误处理和进度报告

        参数:
            source_db: 源数据库名
            source_col: 源集合名
            target_db: 目标数据库名
            target_col: 目标集合名
            batch_size: 批量插入大小
        """
        client = MongoClient(f'{mongodb_uri}',
                             connectTimeoutMS=30000,
                             socketTimeoutMS=None)

        source = client[source_db][source_col]
        target = client[target_db][target_col]

        # 清空目标集合
        target.bulk_write([DeleteMany({})])

        # 获取文档总数
        total_docs = source.count_documents({})
        logger.info(f"开始复制 {total_docs} 条文档...")

        # 使用批量写入操作提高性能
        processed = 0
        cursor = source.find().batch_size(batch_size)
        batch = []

        while True:
            try:
                for doc in cursor:
                    batch.append(InsertOne(doc))
                    if len(batch) == batch_size:
                        target.bulk_write(batch, ordered=False)
                        processed += len(batch)
                        batch = []
                        logger.info(f"进度: {processed}/{total_docs} ({processed / total_docs:.1%})")

                if batch:
                    target.bulk_write(batch, ordered=False)
                    processed += len(batch)
                    logger.info(f"进度: {processed}/{total_docs} ({processed / total_docs:.1%})")
                    break

            except Exception as e:
                logger.error(f"批量插入时出错: {str(e)}")
                logger.error("重试当前批次...")
                continue
            break

        logger.info(f"复制完成！共复制 {processed} 条文档")





# 使用示例
if __name__ == "__main__":
    # MongoDB 连接配置
    name=quote_plus("jun")
    pwd=quote_plus("xsd@d234F66lk77@44fx")
    mongodb_uri = f"mongodb://{name}:{pwd}@localhost:2989/?connectTimeoutMS=9000000&authSource=webportal-dev&directConnection=true"
    # mongodb_uri = f"mongodb://{name}:{pwd}@webportal-k8s-dev-mongodb-0-f511ed4cc11a5904.elb.us-east-2.amazonaws.com:27017/?connectTimeoutMS=9000000&authSource=webportal-dev&directConnection=true"

    database_name = "webportal-dev"  # 数据库名

    # 对比的集合名称
    collection_a = "ecgEventChartData"  # 第一个集合名
    collection_b = "ecgEventChartData_copy1"  # 第二个集合名

    # 查询参数
    clinic_ID="6848e8dd6b1fa7dec17a376e"

    output_excel = f"collection_comparison_{collection_a}_{clinic_ID}_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"  # 输出文件名


    #开始时间
    start=time.time()
    logger.info(f"开始时间：{datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}")

    # 数据集复制
    collection_large_copy(mongodb_uri=mongodb_uri,source_db=database_name, source_col=collection_a, target_db=database_name, target_col=collection_b)

    # # 数据集对比
    # compare_collections(
    #     uri=mongodb_uri,
    #     db_name=database_name,
    #     collection1=collection_a,
    #     collection2=collection_b,
    #     output_file=output_excel,
    #     query_date = { "$and": [ { "createdAt": { "$gte": datetime(2021, 5, 29) } }, { "clinic": ObjectId(clinic_ID) } ] }
    # )
    # 结束时间
    logger.info(f"结束时间：{datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"总计耗时：{time.time()-start:.2f}s")



    collection_large_copy(mongodb_uri=mongodb_uri,source_db=database_name, source_col=collection_a, target_db=database_name, target_col=collection_b)
