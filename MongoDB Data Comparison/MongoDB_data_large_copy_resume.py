import pymongo
from pymongo import MongoClient,InsertOne,DeleteMany
from logger import logger
from typing import Optional, Dict, List
from urllib.parse import quote_plus
from bson import ObjectId
import time
from datetime import datetime


def resume_copy_with_objectid(
        source_uri: str,
        target_uri: str,
        db_name: str,
        source_collection: str,
        target_collection: str,
        batch_size: int = 1000,
        checkpoint_file: str = "checkpoint.txt",
        query_filter: Optional[Dict] = None
) -> None:
    """
    处理ObjectId类型的断点续传复制

    参数:
        source_uri: 源MongoDB连接字符串
        target_uri: 目标MongoDB连接字符串
        db_name: 数据库名称
        source_collection: 源集合名称
        target_collection: 目标集合名称
        batch_size: 每批处理的文档数量
        checkpoint_file: 检查点文件路径
        query_filter: 可选的查询过滤器
    """
    # 连接数据库
    source_client = MongoClient(source_uri,datetime_conversion='DATETIME_AUTO')
    target_client = MongoClient(target_uri,datetime_conversion='DATETIME_AUTO')

    try:
        source_db = source_client[db_name]
        target_db = target_client[db_name]

        source_coll = source_db[source_collection]
        target_coll = target_db[target_collection]

        logger.info(f"{source_collection}: 数据集复制开始...")

        # 尝试从检查点文件读取最后处理的ObjectId
        last_processed_id = None
        try:
            with open(checkpoint_file, 'r') as f:
                id_str = f.read().strip()
                if id_str:
                    last_processed_id = ObjectId(id_str)
                    logger.info(f"从检查点恢复，最后处理的ObjectId: {last_processed_id}")
        except FileNotFoundError:

            logger.info(f"未找到检查点文件，将从集合开头开始复制,清除目标集合「{collection_b}」数据...")
            # 清空目标集合，从头开始传
            target_coll.bulk_write([DeleteMany({})])
            logger.info(f"目标集合「{collection_b}」数据清理完成!")

        except Exception as e:
            logger.error(f"读取检查点文件出错: {str(e)}")
            raise

        # 创建基础查询条件
        query = query_filter or {}
        if last_processed_id:
            query['_id'] = {'$gt': last_processed_id}

        # 获取总文档数用于进度跟踪
        total_docs = source_coll.count_documents(query)
        logger.info(f"需要复制的文档总数: {total_docs}")

        if total_docs == 0:
            logger.info("没有需要复制的文档")
            return

        # 批量复制文档
        processed_count = 0
        while True:
            try:
                # 获取一批文档，按_id升序排序
                cursor = source_coll.find(query).sort('_id', pymongo.ASCENDING).limit(batch_size)
                batch = list(cursor)
            except Exception as e:
                logger.error(f"获取文档失败「{e}」,重新获取")
                cursor = source_coll.find(query).sort('_id', pymongo.ASCENDING).limit(batch_size)
                batch = list(cursor)

            if not batch:
                logger.info("没有更多文档需要复制")
                break

            # 插入到目标集合
            try:
                result = target_coll.insert_many(batch, ordered=False)
                processed_count += len(batch)

                # 更新检查点（使用最后一个文档的ObjectId）
                last_id = batch[-1]['_id']
                with open(checkpoint_file, 'w') as f:
                    f.write(str(last_id))

                logger.info(
                    f"已处理: {processed_count}/{total_docs} ({(processed_count / total_docs) * 100:.3f}%) - 最后ObjectId: {last_id}")

                # 更新查询条件以获取下一批
                query['_id'] = {'$gt': last_id}
            except pymongo.errors.BulkWriteError as e:
                logger.warning(f"批量写入时部分文档出错，将继续处理: {str(e.details['writeErrors'][0]['errmsg'])}")
                # 即使有错误也继续处理
                last_id = batch[-1]['_id']
                query['_id'] = {'$gt': last_id}
                continue
            except Exception as e:
                logger.error(f"复制过程中出错: {str(e)}")
                raise

        logger.info(f"{collection_a} 数据集复制完成! 总共处理了 {processed_count} 个文档")

    finally:
        source_client.close()
        target_client.close()

    def collection_large_copy(mongodb_uri, source_db, source_col, target_db, target_col, batch_size=5000):
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


if __name__ == "__main__":
    # MongoDB 连接配置
    name=quote_plus("jun")
    pwd=quote_plus("xsd@d234F66lk77@44fx")
    mongodb_uri = f"mongodb://{name}:{pwd}@localhost:2989/?connectTimeoutMS=9000000&authSource=webportal-dev&directConnection=true"
    # mongodb_uri = f"mongodb://{name}:{pwd}@webportal-k8s-dev-mongodb-0-f511ed4cc11a5904.elb.us-east-2.amazonaws.com:27017/?connectTimeoutMS=9000000&authSource=webportal-dev&directConnection=true"

    database_name = "webportal-dev"  # 数据库名

    """
        待集合名称
            "ecgBeatData","ecgEvents","ecgEventChartData","ecgTraitData"
        """

    # # 测试用集合
    # collections = ["ecgReports"]

    # 正式复制集合
    collections = ["ecgBeatData", "ecgEvents", "ecgEventChartData", "ecgTraitData"]

    for collection in collections:
        collection_a = collection
        collection_b = f"{collection_a}_copy1"  # 第二个集合名

        # 开始时间
        start = time.time()
        logger.info(f"---->开始时间：{datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}")
        # 复制数据
        resume_copy_with_objectid(
            source_uri=mongodb_uri,
            target_uri=mongodb_uri,
            db_name=database_name,
            source_collection=collection_a,
            target_collection=collection_b,
            batch_size=1000,
            checkpoint_file=f"{collection_a}_copy_checkpoint.txt",
            # query_filter={"status": "active"}  # 可选: 只复制符合条件的文档
            query_filter={}  # 可选: 只复制符合条件的文档

        )
        # 结束时间
        logger.info(f"---->结束时间：{datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"总计耗时：{time.time() - start:.2f}s")