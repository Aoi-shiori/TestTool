#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Project: TestTool
@Author: guojun
@Email: 391350540@qq.com
@Date: 2026/5/21 11:08
@File: dynamodb_data_query.py
@IDE: PyCharm
@Description: 
"""
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone
import sys
import os

# ==================== 认证配置 ====================
# 方式一（推荐）：从环境变量读取 AWS 凭证
# 运行前请设置环境变量：AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
# 方式二（不推荐）：直接在此处填写（注意不要提交到公开代码库）
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "11")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "22")
AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")

if AWS_ACCESS_KEY_ID == "YOUR_ACCESS_KEY" or AWS_SECRET_ACCESS_KEY == "YOUR_SECRET_KEY":
    print("⚠️ 警告：未设置环境变量 AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY，使用硬编码凭证有安全风险！")
    print("   建议通过 export AWS_ACCESS_KEY_ID=xxx 等方式设置，然后删除脚本中的默认值。")

# ==================== 查询参数 ====================
TABLE_NAME = "TEST_VCLOUD_EVENT"
PARTITION_KEY_VALUE = "ECGRec_202613/F5M0229"   # 主键值（分区键）
START_TIME = 1775124000000                      # recordTime 起始（毫秒）
END_TIME = 1775728800000                        # recordTime 结束（毫秒）
TIME_FIELD = "recordTime"                       # 排序键字段名
ANOTHER_TIME_FIELD = "receiveTime"              # 需要统计时间差的字段

def main():
    print("正在初始化 DynamoDB 客户端...")
    try:
        # 显式使用提供的凭证
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_DEFAULT_REGION
        )
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(TABLE_NAME)
        # 测试连接：尝试描述表
        table.table_status
        print(f"✅ 成功连接到表 {TABLE_NAME} (区域 {AWS_DEFAULT_REGION})")
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        sys.exit(1)

    items = []
    last_evaluated_key = None

    print(f"开始查询分区键 '{PARTITION_KEY_VALUE}' 且 {TIME_FIELD} 在 [{START_TIME}, {END_TIME}] 范围内的数据...( dynamodb左右都是闭区间)")
    try:
        while True:
            query_params = {
                'KeyConditionExpression': Key('sensorId').eq(PARTITION_KEY_VALUE) & Key(TIME_FIELD).between(START_TIME, END_TIME)
            }
            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key

            response = table.query(**query_params)
            items.extend(response.get('Items', []))

            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break


        total_count = len(items)
        print(f"✅ 查询完成，共获取 {total_count} 条记录。")
        if total_count == 0:
            return

        # 提取 receiveTime 字段并过滤无效数据
        valid_recordtime=[]
        valid_times = []
        for item in items:
            rt = item.get(ANOTHER_TIME_FIELD)
            rt_recordtime=item.get(TIME_FIELD)
            if rt is not None:
                try:
                    valid_times.append(int(rt))
                except (ValueError, TypeError):
                    pass

            if rt_recordtime is not None:
                try:
                    valid_recordtime.append(int(rt_recordtime))
                except (ValueError,TypeError):
                    pass

        if not valid_times:
            print(f"⚠️ 未找到有效的 '{ANOTHER_TIME_FIELD}' 字段，无法计算时间差。")
            return

        min_ts = min(valid_times)
        max_ts = max(valid_times)
        diff_seconds = (max_ts - min_ts) / 1000.0

        min_readable = datetime.fromtimestamp(min_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        max_readable = datetime.fromtimestamp(max_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        print("\n--- 统计结果 ---")
        print(f"📊 总记录条数: {total_count}")
        print(f"🕐 最早的 {ANOTHER_TIME_FIELD}: {min_ts} -> {min_readable} UTC")
        print(f"🕒 最晚的 {ANOTHER_TIME_FIELD}: {max_ts} -> {max_readable} UTC")
        print(f"⏱️ 时间差值: {diff_seconds:.3f} 秒")
    except Exception as e:
        print(f"❌ 查询或处理数据时出错: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()