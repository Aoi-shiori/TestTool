# -*- coding: utf-8 -*-
"""
# @Creation time: 2025/3/13 23:30
# @Author       : 郭军
# @Email        : 391350540@qq.com
# @FileName     : dynamodb_Data_Upload.py.py
# @Software     : PyCharm
# @Project      : AWS Tool
# @PythonVersion: python 3.12
# @Version      : 
# @Description  : 
# @Update Time  : 
# @UpdateContent:  

"""
import json
import boto3
import time
from logger import logger
from queue import Queue

# 脚本用于插入数据到Dynamodb
# TEST_VCLOUD_SLEEP_DATA


# # 压测环境
# dynamodb = boto3.resource(
#     'dynamodb',
#     aws_access_key_id='XX',
#     aws_secret_access_key='XX',
#     region_name='ap-south-1',
# )

# 测试环境
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id='XX',
    aws_secret_access_key='xxx',
    region_name='ap-south-1',
)


# 插入数据到Dynamodb
def insert_event_data_items(table_name, items):

    table = dynamodb.Table(table_name)

    try:
        with table.batch_writer() as batch:
            for _item in items:
                batch.put_item(Item=_item)
                logger.info(f"Insert Dynamodb Data: {_item}")
        logger.info(f"Insert Dynamodb Data is Done!")
        return True
    except Exception as e:
        logger.error(f"Insert Dynamodb is Error! {e}")
        return False

# 组装数据并放到队列中
def production_data_put_to_queue(sensor_id, start_time, end_time,  subject_id, tenant, timezone_name, timezone_offset,str_stage_dict):
    result = []
    stage_dict=str_stage_dict_to_timestamp(str_stage_dict)

    # 将字符串时间转换时间戳，取睡眠的开始结束时间
    start_time = start_time
    end_time = end_time

    start_time= int(time.mktime(time.strptime(start_time, "%Y-%m-%d %H:%M"))) * 1000
    end_time= int(time.mktime(time.strptime(end_time, "%Y-%m-%d %H:%M"))) * 1000

    # 获取开始结束之间差的分钟数，四舍五入
    minutes = round((end_time - start_time) / 60000)

    print(minutes)
    try:
        for i in range(minutes):
            data = {"sensorId": sensor_id, "startTime": start_time + i * 60000, "endTime": start_time + (i + 1) * 60000,
                    "sleepStatus": 0, "stage": "", "subjectId": subject_id, "tenant": tenant,
                    "timezoneName": timezone_name, "timezoneOffset": timezone_offset,
                    "_expire_time": int(time.time()) + 60 * 60 * 24 * 1}

            t1=data["startTime"]

            starttime_stage = get_stage(t1,stage_dict)

            if starttime_stage>0 or starttime_stage==-1:
                data["stage"] = starttime_stage
            else:
                data["stage"] = 0
            # logger.info(f"Generate data : {data}")
            result.append(data)
        return True, result
    except Exception as e:
        logger.error(f"Production Data is Error! {e}")
        return False, None



#字符串时间转换时间戳
def str_time_to_timestamp(str_time):
    return int(time.mktime(time.strptime(str_time, "%Y-%m-%d %H:%M"))) * 1000

# 判断是否在stage范围
def get_stage(timeshift, stage_dict):

    stage_num = {
        "Stage1": 1,
        "Stage2": 2,
        "Stage3": 3,
        "Stage-1": -1
    }
    stages=stage_dict


    status=0
    # 判断是否在stage范围
    for key in stages.keys():
        for item in stages[key]:
            if item["start"] <= timeshift < item["end"]:
                status=stage_num[key]
            else:
                pass
    return status

# 将字符串时间转为时间戳stage_dict
def str_stage_dict_to_timestamp(stage_dict):
    stages=stage_dict
    # 将stage中所有字符串时间转为时间戳
    for key in stages.keys():
        # print(stages[key])
        for item in stages[key]:
            item["start"] = str_time_to_timestamp(item["start"])
            item["end"] = str_time_to_timestamp(item["end"])
    return stages

# 从queue中获取数据并插入到Dynamodb
def get_data_check_and_insert(table_name, items):
    duplicate_set = set()
    duplicate_items = []
    for item_o in items:
        key_index = item_o.get("sensorId") + str(item_o.get("startTime"))
        if key_index in duplicate_set:
            print(f"duplicate data: {item_o}")
        else:
            duplicate_set.add(key_index)
            duplicate_items.append(item_o)

    if len(duplicate_items) > 0:
        insert_event_data_items(table_name, duplicate_items)
    else:
        return False

def setup(sensor_id, start_time, end_time,  subject_id, tenant, timezone_name, timezone_offset,str_stage_dict):
    table_name = "TEST_VCLOUD_SLEEP_DATA"

    status, result = production_data_put_to_queue(sensor_id=sensor_id, start_time=start_time, end_time=end_time, subject_id=subject_id, tenant=tenant, timezone_name=timezone_name, timezone_offset=timezone_offset,str_stage_dict=str_stage_dict)


    if status:
        if len(result) != 0:
            get_data_check_and_insert(table_name, result)
    else:
        return False

if __name__ == '__main__':

    # 睡眠阶段
    stage_dict = {
    # Light sleep 浅睡
    "Stage1": [
        {
            "start": "2025-3-19 22:30",
            "end": "2025-3-20 02:30"
        }
    ],
    # REM sleep 快速眼动
    "Stage2": [

        {
            "start": "2025-3-20 06:30",
            "end": "2025-3-20 07:30"
        }
    ],
    # Deep sleep 深睡
    "Stage3": [
        {
            "start": "2025-3-20 02:30",
            "end": "2025-3-20 06:30"
        }
    ],
    # 断连,睡眠数据过短无法分析
    "Stage-1": [

    ]
}

    # # 参数信息
    # Patient_Data = {
    #     "sensorId": "ECGRec_202512/J032401",
    #     "subjectId": "J2025032401",
    #     "tenant": "UAT_310",
    #     "timezoneName": "Asia/Shanghai",
    #     "timezoneOffset": 28800,
    #     "sleep_start_time" :"2025-3-19 22:30",
    #     "sleep_end_time" :"2025-3-20 07:30"
    # }

    # 参数信息
    Patient_Data = {
        "sensorId": "ECGRec_202513/J032701",
        "subjectId": "J20250327001",
        "tenant": "Test_310",
        "timezoneName": "Asia/Shanghai",
        "timezoneOffset": 28800,
            "sleep_start_time" :"2025-3-19 22:30",
            "sleep_end_time" :"2025-3-20 07:30"
    }

    setup(sensor_id=Patient_Data["sensorId"], start_time=Patient_Data["sleep_start_time"], end_time=Patient_Data["sleep_end_time"], subject_id=Patient_Data["subjectId"], tenant=Patient_Data["tenant"], timezone_name=Patient_Data["timezoneName"], timezone_offset=Patient_Data["timezoneOffset"],str_stage_dict=stage_dict)