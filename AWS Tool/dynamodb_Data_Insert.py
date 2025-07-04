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
#     aws_secret_access_key='xxx',
#     region_name='ap-south-1',
# )

# 测试环境
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id='XX',
    aws_secret_access_key='xxx',
    region_name='ap-south-1',
)



def get_event_data_items(table_name, sensor_id, start_time, end_time):
    if start_time > end_time:
        logger.error(f"Start time cannot be greater than end time, sensorId: {sensor_id}, startTime: {start_time}, "
                     f"endTime: {end_time}")
        return None
    table = dynamodb.Table(table_name)
    result = []
    if end_time == 0:
        # 当end_time为0时，表示结束时间不设定，默认 2100-01-01 00:00:00
        end_time = 4102416000000
    try:
        last_evaluated_key = None
        index = 0
        while True:
            if last_evaluated_key is not None:
                response = table.query(
                    KeyConditionExpression=f'sensorId = :sensor AND recordTime BETWEEN :start AND :end',
                    ExpressionAttributeValues={':sensor': sensor_id, ':start': int(start_time), ':end': int(end_time)},
                    Limit=1000,
                    ExclusiveStartKey=last_evaluated_key
                )
            else:
                response = table.query(
                    KeyConditionExpression=f'sensorId = :sensor AND recordTime BETWEEN :start AND :end',
                    ExpressionAttributeValues={':sensor': sensor_id, ':start': int(start_time), ':end': int(end_time)},
                    Limit=1000
                )
            last_evaluated_key = response.get('LastEvaluatedKey', None)
            if not response['Items']:
                break
            index += 1
            print(f"{index} Get Dynamodb Data Size: {len(response['Items'])}")
            result.extend(response['Items'])
            if last_evaluated_key is None:
                break
        return result
    except Exception as e:
        logger.error(f"Get Dynamodb is Error! {e}")
        return None

# 插入数据到Dynamodb
def insert_event_data_items(table_name, items):

    table = dynamodb.Table(table_name)

    # print(items)

    try:
        with table.batch_writer() as batch:
            for _item in items:
                batch.put_item(Item=_item)
                logger.info(f"Insert Dynamodb Data: {_item}")
            return True


    except Exception as e:
        logger.error(f"Insert Dynamodb is Error! {e}")
        return False

# 组装数据并放到队列中
def production_data_put_to_queue(sensor_id, start_time, end_time,  subject_id, tenant, timezone_name, timezone_offset):
    result = []

    # 将字符串时间转换时间戳，取睡眠的开始结束时间
    start_time = start_time
    end_time = end_time
    start_time= int(time.mktime(time.strptime(start_time, "%Y-%m-%d %H:%M"))) * 1000
    end_time= int(time.mktime(time.strptime(end_time, "%Y-%m-%d %H:%M"))) * 1000
    print(start_time, end_time)
    # 获取开始结束之间差的分钟数，四舍五入
    minutes = round((end_time - start_time) / 60000)

    print(minutes)
    try:
        for i in range(minutes):
            data = {"sensorId": sensor_id, "startTime": start_time + i * 60000, "endTime": start_time + (i + 1) * 60000,
                    "sleepStatus": 0, "stage": stage, "subjectId": subject_id, "tenant": tenant,
                    "timezoneName": timezone_name, "timezoneOffset": timezone_offset,
                    "_expire_time": int(time.time()) + 60 * 60 * 24 * 1}
            if get_stage(data["startTime"], stage) == get_stage(data["endTime"], stage):
                data["stage"] = get_stage(data["startTime"], stage)
            else:
                data["stage"] = 0
            print(data)
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
    # 将stage中所有字符串时间转为时间戳
    for key in stages.keys():
        print(stages[key])
        for item in stages[key]:
            item["start"] = str_time_to_timestamp(item["start"])
            item["end"] = str_time_to_timestamp(item["end"])
    # print(999,stages)
    # 判断是否在stage范围
    for key in stages.keys():
        for item in stages[key]:
            if item["start"] <= timeshift <= item["end"]:
                return stage_num[key]
            else:
                return 0

# 从queue中获取数据并插入到Dynamodb
def get_data_from_queue_and_insert(table_name, items):
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

def setup(sensor_id, start_time, end_time,  subject_id, tenant, timezone_name, timezone_offset):
    table_name = "TEST_VCLOUD_SLEEP_DATA"

    status, result = production_data_put_to_queue(sensor_id=sensor_id, start_time=start_time, end_time=end_time, subject_id=subject_id, tenant=tenant, timezone_name=timezone_name, timezone_offset=timezone_offset)


    while status:
        if len(result) != 0:
            get_data_from_queue_and_insert(table_name, result)
        # time.sleep(1)

if __name__ == '__main__':

    stage = {
    "Stage1": [
        {
            "start": "2025-3-13 10:10",
            "end": "2025-3-13 11:10"
        },
        {
            "start": "2025-3-13 15:10",
            "end": "2025-3-13 16:10"
        }
    ],
    "Stage2": [
        {
            "start": "2025-3-13 12:10",
            "end": "2025-3-13 13:10"
        }
    ],
    "Stage3": [
        {
            "start": "2025-3-13 14:10",
            "end": "2025-3-13 15:10"
        }
    ],
    "Stage-1": [
        {
            "start": "2025-3-13 16:10",
            "end": "2025-3-13 17:10"
        }
    ]
}

    Patient_Data = {
        "sensorId": "ECGRec_202348/C9B9371",
        "subjectId": "Nicole",
        "tenant": "UAT_280",
        "timezoneName": "Asia/Shanghai",
        "timezoneOffset": 288

    }

    setup(sensor_id="ECGRec_202348/J20250313", start_time="2025-3-13 10:10", end_time="2025-3-13 18:10", subject_id="Nicole", tenant="UAT_280", timezone_name="Asia/Shanghai", timezone_offset=28800)