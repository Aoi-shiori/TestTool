import boto3

from log import logger

# 脚本用于插入数据到Dynamodb
# TEST_VCLOUD_SLEEP_DATA

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
