import os
import time


def get_data_based_on_second(data_timestamp):
    """
    从Unix时间戳获取一分钟中的第几秒
    兼容10位（秒）和13位（毫秒）时间戳
    """
    timestamp=data_timestamp
    # 判断时间戳是秒还是毫秒
    if timestamp > 1e12:  # 大于 1000000000000 表示是毫秒时间戳
        # 转换为秒，然后取模
        seconds = timestamp / 1000
        second_in_minute = int(seconds % 60)
    else:
        # 已经是秒级时间戳
        second_in_minute = int(timestamp % 60)

    # 根据不同的秒数返回不同的数据
    if 0 <= second_in_minute < 15:
        return f"当前是第 {second_in_minute} 秒，传输第一组数据"
    elif 15 <= second_in_minute < 30:
        return f"当前是第 {second_in_minute} 秒，传输第二组数据"
    elif 30 <= second_in_minute < 45:
        return f"当前是第 {second_in_minute} 秒，传输第三组数据"
    else:
        return f"当前是第 {second_in_minute} 秒，传输第四组数据"


if __name__ == '__main__':
    while True:
        timestamp=int(time.time()*1000)
        print(timestamp)
        result=get_data_based_on_second(timestamp)
        time.sleep(1)
        print(result)