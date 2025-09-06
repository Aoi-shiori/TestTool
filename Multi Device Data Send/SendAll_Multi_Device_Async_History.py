# -*- coding: utf-8 -*-
"""
# @Creation time: 2025/09/06 14:38
# @Author       : 郭军
# @Email        : 391350540@qq.com
# @FileName     : SendAll_Multi_Device_Async_History.py
# @Software     : PyCharm
# @Project      : TestTool
# @PythonVersion: python 3.12
# @Version      :
# @Description  : 发送多设备历史数据数据到云端
# @Update Time  :
# @UpdateContent:
v2.0-20250906:
1、更新可以自定义每秒数据上传，暂时只支持 ECG 数据

"""


import json
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread
from getEcgData import *
import requests
from queue import Queue
from logger import logger
import asyncio
from datetime import datetime, timedelta, UTC
from typing import Dict, Any, List

queue = Queue()


def get_app_token():
    # url = "https://vcloud2.vivalink.com/auth"
    #
    # payload = json.dumps({
    #     "id": "6170706009be6b1f2045cbac77",
    #     "key": "Whn_Nla;UtMLt@uUsQL]PDLx^?h46n<ri?v`K[@D"
    # })
    # headers = {
    #     'Content-Type': 'application/json'
    # }
    #
    # response = requests.request("POST", url, headers=headers, data=payload)
    # return response.json()['data']['token']

    url = "https://vcloud-test.vivalink.com/auth"

    payload = json.dumps({
        "id": "617070e40daf63ba334ece90d1",
        "key": "@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF"
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()['data']['token']

    # url = "https://site3-vcloud.vivalink.com/auth"
    #
    # payload = json.dumps({
    #     "id": "6170706009be6b1f2045cbac77",
    #     "key": "Whn_Nla;UtMLt@uUsQL]PDLx^?h46n<ri?v`K[@D"
    # })
    # headers = {
    #     'Content-Type': 'application/json'
    # }
    #
    # response = requests.request("POST", url, headers=headers, data=payload)
    # return response.json()['data']['token']

    # url = "https://site2-vcloud.vivalink.com/auth"
    #
    # payload = json.dumps({
    #     "id": "6170706009be6b1f2045cbac77",
    #     "key": "Whn_Nla;UtMLt@uUsQL]PDLx^?h46n<ri?v`K[@D"
    # })
    # headers = {
    #     'Content-Type': 'application/json'
    # }
    #
    # response = requests.request("POST", url, headers=headers, data=payload)
    # return response.json()['data']['token']

    # url = "https://site4-vcloud.vivalink.com/auth"
    #
    # payload = json.dumps({
    #     "id": "6170706009be6b1f2045cbac77   ",
    #     "key": "Whn_Nla;UtMLt@uUsQL]PDLx^?h46n<ri?v`K[@D"
    # })
    # headers = {
    #     'Content-Type': 'application/json'
    # }
    #
    # response = requests.request("POST", url, headers=headers, data=payload)
    # return response.json()['data']['token']

    # url = "https://site5-vcloud.vivalink.com/auth"
    #
    # payload = json.dumps({
    #     "id": "746e74390b36330c5044c1b47359bb4",
    #     "key": "agYzw`gqExp:urcZu=]5j6oqriBoqywpJ]zFpA5["
    # })
    # headers = {
    #     'Content-Type': 'application/json'
    # }
    #
    # response = requests.request("POST", url, headers=headers, data=payload)
    # return response.json()['data']['token']

    # url = "https://test.vivalink.com/auth"
    #
    # payload = json.dumps({
    #     "id": "617070e40daf63ba334ece90d1",
    #     "key": "@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF"
    # })
    # headers = {
    #     'Content-Type': 'application/json'
    # }
    #
    # response = requests.request("POST", url, headers=headers, data=payload)
    # return response.json()['data']['token']


def assemble_data_ECG(deviceId, recordTime, ecgData, HR, RR, Temp, accStepTotal, timeZone_offset, timezoneName,
                      ProjectId, Subjectid):
    dict = {
        "app_id": "com.vivalnk.vitalsmonitor.bms",
        "category": "N/A",
        "collectTime": recordTime,
        "customData": {
            "sdkVersionName": "3.2.0-2",
            "product": "a12uue",
            "appVersion": "3.2.4.325",
            "sdkCommonCode": "486",
            "appVersionName": "3.2.4.325",
            "appVersionCode": "325",
            "manufacturer": "samsung",
            "sdkVersionCode": "2470",
            "sdkCommonName": "1.3.1",
            "model": "SM-A125U1",
            "ID": "SP1A.210812.016",
            "device": "a12u",
            "brand": "samsung",
            # 项目编号
            "projectId": ProjectId,
            "subjectId": Subjectid
        },
        "data": {
            "acc": [
                {
                    "x": -33,
                    "y": -23,
                    "z": 2007
                },
                {
                    "x": -27,
                    "y": -25,
                    "z": 2006
                },
                {
                    "x": -30,
                    "y": -20,
                    "z": 2000
                },
                {
                    "x": -25,
                    "y": -17,
                    "z": 2004
                },
                {
                    "x": -28,
                    "y": -22,
                    "z": 2000
                },
                {
                    "x": -30,
                    "y": -19,
                    "z": 2001
                },
                {
                    "x": -30,
                    "y": -23,
                    "z": 2002
                },
                {
                    "x": -29,
                    "y": -16,
                    "z": 2002
                },
                {
                    "x": -25,
                    "y": -16,
                    "z": 2008
                },
                {
                    "x": -30,
                    "y": -18,
                    "z": 2003
                }
            ],
            "accAccuracy": 2048,
            "accActivity": 0,
            "accFrequency": 10,
            "accStepOffset": 0,
            "accStepTotal": accStepTotal,
            "activity": 0,
            "activityScore": 0,
            "avRR": -10001,
            "battery": 71,
            "calibratedTemp": "-1.0",
            "crc": 1,
            "dataMode": "FullDualMode",
            ### deviceId
            "deviceId": "CC: FB: BB: AD: DA: C6",
            "deviceName": deviceId,
            "deviceSN": deviceId,
            "deviceType": "VV330_1",
            "ecg": ecgData,
            "sf": 128,
            "flash": 1,
            "hr": HR,
            "leadOn": 1,
            "magnification": 1000,
            "noise": 1,
            "posture": 4,
            "rawTemp": Temp,
            "rmssd": 0,
            "rr": RR,
            "rri": [
                0,
                0,
                0,
                0,
                0
            ],
            "rssi": -10001,
            "rwl": [
                -1,
                -1,
                -1,
                -1,
                -1
            ],
            "snr": 0.7863115362155282,
            "temperature": Temp,
            "recordTime": recordTime
        },
        "deviceBattery": 74,
        "deviceOsType": "Android",
        "deviceOsVersion": "12",
        "deviceToken": "N/A",
        "deviceType": "SM-A125U1",
        "language": "en-US",
        "name": "N/A",
        "networkType": "WIFI",
        "patchMessage": "{\"fwVersion\":\"2.2.0.0041\",\"cpuStatus\":0,\"batteryStatus\":\"NOT_INCHARGEING\",\"timeStamp\":1648020731,\"magnification\":1000,\"accSamplingEnable\":1,\"hwVersion\":\"08\",\"ackStatus\":0,\"flashNum\":860,\"leadOffAccEnable\":0}",
        "phoneCache": 605,
        "profileId": "N/A",
        "recordTime": recordTime,
        "sdkVersion": "3.2.0-2",
        "sensorBlocks": 2376,
        "sensorId": deviceId,
        "timezone": f"{timeZone_offset}",
        "timezoneName": timezoneName,
        "type": "EcgRaw"
    }
    return dict


async def sendDataToVCloud(payload, recordTime, token):
    try:
        # url = "https://vcloud2.vivalink.com/tenants/VivaLNK/events?type=dataEvent"
        # url = "https://vcloud-california-test.vivalink.com/tenants/VivaLNK/events?type=dataEvent"
        # url = "https://site3-vcloud.vivalink.com/tenants/VivaLNK/events?type=dataEvent"
        # url = "https://site3-vcloud.vivalink.com/tenants/D1841M00072-Reallife/events?type=dataEvent"
        # url = "https://site2-vcloud.vivalink.com/tenants/VivaLNK/events?type=dataEvent"
        # 测试环境
        url = "https://vcloud-test.vivalnk.com/v2/tenants/VivaLNK/events?type=dataEvent"
        # url = "https://site4-vcloud.vivalink.com/tenants/VivaLNK/events?type=dataEvent"
        # url = "https://site5-vcloud.vivalink.com/tenants/VivaLNK/events?type=dataEvent"
        # url = "https://vcloud-ge.vivalink.com/tenants/vCloudMonitor/events?type=dataEvent"
        # 压测
        # url = "https://test.vivalink.com/tenants/VivaLNK/events?type=dataEvent"

        headers = {
            'Content-Type': 'application/json',
            'Authorization': token
        }
        # logger.info(f'-----------{payload}')

        response = requests.post(url, json=payload, headers=headers)
        index_end = len(payload)

        if json.loads(response.content)['code'] == 200 and json.loads(response.content)[
            'message'] == 'Batch ingestion done':
            logger.info(f'{a} {payload[0]["recordTime"]}~{payload[index_end - 1]["recordTime"]}数据发送成功！ {a}')
            return True
        else:
            logger.error(f'发送失败{response.json()}')
            await sendDataToVCloud(payload, recordTime, token)
            return False
    except Exception as e:
        logger.error(f'数据发送出错:{e}')
        await sendDataToVCloud(payload, recordTime, token)
        return False


def single_assembly_data(count, deviceId, record, stampEndTime, stampStartTime, timeZone_offset, timezoneName,
                         ProjectId, Subjectid, data_router):
    global recordTime, ecgDataList
    ecg_list, hr_list, rr_list = getEcgData1()
    ecgDataList = []

    recordTime = stampStartTime

    # 获取accStepTotal
    accStepTotal = AccStep(recordTime, timeZone_offset).get_acc_step_total()

    # 数据路由
    """
       数据路由功能
           根据时间戳秒数返回特定数据
           返回数据格式，
       {
                       'second': second,
                       'data': time_range['data'],
                       'Note': time_range.get('Note'),
                       'priority': time_range.get('priority'),
                       'timestamp': timestamp,
                       'normalized_timestamp': self.normalize_timestamp(timestamp),
                       'message': '默认配置数据'
                   }

    {
                "range": [0, 15],
                "data": {"HR": 11, "RR": 15, "Temp": 32.3},
                "Note": "Cass1",
                "priority": "lowest"
            }

       """

    result = data_router.get_data_for_timestamp(recordTime)
    hr, rr, temp = result['data']["HR"], result['data']["RR"], result['data']["Temp"]

    logger.info(f"{result}组装中。。。")

    ecgData = assemble_data_ECG(deviceId, recordTime, ecg_list[0], HR=hr, RR=rr, TEMP=temp, accStepTotal=accStepTotal,
                                timeZone_offset=timeZone_offset, timezoneName=timezoneName, ProjectId=ProjectId,
                                Subjectid=Subjectid)

    # timezoneName为空时候删除timezoneName字段
    if timezoneName == "" or timezoneName == None:
        del ecgData["timezoneName"]
    else:
        pass

    ecgDataList.append(ecgData)

    result = {"ecg": ecgDataList, "recordTime": recordTime}
    queue.put(result)
    return True


def main(startTime, endTime, deviceId, timeZone_offset, timezoneName, ProjectId, Subjectid, DATE_CONFIG, ret_lock):
    logger.info(f'{a} 正在组装数据... {a}')
    arrayStartTime = time.strptime(startTime, "%Y-%m-%d %H:%M:%S")

    arrayEndTime = time.strptime(endTime, "%Y-%m-%d %H:%M:%S")
    stampStartTime = int(time.mktime(arrayStartTime) * 1000)
    stampEndTime = int(time.mktime(arrayEndTime) * 1000)
    stampStartTime = stampStartTime
    stampEndTime = stampEndTime
    count = 0
    record = 0

    """
    数据路由功能
        根据时间戳秒数返回特定数据
        返回数据格式，
    {
                    'second': second,
                    'data': time_range['data'],
                    'Note': time_range.get('Note'),
                    'priority': time_range.get('priority'),
                    'timestamp': timestamp,
                    'normalized_timestamp': self.normalize_timestamp(timestamp),
                    'message': '默认配置数据'
                }
    """
    # 初始化数据路由
    data_router = TimestampDataRouter(config=DATE_CONFIG)

    # ret_lock.acquire()
    if stampStartTime == stampEndTime:
        # stampStartTime=stampStartTime-2
        # logger.info(f'单数据零时减2毫秒：{stampStartTime}')
        status = single_assembly_data(count, deviceId, record, stampEndTime, stampStartTime, timeZone_offset,
                                      timezoneName, ProjectId, Subjectid, data_router)
        if status:
            logger.info(f' {a} 数据组装完毕,准备发送数据... {a}')
            time.sleep(3)
            # 运行异步任务
            asyncio.run(send())
        else:
            logger.error(f'{a} 数据组装失败... {a}')
    else:
        status = assembly_data(count, deviceId, record, stampEndTime, stampStartTime, timeZone_offset, timezoneName,
                               ProjectId, Subjectid, data_router)
        if status:
            logger.info(f' {a} 数据组装完毕,准备发送数据... {a}')
            time.sleep(3)
            # 运行异步任务
            asyncio.run(send())
        else:
            logger.error(f'{a} 数据组装失败... {a}')
    # ret_lock.release()


# def assembly_data(count, deviceId, record, stampEndTime, stampStartTime,timeZone_offset,timezoneName,ProjectId,Subjectid,data_router):
#     global recordTime, ecgDataList
#     ecg_list, hr_list, rr_list = getEcgData()
#     # ecg_list = getEcgData()
#
#     while True:
#         # 组装15秒数据
#         if record + 30000 < stampEndTime:
#             ecgDataList = []
#             for i in range(count * 30, count * 30 + 30):
#                 recordTime = stampStartTime + (i * 1000)
#                 # ecgData = assemble_data_ECG(deviceId, recordTime, ecg_list[i], hr_list[i], rr_list[i])
#                 R=random.randint(0, 169)
#
#                 # 数据路由得到数据
#                 """
#                    数据路由功能
#                        根据时间戳秒数返回特定数据
#                        返回数据格式，
#                    {
#                                    'second': second,
#                                    'data': time_range['data'],
#                                    'Note': time_range.get('Note'),
#                                    'priority': time_range.get('priority'),
#                                    'timestamp': timestamp,
#                                    'normalized_timestamp': self.normalize_timestamp(timestamp),
#                                    'message': '默认配置数据'
#                                }
#
#                 {
#                             "range": [0, 15],
#                             "data": {"HR": 11, "RR": 15, "Temp": 32.3},
#                             "Note": "Cass1",
#                             "priority": "lowest"
#                         }
#
#                    """
#                 result = data_router.get_data_for_timestamp(recordTime)
#                 hr, rr, temp = result['data']["HR"], result['data']["RR"], result['data']["Temp"]
#
#                 # print("当前随机数:"+str(R) )
#                 # y=0
#                 # for  y in ecg_list[R]:
#                 #     y=y+1
#                 # if y != 128:
#                 #     print("有问题的数据是",y,R)
#
#                 # 获取accStepTotal
#                 accStepTotal=AccStep(recordTime,timeZone_offset).get_acc_step_total()
#
#                 # 组装数据
#                 ecgData = assemble_data_ECG(deviceId, recordTime, ecg_list[R], HR=hr, RR=rr, Temp=temp,
#                                             accStepTotal=accStepTotal, timeZone_offset=timeZone_offset,
#                                             timezoneName=timezoneName, ProjectId=ProjectId, Subjectid=Subjectid)
#
#                 # timezoneName为空时候删除timezoneName字段
#                 if timezoneName == "" or timezoneName == None:
#                     del ecgData["timezoneName"]
#                 else:
#                     pass
#
#                 record = recordTime
#
#                 logger.info(
#                     # f"数据处理 - 用例[{result.get('Note')}-P{result.get('priority')}] - "
#                     f"数据处理 - 用例:[{result.get('Note')}] - "
#                     f"时间戳[{ecgData['recordTime']}]:{'一致' if result.get('timestamp') == ecgData.get('recordTime') else '不一致'} - "
#                     f"路由→组装: HR({result['data'].get('HR', 'N/A')}→{ecgData.get('data', {}).get('hr', 'N/A')}) | "
#                     f"RR({result['data'].get('RR', 'N/A')}→{ecgData['data']['rr']}) | "
#                     f"Temp({result['data'].get('Temp', 'N/A')}→{ecgData['data']['temperature']}) - "
#                     # f"{'✓数据一致' if result['data'].get('HR') == ecgData.get('data', {}).get('hr') and result['data'].get('RR') == ecgData['data'].get('rr') and result['data'].get('Temp') == ecgData['data'].get('temperature') else '✗数据不一致'} - "
#                     f"{'✓数据一致' if result['data'].get('HR') == ecgData.get('data', {}).get('hr') and result['data'].get('RR') == ecgData['data'].get('rr') and result['data'].get('Temp') == ecgData['data'].get('temperature') else '✗不一致:' + ''.join([f'HR' if result['data'].get('HR') != ecgData.get('data', {}).get('hr') else '', f',RR' if result['data'].get('RR') != ecgData['data'].get('rr') else '', f',Temp' if result['data'].get('Temp') != ecgData['data'].get('temperature') else '']).lstrip(',')} - "
#                     f"设备[{ecgData['sensorId']}] 时区:{ecgData['timezone']} 时区名：{ecgData.get('timezoneName', 'N/A')} 路由说明信息：[{result.get('message')}]"
#                 )
#                 ecgDataList.append(ecgData)
#                 # if len(ecgDataList)==9:
#                 #     break
#
#             result = {"ecg": ecgDataList, "recordTime": recordTime}
#             # print(result)
#             queue.put(result)
#             count += 1
#             record += 6000
#
#         else:
#             t = int((stampEndTime - record) // 1000)
#             for i in range(count * 30, count * 30 + t):
#                 recordTime = stampStartTime + (i * 1000)
#                 R=random.randint(0, 300)
#
#                 # 获取accStepTotal
#                 accStepTotal=AccStep(recordTime,timeZone_offset).get_acc_step_total()
#
#                 # 数据路由
#                 result = data_router.get_data_for_timestamp(recordTime)
#                 hr, rr, temp = result['data']["HR"], result['data']["RR"], result['data']["Temp"]
#
#                 ecgData = assemble_data_ECG(deviceId, recordTime, ecg_list[R], hr, rr,temp,accStepTotal,timeZone_offset,timezoneName,ProjectId,Subjectid)
#
#                 # ecgData = assemble_data_ECG(deviceId, recordTime, ecg_list[R], 65, 28)
#                 record = recordTime
#
#                 logger.info(
#                     # f"数据处理 - 用例[{result.get('Note')}-P{result.get('priority')}] - "
#                     f"数据处理 - 用例:[{result.get('Note')}] - "
#                     f"时间戳[{ecgData['recordTime']}]:{'一致' if result.get('timestamp') == ecgData.get('recordTime') else '不一致'} - "
#                     f"路由→组装: HR({result['data'].get('HR', 'N/A')}→{ecgData.get('data', {}).get('hr', 'N/A')}) | "
#                     f"RR({result['data'].get('RR', 'N/A')}→{ecgData['data']['rr']}) | "
#                     f"Temp({result['data'].get('Temp', 'N/A')}→{ecgData['data']['temperature']}) - "
#                     # f"{'✓数据一致' if result['data'].get('HR') == ecgData.get('data', {}).get('hr') and result['data'].get('RR') == ecgData['data'].get('rr') and result['data'].get('Temp') == ecgData['data'].get('temperature') else '✗数据不一致'} - "
#                     f"{'✓数据一致' if result['data'].get('HR') == ecgData.get('data', {}).get('hr') and result['data'].get('RR') == ecgData['data'].get('rr') and result['data'].get('Temp') == ecgData['data'].get('temperature') else '✗不一致:' + ''.join([f'HR' if result['data'].get('HR') != ecgData.get('data', {}).get('hr') else '', f',RR' if result['data'].get('RR') != ecgData['data'].get('rr') else '', f',Temp' if result['data'].get('Temp') != ecgData['data'].get('temperature') else '']).lstrip(',')} - "
#                     f"设备[{ecgData['sensorId']}] 时区:{ecgData['timezone']} 时区名：{ecgData.get('timezoneName', 'N/A')} 路由说明信息：[{result.get('message')}]"
#                 )
#                 ecgDataList.append(ecgData)
#
#             result = {"ecg": ecgDataList, "recordTime": recordTime}
#             queue.put(result)
#             break
#     return  True


def assembly_data(count, deviceId, record, stampEndTime, stampStartTime, timeZone_offset, timezoneName, ProjectId,
                  Subjectid, data_router):
    # 定义常量
    DATA_POINTS_PER_BATCH = 30
    MS_PER_DATA_POINT = 1000
    BATCH_DURATION_MS = DATA_POINTS_PER_BATCH * MS_PER_DATA_POINT

    ecg_list, hr_list, rr_list = getEcgData()

    # 确保随机数范围不超过ecg_list长度
    max_ecg_index = len(ecg_list) - 1

    while True:
        if record + BATCH_DURATION_MS <= stampEndTime:
            ecgDataList = []
            for i in range(count * DATA_POINTS_PER_BATCH, count * DATA_POINTS_PER_BATCH + DATA_POINTS_PER_BATCH):
                recordTime = stampStartTime + (i * MS_PER_DATA_POINT)

                # 确保随机索引在有效范围内
                R = random.randint(0, min(169, max_ecg_index))

                # 数据路由
                result = data_router.get_data_for_timestamp(recordTime)
                hr, rr, temp = result['data']["HR"], result['data']["RR"], result['data']["Temp"]

                # 获取accStepTotal
                accStepTotal = AccStep(recordTime, timeZone_offset).get_acc_step_total()

                # 组装数据
                ecgData = assemble_data_ECG(deviceId, recordTime, ecg_list[R], HR=hr, RR=rr, Temp=temp,
                                            accStepTotal=accStepTotal, timeZone_offset=timeZone_offset,
                                            timezoneName=timezoneName, ProjectId=ProjectId, Subjectid=Subjectid)

                # 处理timezoneName字段
                if not timezoneName:
                    ecgData.pop("timezoneName", None)

                record = recordTime

                logger.info(
                    # f"数据处理 - 用例[{result.get('Note')}-P{result.get('priority')}] - "
                    # f"数据处理 - 用例:[{result.get('Note')}] - "
                    f"ECG数据处理 - 用例[{result.get('Note')}-第{result.get('second')}秒] - "
                    f"时间戳[{ecgData['recordTime']}]:{'一致' if result.get('timestamp') == ecgData.get('recordTime') else '不一致'} - "
                    f"路由→组装: HR({result['data'].get('HR', 'N/A')}→{ecgData.get('data', {}).get('hr', 'N/A')}) | "
                    f"RR({result['data'].get('RR', 'N/A')}→{ecgData['data']['rr']}) | "
                    f"Temp({result['data'].get('Temp', 'N/A')}→{ecgData['data']['temperature']}) - "
                    # f"{'✓数据一致' if result['data'].get('HR') == ecgData.get('data', {}).get('hr') and result['data'].get('RR') == ecgData['data'].get('rr') and result['data'].get('Temp') == ecgData['data'].get('temperature') else '✗数据不一致'} - "
                    f"{'✓数据一致' if result['data'].get('HR') == ecgData.get('data', {}).get('hr') and result['data'].get('RR') == ecgData['data'].get('rr') and result['data'].get('Temp') == ecgData['data'].get('temperature') else '✗不一致:' + ''.join([f'HR' if result['data'].get('HR') != ecgData.get('data', {}).get('hr') else '', f',RR' if result['data'].get('RR') != ecgData['data'].get('rr') else '', f',Temp' if result['data'].get('Temp') != ecgData['data'].get('temperature') else '']).lstrip(',')} - "
                    f"设备[{ecgData['sensorId']}] 时区:{ecgData['timezone']} 时区名：{ecgData.get('timezoneName', 'N/A')} 路由说明信息：[{result.get('message')}]"
                )
                ecgDataList.append(ecgData)

            result = {"ecg": ecgDataList, "recordTime": recordTime}
            queue.put(result)
            count += 1
            record += BATCH_DURATION_MS

        else:
            # 处理剩余数据
            remaining_points = int((stampEndTime - record) // MS_PER_DATA_POINT)
            if remaining_points <= 0:
                break

            ecgDataList = []
            for i in range(count * DATA_POINTS_PER_BATCH, count * DATA_POINTS_PER_BATCH + remaining_points):
                recordTime = stampStartTime + (i * MS_PER_DATA_POINT)

                # 确保随机索引在有效范围内
                R = random.randint(0, min(300, max_ecg_index))

                # 数据路由和数据处理逻辑...
                # ...（与主循环相同的逻辑）

            result = {"ecg": ecgDataList, "recordTime": recordTime}
            queue.put(result)
            break

    return True


async def send():
    token = get_app_token()
    tasks = []
    while not queue.empty():
        # 批量提交任务
        data = queue.get()
        ecg = data['ecg']
        record_time = data['recordTime']
        task = asyncio.create_task(sendDataToVCloud(ecg, record_time, token))
        tasks.append(task)
        # 等待所有任务完成
        await asyncio.gather(*tasks)
        logger.info(f'{a} 队列剩余数量：{queue.qsize()} {a}')


class ModifyTime:
    def __init__(self, str_time, days=0, hours=0, minutes=0, seconds=0):
        self.str_time = str_time
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds

    def date_plus(self):
        dt = datetime.strptime(self.str_time, '%Y-%m-%d %H:%M:%S')
        dt_plus = dt + timedelta(days=self.days, hours=self.hours, minutes=self.minutes, seconds=self.seconds)
        return dt_plus.strftime('%Y-%m-%d %H:%M:%S')

    def date_minus(self):
        dt = datetime.strptime(self.str_time, '%Y-%m-%d %H:%M:%S')
        dt_minus = dt - timedelta(days=self.days, hours=self.hours, minutes=self.minutes, seconds=self.seconds)
        return dt_minus.strftime('%Y-%m-%d %H:%M:%S')


class AccStep:
    def __init__(self, recordTime, timezone_offset):
        self.recordTime = recordTime
        self.timezone_offset = timezone_offset
        self.steps = 0

    def __str__(self):
        return f"recordTime: {self.recordTime}, steps: {self.steps}"

    @staticmethod
    def get_time_str(timshift, timezone_offset):
        timshift_10 = timshift / 1000
        # 将时间戳转换为 UTC 时间
        utc_time = datetime.fromtimestamp(timshift_10, tz=UTC)
        # 转为本地时区时间
        utc_time3 = datetime.fromtimestamp(timshift_10)

        # 根据时区偏移量调整时间
        timezone_time = utc_time + timedelta(seconds=timezone_offset)
        # print("我们来测试",timezone_time,utc_time,timedelta(seconds=timezone_offset))

        # datetime对象转换为时间戳
        timshift_Timezone = timezone_time.timestamp()

        time_str_hour = timezone_time.strftime("%H")
        time_str_min = timezone_time.strftime("%M")
        time_str_sec = timezone_time.strftime("%S")

        return time_str_hour, time_str_min, time_str_sec

    def get_acc_step_total(self):
        hour, min, sec = self.get_time_str(self.recordTime, self.timezone_offset)
        # print(hour, min, sec)
        steps = int(hour) * 3600 + int(min) * 60 + int(sec) + 1
        self.steps = steps
        return steps


class TimestampDataRouter:
    def __init__(self, config: Dict[str, Any] = None):
        """初始化数据路由器"""
        self.config = config or DEFAULT_CONFIG
        self.time_ranges = self.config.get('time_ranges', [])

    def normalize_timestamp(self, timestamp: float) -> float:
        """标准化时间戳为秒级"""
        if timestamp > 1e18:  # 纳秒
            return timestamp / 1e9
        elif timestamp > 1e15:  # 微秒
            return timestamp / 1e6
        elif timestamp > 1e12:  # 毫秒
            return timestamp / 1000
        else:  # 秒
            return timestamp

    def get_second_in_minute(self, timestamp: float) -> int:
        """获取一分钟中的第几秒"""
        normalized_ts = self.normalize_timestamp(timestamp)
        return int(normalized_ts % 60)

    def get_data_for_timestamp(self, timestamp: float) -> Dict[str, Any]:
        """根据时间戳获取对应的数据配置"""
        second = self.get_second_in_minute(timestamp)

        for time_range in self.time_ranges:
            range_start, range_end = time_range['range']
            if range_start <= second < range_end:
                return {
                    'second': second,
                    'data': time_range['data'],
                    'Note': time_range.get('Note'),
                    'priority': time_range.get('priority'),
                    'timestamp': timestamp,
                    'normalized_timestamp': self.normalize_timestamp(timestamp),
                    'message': '默认配置数据'
                }

        # 如果没有匹配的范围，返回默认数据或错误
        HR = random.choice(
            [-101, -201, -301, -316, -401, 0, 1, 11, 22, 33, 44, 55, 66, 77, 88, 99, 100, 151, 181, 199, 200, 300])
        RR = random.choice([15, 18, 19, 20])
        Temp = random.choice([33.2, 20, 44])

        return {
            'second': second,
            "data": {"HR": HR, "RR": RR, "Temp": Temp},
            'Note': "默认随机列表数据",
            'priority': '默认',
            'timestamp': timestamp,
            'normalized_timestamp': self.normalize_timestamp(timestamp),
            'message': '没有匹配的时间范围,返回默认随机数据'
        }


if __name__ == '__main__':

    ret_lock = threading.Lock()

    # 数据默认配置
    DEFAULT_CONFIG = {
        "timestamp_formats": {
            "second": 10,
            "millisecond": 13,
            "microsecond": 16,
            "nanosecond": 19
        },
        "time_ranges": [
            {
                "range": [0, 15], # 0-14 秒
                "data": {"HR": 11, "RR": 15, "Temp": 32.3},
                "Note": "Cass1",
                "priority": "high"
            },
            {
                "range": [15, 30],
                "data": {"HR": 11, "RR": 15, "Temp": 32.3},
                "Note": "Cass1",
                "priority": "medium"
            },
            {
                "range": [30, 45],
                "data": {"HR": 11, "RR": 15, "Temp": 32.3},
                "Note": "Cass1",
                "priority": "low"
            },
            {
                "range": [45, 60],
                "data": {"HR": 11, "RR": 15, "Temp": 32.3},
                "Note": "Cass1",
                "priority": "lowest"
            }
        ]
    }

    # 上传病人信息
    Patient_Profile = {
        "ProjectId": "Test_310",
        "SubjectId": "J250317005",
        "DeviceId": ["ECGRec_202511/JUN0005"],
        "Data_Config": DEFAULT_CONFIG,
        "TimeZoneName": "",  # TimeZoneName为“”，不传TimeZoneName字段
        "TimeZoneOffset": 28800,
        "StartTime": "2025-03-19 00:00:00",
        # "StartTime": "2025-03-17 23:59:59",
        "Days": 1

    }

    # Patient_Profile ={
    #     "ProjectId": "Test_310",
    #     "SubjectId": "J250314001",
    #     "DeviceId": ["ECGRec_202327/E111883"],
    #     "TimeZone": "Asia/Shanghai",
    #     "TimeZoneOffset": 28800,
    #     "StartTime": "2025-03-19 00:00:00",
    #     # "StartTime": "2025-03-17 23:59:59",
    #     "Days": 1
    #
    # }

    # 计时
    start_time1 = time.time()
    a = "-" * 15
    for device in Patient_Profile['DeviceId']:
        logger.info(f'{"@" * 200}')
        P = Patient_Profile
        days = P["Days"]
        str_time = P["StartTime"]

        # 从str_time开始往后推送数据
        for i in range(days, -1, -1):
            modify_Time = ModifyTime(str_time, days=i).date_minus()

            start_time = ModifyTime(modify_Time, hours=0, minutes=00, seconds=0).date_plus()

            end_time = ModifyTime(start_time, hours=23, minutes=59, seconds=59).date_plus()

            logger.info(f"{a}↓↓↓ 发送Device：{device}  {start_time}-->{end_time} 的数据 ↓↓↓{a}")

            main(P["StartTime"], end_time, device, P["TimeZoneOffset"], P["TimeZoneName"], P["ProjectId"],
                 P["SubjectId"], P["Data_Config"], ret_lock)

            logger.info(f"{a}↑↑↑ Device：{device}  {start_time}->{end_time} 数据发送完成 ↑↑↑{a}\n")

    # 耗时
    end_time1 = time.time()
    count_time = end_time1 - start_time1
    count_time = round(count_time, 4)
    logger.info(f'{a}本次发送数据耗时：{count_time}秒 {a}')
    logger.info(f'{"@" * 200}')
