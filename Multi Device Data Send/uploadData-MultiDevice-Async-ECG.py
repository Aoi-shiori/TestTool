# -*- coding: utf-8 -*-
"""
# @Creation time: 2025/3/18 00:28
# @Author       : 郭军
# @Email        : 391350540@qq.com
# @FileName     : uploadData-MultiDevice-Async-ECG.py
# @Software     : PyCharm
# @Project      : Multi-Device-UploadData
# @PythonVersion: python 3.12
# @Version      : 
# @Description  : 发送固定的数据至云端
# @Update Time  : 
# @UpdateContent:  

"""
import threading
import time
import random
from getEcgData import *
import requests
from queue import Queue
from logger import logger
import asyncio
from datetime import datetime, timedelta,UTC

queue = Queue()

a = "-" * 15
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

    # url = "https://site2-vcloud-test.vivalink.com/auth"
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


    # url = "https://site6-vcloud.vivalink.com/auth"
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


def assemble_data_ECG(deviceId, recordTime, ecgData, HR,RR,accStepTotal,timeZone_offset,timezoneName,ProjectId,Subjectid):
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
        "subjectId" : Subjectid
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
        "rawTemp": "-1.0",
        "rmssd": 0,
        "rr": RR,
        "rri": [
            random.randint(1,1000),
            0,
            0,
            0,
            0
        ],
        "rssi": -10001,
        "rwl": [
            random.randint(1, 127),
            -1,
            -1,
            -1,
            -1
        ],
        "snr": 0.7863115362155282,
        "temperature": random.choice(["26.00", "28.00", "32.00", "33.00"]),
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
        # 澳大利亚
        # url = "https://site6-vcloud.vivalink.com/tenants/VivaLNK/events?type=dataEvent"
        # 测试环境
        url = "https://vcloud-test.vivalnk.com/v2/tenants/VivaLNK/events?type=dataEvent"
        # 伦敦
        # url = "https://site4-vcloud.vivalink.com/tenants/VivaLNK/events?type=dataEvent"
        # url = "https://site5-vcloud.vivalink.com/tenants/VivaLNK/events?type=dataEvent"
        # url = "https://vcloud-ge.vivalink.com/tenants/vCloudMonitor/events?type=dataEvent"
        # 压测
        # url = "https://test.vivalink.com/tenants/VivaLNK/events?type=dataEvent"
        # url = "https://site2-vcloud-test.vivalink.com/internal/tenants/VivaLNK/events?type=dataEvent"

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


def single_assembly_data(count, deviceId, record, stampEndTime, stampStartTime,timeZone_offset,timezoneName,ProjectId,Subjectid):
    global recordTime, ecgDataList
    ecg_list, hr_list, rr_list = getEcgData()
    ecgDataList = []

    recordTime = stampStartTime
    # 获取accStepTotal
    accStepTotal = AccStep(recordTime,timeZone_offset).get_acc_step_total()
    ecgData = assemble_data_ECG(deviceId, recordTime, ecg_list[0], 99, 99, accStepTotal,timeZone_offset,timezoneName,ProjectId,Subjectid)
    ecgDataList.append(ecgData)
    result = {"ecg": ecgDataList, "recordTime": recordTime}
    queue.put(result)
    return True


def setup(startTime, endTime, deviceId,timeZone_offset,timezoneName,ProjectId,Subjectid):
    logger.info(f'{a} 正在组装数据... {a}')
    arrayStartTime = time.strptime(startTime, "%Y-%m-%d %H:%M:%S")

    arrayEndTime = time.strptime(endTime, "%Y-%m-%d %H:%M:%S")
    stampStartTime = int(time.mktime(arrayStartTime) * 1000)
    stampEndTime = int(time.mktime(arrayEndTime) * 1000)
    stampStartTime = stampStartTime
    stampEndTime = stampEndTime
    count = 0
    record = 0
    # ret_lock.acquire()
    if stampStartTime == stampEndTime:
        # stampStartTime=stampStartTime-2
        # logger.info(f'单数据零时减2毫秒：{stampStartTime}')
        status= single_assembly_data(count, deviceId, record, stampEndTime, stampStartTime,timeZone_offset,timezoneName,ProjectId,Subjectid)
        if status:
            logger.info(f' {a} 数据组装完毕,准备发送数据... {a}')
            time.sleep(3)
            # 运行异步任务
            asyncio.run(send())
        else:
            logger.error(f'{a} 数据组装失败... {a}')
    else:
        status= assembly_data(count, deviceId, record, stampEndTime, stampStartTime,timeZone_offset,timezoneName,ProjectId,Subjectid)
        if status:
            logger.info(f' {a} 数据组装完毕,准备发送数据... {a}')
            time.sleep(3)
            # 运行异步任务
            asyncio.run(send())
        else:
            logger.error(f'{a} 数据组装失败... {a}')
    # ret_lock.release()

def assembly_data(count, deviceId, record, stampEndTime, stampStartTime,timeZone_offset,timezoneName,ProjectId,Subjectid):
    global recordTime, ecgDataList
    ecg_list, hr_list, rr_list = getEcgData()
    # ecg_list = getEcgData()

    while True:
        # 组装15秒数据
        if record + 30000 < stampEndTime:
            ecgDataList = []
            for i in range(count * 30, count * 30 + 30):
                recordTime = stampStartTime + (i * 1000)
                # ecgData = assemble_data_ECG(deviceId, recordTime, ecg_list[i], hr_list[i], rr_list[i])
                R=random.randint(0, 169)
                HR=random.choice([-101,-201,-301,-316,-401,0,1,11,22,33,44,55,66,77,88,99,100,151,181,199,200,300])
                # print("当前随机数:"+str(R) )
                # y=0
                # for  y in ecg_list[R]:
                #     y=y+1
                # if y != 128:
                #     print("有问题的数据是",y,R)

                # 获取accStepTotal
                accStepTotal=AccStep(recordTime,timeZone_offset).get_acc_step_total()

                ecgData = assemble_data_ECG(deviceId, recordTime, ecg_list[R], HR, 28,accStepTotal,timeZone_offset,timezoneName,ProjectId,Subjectid)

                record = recordTime
                ecgDataList.append(ecgData)
                # if len(ecgDataList)==9:
                #     break

            result = {"ecg": ecgDataList, "recordTime": recordTime}
            # print(result)
            queue.put(result)
            count += 1
            # record += 6000

        else:
            t = int((stampEndTime - record) // 1000)
            for i in range(count * 30, count * 30 + t):
                recordTime = stampStartTime + (i * 1000)
                R=random.randint(0, 300)
                HR = random.choice(
                    [-101, -201, -301, -316, -401, 0, 1, 11, 22, 33, 44, 55, 66, 77, 88, 99, 100, 151, 181, 199, 200,
                     300])
                # print("当前随机数:"+str(R) )

                # 获取accStepTotal
                accStepTotal=AccStep(recordTime,timeZone_offset).get_acc_step_total()

                ecgData = assemble_data_ECG(deviceId, recordTime, ecg_list[R], HR, 28,accStepTotal,timeZone_offset,timezoneName,ProjectId,Subjectid)

                # ecgData = assemble_data_ECG(deviceId, recordTime, ecg_list[R], 65, 28)
                record = recordTime
                ecgDataList.append(ecgData)
            result = {"ecg": ecgDataList, "recordTime": recordTime}
            queue.put(result)
            break
    return  True



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
    def __init__(self, recordTime,timezone_offset):
        self.recordTime = recordTime
        self.timezone_offset = timezone_offset
        self.steps = 0

    def __str__(self):
        return f"recordTime: {self.recordTime}, steps: {self.steps}"

    @staticmethod
    def get_time_str(timshift,timezone_offset):
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
        hour, min, sec = self.get_time_str(self.recordTime,self.timezone_offset)
        steps = int(hour) * 3600 + int(min) * 60 + int(sec) + 1
        self.steps = steps
        return steps
#字符串时间转换时间戳
def str_time_to_timestamp(str_time):
    return int(time.mktime(time.strptime(str_time, "%Y-%m-%d %H:%M:%S"))) * 1000



def start_up(Patient_Profile):
    # 计时
    start_time1=time.time()


    P = Patient_Profile
    days = P["Days"]
    str_time = P["StartTime"]

    end_time = P["EndTime"]

    start_send_time = P["Start_send_time"]
    data_sending_duration = P["Data_sending_duration"]









    for device in Patient_Profile['DeviceId']:
        logger.info(f'{"@"*200}')

        # 从str_time开始往后推送数据
        for i in range(days, -1, -1):
            modify_Time= ModifyTime(str_time, days=i).date_minus()

            if len(start_send_time)==0:
                start_time= ModifyTime(modify_Time, hours=0,minutes=0,seconds=0).date_plus() # 从几点开始发送
            elif len(start_send_time)>0:
                hour, minute, second = start_send_time
                if hour < 24 and minute < 60 and second < 60:
                    start_time = ModifyTime(modify_Time, hours=hour, minutes=minute, seconds=second).date_plus()
                else:
                    logger.error(f'数据错误！！,请检查start_send_time')
                    exit(1)
            else:
                logger.info(f'{start_send_time}列表填写不正确,请检查！')


                # end_time=ModifyTime(start_time,hours=23,minutes=59,seconds=59).date_plus()
            if end_time != "":
                end_time_1 = P["endTime"]
                start_time_1 = P["startTime"]
                end_time_1=ModifyTime(end_time_1,hours=00,minutes=00,seconds=00).date_plus()

                end_time_1=str_time_to_timestamp(end_time_1)
                start_time=str_time_to_timestamp(start_time_1)
                logger.info(f'转换时间戳开始结束时间：{start_time}-->{end_time_1}')
                if end_time_1 >= start_time:
                    end_time = P["endTime"]
                    start_time = P["StartTime"]
                else:
                    logger.error(f'结束时间大于开始时间，结束时间为：{P["endTime"]},开始时间为：{P["StartTime"]}，请重新设置结束时间！！！！')
                    break
            elif  end_time == "":
                if len(data_sending_duration) == 0:
                    end_time_1 = P["endTime"]
                    end_time=ModifyTime(end_time_1,hours=0,minutes=0,seconds=0).date_plus() # 发送几个小时数据
                elif len(data_sending_duration) > 0:
                    start_time = P["StartTime"]
                    hour, minute, second = data_sending_duration
                    if hour<24 and minute<60 and second<60:
                        end_time = ModifyTime(start_time, hours=hour, minutes=minute, seconds=second).date_plus()  # 发送几个小时数据
                    else:
                        logger.error(f'数据错误！！,请检查Data_sending_duration')
                        exit(1)
            else:
                start_time = P["startTime"]
                end_time = ModifyTime(start_time, hours=23, minutes=59, seconds=59).date_plus()

            logger.info(f"{a}↓↓↓ 发送Device：{device}  {start_time}-->{end_time} 的数据,Tenant：{P["ProjectId"]}, SubjectId：{P["SubjectId"]}, 数据数据偏移量：{P["TimeZoneOffset"]},数据时区：{P["TimeZone"]} ↓↓↓{a},")

            time.sleep(5)

            setup(start_time, end_time, device,P["TimeZoneOffset"],P["TimeZone"],P["ProjectId"],P["SubjectId"])

            logger.info(f"{a}↑↑↑ Device：{device}  {start_time}->{end_time} 数据发送完成 ↑↑↑{a}\n")

    #耗时
    end_time1=time.time()
    count_time= end_time1 - start_time1
    count_time=round(count_time,4)
    logger.info(f'{a}本次发送数据耗时：{count_time}秒 {a}')
    logger.info(f'{"@" * 200}')


"""

2025年09月06日10:33:13

未修改完成的脚本，等待修改
"""

if __name__ == '__main__':

    ret_lock = threading.Lock()

    # # @@@ +8时区
    # Patient_Profile ={
    #     "ProjectId": "Test_310",
    #     "SubjectId": "J20250323006",
    #     "DeviceId": ["ECGRec_202511/J032306"],
    #     "TimeZone": "Asia/Shanghai",
    #     "TimeZoneOffset": 28800,
    #     "StartTime": "2025-03-23 22:00:00",
    #     "endTime": "2025-03-23 22:50:00",
    #     "Days": 0
    # }
    # @@@ +8时区
    Patient_Profile ={
        "ProjectId": "Test_310",
        "SubjectId": "J20250423001",
        "DeviceId": ["ECGRec_202327/E111895"],
        "TimeZone": "Asia/Shanghai”",
        "TimeZoneOffset": 28800,
        "StartTime": "2025-04-30 12:00:00",
        "EndTime": "",
        "Days": 0,
        "Start_send_time": [0,0,0], # 时分秒
        "Data_sending_duration": [3,0,0] #时分秒
    }
    # @@@ +10时区
    # Patient_Profile ={
    #     "ProjectId": "Test_310",
    #     "SubjectId": "J20250323006",
    #     "DeviceId": ["ECGRec_202511/J032306"],
    #     "TimeZone": "Pacific/Saipan",
    #     "TimeZoneOffset": 36000,
    #     "StartTime": "2025-03-24 00:10:00",
    #     "EndTime": "",
    #     "Days": 0,
    #     "Start_send_time": [1,0,0], # 时分秒
    #     "Data_sending_duration": [2,1,59] #时分秒
    # }



    # Patient_Profile ={
    #     "ProjectId": "UAT_310",
    #     "SubjectId": "J20250411001",
    #     "DeviceId": ["ECGRec_202327/E111898"],
    #     "TimeZone": "Asia/Shanghai",
    #     "TimeZoneOffset": 28800,
    #     "StartTime": "2025-04-17 00:00:00",
    #     "EndTime": "",
    #     "Days": 0,
    #     "Start_send_time": [1,0,0], # 时分秒
    #     "Data_sending_duration": [2,0,0] #时分秒
    # }

    # Patient_Profile ={
    #     "ProjectId": "Sydney-VL-Reallife",
    #     "SubjectId": "J20250428002",
    #     "DeviceId": ["ECGRec_202420/E310614"],
    #     "TimeZone": "Asia/Shanghai",
    #     "TimeZoneOffset": 28800,
    #     "StartTime": "2025-04-28 12:00:00",
    #     "EndTime": "",
    #     "Days": 0,
    #     "Start_send_time": [0,0,0], # 时分秒
    #     "Data_sending_duration": [0,30,0] #时分秒
    # }
    # 开始发送
    start_up(Patient_Profile)

