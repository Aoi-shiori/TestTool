# -*- coding: utf-8 -*-
"""
# @Creation time: 2025/09/06 14:38
# @Author       : 郭军
# @Email        : 391350540@qq.com
# @FileName     : SendAll_Multi_Device_Realtime.py
# @Software     : PyCharm
# @Project      : TestTool
# @PythonVersion: python 3.12
# @Version      :
# @Description  : 发送多设备实时数据数据到云端
# @Update Time  :
# @UpdateContent:
v2.0-20250906:
1、更新可以自定义每秒数据上传，暂时只支持 ECG 数据

"""
from threading import Lock
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue
import requests
import time
import random

from logger import logger

# from odbc import NUMBER
from typing import Dict, Any, List
from getEcgData import *
from apscheduler.schedulers.background import BlockingScheduler

pool = ThreadPoolExecutor(max_workers=50)


class DataSender():
    token = None
    interval_s = None
    ECG_Patch = None
    normal_HR_list = None
    normal_RR_list = None
    abnormal_HR_list = None
    abnormal_RR_list = None
    # temp = random.choice([i for i in range(35, 38)])
    normal_temp_list = None
    abnormal_temp_list = None

    normal_spo2_list = None
    abnormal_spo2_list = None


    normal_bp_dia_list = None
    normal_bp_sys = None
    data_list =None
    alert_case_list = None
    def __init__(self, ECG_Patch=None, Temperature_Patch=None, SpO2_Patch=None, BP_Patch=None, ProjectId=None,
                 SubjectId=None,normal_HR_list=None,normal_RR_list=None,abnormal_HR_list=None,
                                 abnormal_RR_list=None, normal_temp_list=None, abnormal_temp_list=None, normal_spo2_list=None, abnormal_spo2_list=None, normal_bp_dia_list=None,normal_bp_sys=None,data_list=None,alert_case_list=None,data_router=None):
        self.token = self.get_token()
        self.queue = Queue()
        self.delay_data_list = []
        self.send_history = []
        self.count = 0
        self.ECG_Patch = ECG_Patch
        self.Temperature_Patch = Temperature_Patch
        self.SpO2_Patch = SpO2_Patch
        self.BP_Patch = BP_Patch
        self.ProjectId = ProjectId
        self.SubjectId = SubjectId
        self.lock = Lock()
        self.normal_HR_list = normal_HR_list
        self.normal_RR_list = normal_RR_list
        self.abnormal_HR_list = abnormal_HR_list
        self.abnormal_RR_list = abnormal_RR_list
        self.normal_temp_list = normal_temp_list
        self.abnormal_temp_list = abnormal_temp_list
        self.normal_spo2_list = normal_spo2_list
        self.normal_bp_dia_list = normal_bp_dia_list
        self.normal_bp_sys = normal_bp_sys
        self.abnormal_spo2_list = abnormal_spo2_list
        self.alert_case_list = alert_case_list
        self.data_list= data_list
        self.number = 1
        self.data_router=data_router


    def get_token(self):
        """
        获取token，用于后续发送接口请求
        :return: token信息
        """
        # url = "https://vcloud2.vivalink.com/auth"
        # url = "https://site2-vcloud.vivalink.com/auth"
        # url = "https://site3-vcloud.vivalink.com/auth"
        # url = "https://site4-vcloud.vivalink.com/auth"
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
        #
        # return response.json()['data']['token']

        url = "https://vcloud-test.vivalink.com/auth"
        # url = "https://site2-vcloud-test.vivalink.com/auth"

        payload = json.dumps({
            "id": "617070e40daf63ba334ece90d1",
            "key": "@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF"
        })
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        return response.json()['data']['token']

    def assemble_ECG_data(self, recordTime,  ECG, HR, RR, TEMP,TimeZoneOffset,TimeZoneName, index):
        """
        组装ECG数据
        :param recordTime:
        :param deviceId:
        :param projectId:
        :param subjectId:
        :param ECG:
        :param HR:
        :param RR:
        :param index:
        :return:
        """
        collect = int(time.time() * 1000)
        dict = {
            "longtitude": 120.22000122070312,
            "language": "zh-Hans-CN",
            "receiveTime": 0,
            "deviceType": "iPhone",
            "patchMessage": "{\"fwVersion\":\"2.2.0.0041\",\"cpuStatus\":0,\"batteryStatus\":\"NOT_INCHARGEING\",\"timeStamp\":1648020731,\"magnification\":1000,\"accSamplingEnable\":1,\"hwVersion\":\"08\",\"ackStatus\":0,\"flashNum\":860,\"leadOffAccEnable\":0}",
            "deviceToken": "6f6b8a0683b1c2735cd5e5c70c9fef531b63682d",
            "category": "not know",
            "latitude": 30.180000305175781,
            "deviceIp": "10.10.1.174",
            "name": "",
            "app_id": "com.vivalnk.mvm",
            "type": "EcgRaw",
            "customData": {
                "subjectId": self.SubjectId,
                "projectId": self.ProjectId
            },
            "deviceOsType": "iOS",
            "sensorId": self.ECG_Patch,
            "deviceBattery": 90,
            "networkType": "WiFi",
            "sdkVersion": "2.2.4_beta1",
            "timezone": f"{TimeZoneOffset}",
            "timezoneName": TimeZoneName,
            "carrier": "中国电信",
            "deviceOsVersion": "14.7.1",
            "recordTime": recordTime,
            "collectTime": collect,
            "profileId": "cyan",
            "data": {
                "deviceId": "CA:B2:78:25:17:88",
                "receiveTime": collect,
                "temperature": TEMP,
                "rawTemp": TEMP,
                "accAccuracy": 2048,
                "sf": "128",
                "deviceName": self.ECG_Patch,
                "dataMode": "fullDual",
                # "rr": 81,
                "rr": RR,
                # "rr": -101,
                "activity": random.choice([0, 1]),
                "rwl": [
                    27,
                    71,
                    106,
                    -1,
                    -1
                ],
                "magnification": 1000,
                "avRR": 25,
                "rmssd": random.choice([55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65]),
                "battery": 15,
                "acc": [{
                    "x": -15,
                    "y": 67,
                    "z": -2022
                },
                    {
                        "x": -8,
                        "y": 60,
                        "z": -2030
                    },
                    {
                        "x": -18,
                        "y": 63,
                        "z": -2028
                    },
                    {
                        "x": -15,
                        "y": 65,
                        "z": -2036
                    },
                    {
                        "x": -16,
                        "y": 64,
                        "z": -2027
                    }
                ],
                "deviceSN": self.ECG_Patch,
                "leadOn": 1,
                "rri": [
                    242,
                    257,
                    242,
                    242,
                    0
                ],
                # "ecg": [7, 11, 22, 15, 16, 21, 20, 6, 15, 11, 14, 11, 4, 4, 6, 6, -2, 5, 0, 2, 8, 9, 4, 7, 0, 2, -3, -1,
                #         -2, 1,
                #         0,
                #         -3, -5, 0, 3, -9, -7, -4, -13, -7, -13, -11, -9, -16, -12, -7, -15, -15, -12, -13, -22, -17,
                #         -19, -15,
                #         -14,
                #         -13, -8, 3, 9, 17, 16, 4, -5, 2, 9, 4, -11, -19, -30, -32, -24, -25, -32, -44, -63, 110, 405,
                #         544, 99,
                #         -327,
                #         -355, -314, -203, -115, -91, -78, -64, -59, -52, -40, -32, -22, 1, 21, 58, 97, 149, 190, 227,
                #         236, 231,
                #         207,
                #         170, 124, 82, 43, 17, -6, -16, -27, -29, -41, -37, -35, -39, -37, -41, -36, -41, -42, -43, -52,
                #         -54, -48,
                #         -50, -47, -48, -45],
                "ecg": ECG,
                "recordTime": recordTime,
                "flash": 0,
                "modeType": "mode4",
                # "hr": 121
                "hr": HR
            }
        }
        return dict

    def assemble_Temp_data(self, recordTime,temp):
        dict = {
            "longtitude": 0,
            "language": "en-CN",
            "receiveTime": 0,
            "deviceType": "iPhone",
            "patchMessage": "{\"firmware\":\"N\\\/A\",\"chargerFw\":\"N\\\/A\",\"chargeBatteryStatus\":\"N\\\/A\"}",
            "deviceToken": "80447b01d0a5b38e1e864ca746cfadcc616464fb",
            "category": "notknow",
            "latitude": 0,
            "deviceIp": "10.10.1.117",
            "name": "",
            "app_id": "com.vivalnk.mvm",
            "type": "TemperatureRaw",
            "customData": {
                "appVersion": "3.0.0.9",
                "subjectId": self.SubjectId,
                "projectId": self.ProjectId
            },
            "deviceOsType": "iOS",
            "sensorId": self.Temperature_Patch,
            "deviceBattery": 70,
            "networkType": "WiFi",
            "sdkVersion": "3.0.1_beta3",
            "timezone": "28800",
            "carrier": "中国电信",
            "deviceOsVersion": "15.7",
            "recordTime": recordTime,
            "collectTime": recordTime,
            "profileId": "",
            "data": {
                "deviceSN": self.Temperature_Patch,
                "fw": "N\/A",
                "rssi": -35,
                "deviceName": self.Temperature_Patch,
                # "displayTemp": random.choice(["36.50", "36.56", "36.65", "36.70", "37.30", "37.61","37.61", "37.61", "37.61", "37.61", "37.61", "37.61", "37.61", "37.61"]),
                # "displayTemp": "37.1",
                "displayTemp": f"{temp}",
                "deviceType": "VV200",
                "flash": 0,
                "recordTime": recordTime,
                "battery": 55,
                "deviceId": "",
                # "rawTemp": random.choice(["36.50", "36.56", "36.65", "36.70", "37.30", "37.61","37.61", "37.61", "37.61", "37.61", "37.61", "37.61", "37.61", "37.61"])
                # "rawTemp": "37.1"
                "rawTemp": f"{temp}"
                # "rawTemp": "-37.11"
            }
        }

        return dict

    def assemble_SpO2_data(self, recordTime,spo2):
        dict = {}
        dict["longtitude"] = 120.22000122070312
        dict["language"] = "zh-Hans-CN"
        dict["receiveTime"] = 0
        dict["deviceType"] = "iPhone"
        dict[
            "patchMessage"] = "{\"fwVersion\":\"2.2.0.0041\",\"cpuStatus\":0,\"batteryStatus\":\"NOT_INCHARGEING\",\"timeStamp\":1648020881,\"magnification\":1000,\"accSamplingEnable\":1,\"hwVersion\":\"08\",\"ackStatus\":0,\"flashNum\":326,\"leadOffAccEnable\":0}"
        dict["deviceToken"] = "6f6b8a0683b1c2735cd5e5c70c9fef531b63682d"
        dict["category"] = "not know"
        dict["latitude"] = 30.180000305175781
        dict["deviceIp"] = "10.10.1.174"
        dict["name"] = ""
        dict["app_id"] = "com.vivalnk.vitalsmonitor"
        dict["type"] = "SpO2Raw"
        dict["customData"] = {
            "subjectId": self.SubjectId,
            "projectId": self.ProjectId,
            "appVersion":"3.3.0.319"
        }
        dict["deviceOsType"] = "iOS"
        dict["sensorId"] = self.SpO2_Patch
        dict["deviceBattery"] = 88
        dict["networkType"] = "WiFi"
        dict["sdkVersion"] = "2.2.4_beta1"
        dict["timezone"] = "28800"
        dict["carrier"] = "中国电信"
        dict["deviceOsVersion"] = "14.7.1"
        dict["recordTime"] = recordTime
        dict["collectTime"] = recordTime
        dict["profileId"] = "cyan"
        data = {}
        data["flash"] = 0
        # spo2 = [97, 98, 99, 95, 96]
        # spo2 = [o for o in range(80,100)]
        # data["spo2"] = random.choice(spo2)
        data["spo2"] = spo2
        data["pr"] = 77
        data["pi"] = "1.5"
        data["battery"] = 71
        data["deviceType"] = "Checkme_O2"
        data["deviceSN"] = self.SpO2_Patch
        data["deviceName"] = self.SpO2_Patch
        data["chargerStatus"] = 0
        data["waveform"] = []
        data["recordTime"] = recordTime
        data["steps"] = -1
        dict['data'] = data
        return dict

    def assemble_BP_data(self, recordTime,bp_dia,bp_sys):
        dict = {
            "longtitude": 120.22000122070312,
            "language": "zh-Hans-CN",
            "receiveTime": 0,
            "deviceType": "iPhone",
            "patchMessage": "",
            "deviceToken": "6f6b8a0683b1c2735cd5e5c70c9fef531b63682d",
            "category": "not know",
            "latitude": 30.180000305175781,
            "deviceIp": "10.10.1.174",
            "name": "",
            "app_id": "com.vivalnk.mvm",
            "type": "BPRaw",
            "customData": {
                "subjectId": self.SubjectId,
                "projectId": self.ProjectId
            },
            "deviceOsType": "iOS",
            "sensorId": self.BP_Patch,
            "deviceBattery": 100,
            "networkType": "WiFi",
            "sdkVersion": "2.2.4_beta1",
            "timezone": "28800",
            "carrier": "中国电信",
            "deviceOsVersion": "14.7.1",
            "recordTime": recordTime,
            "collectTime": recordTime,
            "profileId": "cyan",
            "data": {
                "deviceId": "00:4D:32:0F:4A:9E",
                # "dia": random.choice([71, 72, 73, 75, 77, 78, 80]),
                # "dia": 151,
                "dia": bp_dia,
                "deviceName": self.BP_Patch,
                "deviceType": "BP5C",
                "heartRate": 68,
                "flash": 0,
                "recordTime": recordTime,
                "hsdValue": 0,
                "battery": 100,
                # "sys": random.choice([110, 111, 112, 115, 120, 131, 123, 125, 128]),
                # "sys": 151,
                "sys": bp_sys,
                "arrhythmia": 0
            }
        }

        return dict

    def send_realtime_data(self, payload):
        try:
            url = "https://vcloud-test.vivalnk.com/v2/tenants/First/events?type=dataEvent"
            # url = "https://site2-vcloud-test.vivalink.com/tenants/First/events?type=dataEvent"
            # url = "https://vcloud2.vivalink.com/tenants/Vivalink-202301/events?type=dataEvent"
            # url = "https://site2-vcloud.vivalink.com/tenants/Vivalink-202301/events?type=dataEvent"
            # url = "https://site3-vcloud.vivalink.com/tenants/DF6215-001-Reallife/events?type=dataEvent"
            # url = "https://site4-vcloud.vivalink.com/tenants/IMMUNE-DCM-Reallife/events?type=dataEvent"

            headers = {
                'Content-Type': 'application/json',
                'Authorization': self.token
            }
            response = requests.post(url, json=payload, headers=headers)
            t2 = time.time()
            # print(f"start--done cost : {t2 - self.t1}" + "\n")
            if self.interval_s == None:
                pass
            else:
                print(f"Request completion interval: {t2 - self.interval_s}" + "\n")
            self.interval_s = t2
            if json.loads(response.content)['code'] == 200 and json.loads(response.content)[
                'message'] == 'Batch ingestion done':
                for data_dict_ECG in self.send_array:
                    data_dict_ECG["data"]["flash"] = 1
                    self.queue.put(data_dict_ECG)
                # 判断队列长度是否达到15，是否应该发送历史数据
                print(self.queue.qsize())
                if self.queue.qsize() == 15:
                    # print(len(self.delay_data_list))
                    for data_dict_ECG in self.delay_data_list:
                        self.queue.put(data_dict_ECG)
                    self.delay_data_list = []
                    # 发送时将历史数据的collectTime进行更新
                    collect_time = int(time.time() * 1000)
                    while not self.queue.empty():
                        data = self.queue.get()
                        data["collectTime"] = collect_time
                        self.send_history.append(data)
                    pool.submit(self.send_history_data, self.send_history)
                return True
            else:
                with open("./log.txt") as file:
                    file.write(f"数据发送失败:{int(time.time() * 1000)}" + "\n")
                print('Data send failed')
                print('发送失败')
            # 将ECG 实时数据保存成历史数据
        except Exception as e:
            with open("./log.txt") as file:
                file.write(f"数据发送失败:{int(time.time() * 1000)}" + "\n")
            print('Data send failed')
            print("数据发送出错")
            print(e)

    def send_history_data(self, payload):
        try:
            url = "https://vcloud-test.vivalnk.com/v2/tenants/First/events?type=dataEvent"
            # url = "https://site2-vcloud-test.vivalink.com/tenants/First/events?type=dataEvent"
            # url = "https://site2-vcloud.vivalink.com/tenants/Vivalink-202301/events?type=dataEvent"
            # url = "https://site3-vcloud.vivalink.com/tenants/DF6215-001-Reallife/events?type=dataEvent"
            # url = "https://site4-vcloud.vivalink.com/tenants/IMMUNE-DCM-Reallife/events?type=dataEvent"

            headers = {
                'Content-Type': 'application/json',
                'Authorization': self.token
            }
            response = requests.post(url, json=payload, headers=headers)

            if json.loads(response.content)['code'] == 200 and json.loads(response.content)[
                'message'] == 'Batch ingestion done':
                self.send_history = []
                # print("数据发送成功")
                return True
            else:
                with open("./log.txt") as file:
                    file.write(f"数据发送失败:{int(time.time() * 1000)}" + "\n")
                print('Data send failed')
                print('发送失败')
                return False
        except Exception as e:
            with open("./log.txt") as file:
                file.write(f"数据发送失败:{int(time.time() * 1000)}" + "\n")
            print('Data send failed')
            print("数据发送出错")
            print(e)
            return False

    def assemble_send(self, startTime):
        """
        """
        # HR 60-100
        # RR 12-20

        # # 取第几条CASE
        # k = 7
        # print(self.alert_case_list[k])

        self.send_array = []
        # self.t1 = time.time()
        for i in range(5):
            index = (self.count * 5) + i
            if index >= len(self.data_list):
                self.count = 0
                ECG = self.data_list[i]['ecg']
            else:
                ECG = self.data_list[index]['ecg']

            # self.RR = random.choice(self.normal_RR_list)


            # #处理数据值
            # self.RR = random.choice(self.abnormal_RR_list)
            #
            # if 1<=self.number <=60:
            #     self.HR = random.choice(self.alert_case_list[k]["1min"])
            # if 61<=self.number <=120:
            #     self.HR = random.choice(self.alert_case_list[k]["2min"])
            # if 121<=self.number <=180:
            #     self.HR = random.choice(self.alert_case_list[k]["3min"])
            # if 181<=self.number <=240:
            #     self.HR = random.choice(self.alert_case_list[k]["4min"])
            # if 241<=self.number <=300:
            #     self.HR = random.choice(self.alert_case_list[k]["5min"])


            # 数据路由数据处理
            #  "data": {"HR": 11, "RR": 15, "Temp": 32.3,"TimeZoneOffset": 28800,"TimeZoneName": ""},
            result = self.data_router.get_data_for_timestamp(startTime)
            hr, rr, temp,timeZoneoffset,timezonename= result['data']["HR"], result['data']["RR"], result['data']["Temp"],result['data']["TimeZoneOffset"],result['data']["TimeZoneName"]
            self.HR = hr
            self.RR = rr
            self.TEMP = temp
            self.TimeZoneOffset = timeZoneoffset
            self.TimeZoneName = timezonename



            # 设置不发送数据的时间段
            if 0<=self.number<=0:
                print(self.number,"停止发送！")
            else:

                data_dict_ECG = self.assemble_ECG_data(startTime, ECG, self.HR, self.RR,self.TEMP,self.TimeZoneOffset,self.TimeZoneName,index)

                self.send_array.append(data_dict_ECG)
                # logger.info(f"{startTime, self.HR, self.RR, self.TEMP,self.TimeZoneOffset,self.TimeZoneName}")

                ecgData=data_dict_ECG

                # 处理timezoneName字段

                if not ecgData["timezoneName"]:
                    ecgData.pop("timezoneName", None)

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

                if (index + 1) % 12 == 0:
                    # self.temp = random.choice(self.normal_temp_list)
                    self.temp = random.choice(self.abnormal_temp_list)
                    if 350 < (index + 1) < 600:
                        self.temp = random.choice(self.abnormal_temp_list)
                    data_dict_Temp = self.assemble_Temp_data(startTime, self.temp)
                    self.delay_data_list.append(data_dict_Temp)
                    # self.send_array.append(data_dict_Temp)
                if (index + 1) % 4 == 0:
                    # self.spo2 = random.choice(self.normal_spo2_list)
                    self.spo2 = random.choice(self.abnormal_spo2_list)
                    if self.number >= 300:
                        self.spo2 = random.choice(self.abnormal_spo2_list)
                    data_dict_SpO2 = self.assemble_SpO2_data(startTime, self.spo2)
                    self.delay_data_list.append(data_dict_SpO2)
                    # self.send_array.append(data_dict_SpO2)
                # if (index + 1) % 60 == 0:
                if (index + 1) % 300 == 0:
                    self.bp_dia = random.choice(self.normal_bp_dia_list)
                    self.bp_sys = random.choice(self.normal_bp_sys)
                    if 20 < (index + 1) < 150:
                        self.bp_dia = random.choice([i for i in range(50, 60)])
                    data_dict_BP = self.assemble_BP_data(startTime, self.bp_dia, self.bp_sys)
                    self.delay_data_list.append(data_dict_BP)

            startTime += 1000
            self.count += 1
            self.number +=1
            if self.number >=301:
                self.number =1
        # 5s 实时数据组装完成，发送数据
        # 使用线程池发送数据上传请求
        pool.submit(self.send_realtime_data, self.send_array)

# 数据路由
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
        HR = random.choice( [-101, -201, -301, -316, -401, 0, 1, 11, 22, 33, 44, 55, 66, 77, 88, 99, 100, 151, 181, 199, 200, 300])
        RR = random.choice([15,18,19,20])
        Temp = random.choice([33.2,20,44])

        return {
            'second': second,
            "data": {"HR": HR, "RR": RR, "Temp": Temp},
            'Note': "默认随机列表数据",
            'priority': '默认',
            'timestamp': timestamp,
            'normalized_timestamp': self.normalize_timestamp(timestamp),
            'message': '没有匹配的时间范围,返回默认随机数据'
        }


# 创建一个方法定时调用对象的组装和发送
def sender_timer(obj_list):
    # startTime = int(time.time())* 1000-4000 - 660000
    startTime = int(time.time())* 1000-4000
    # print(startTime)
    for obj in obj_list:
        pool.submit(obj.assemble_send,startTime)


if __name__ == '__main__':
    # V1 每个Sender对象拥有各自的定时器，20个对象就需要启动20个定时器
    # V2 创建1个定时器，管理20个对象

    # 创建ECG需要的数据
    # data_list10 = getEcgData10()
    # data_list9 = getEcgData9()
    # data_list8 = getEcgData8()
    data_list8 = getEcgData7()
    alert_case_list = getEcgData11()

    # 数据路由默认配置（目前只修改 ECG）
    DEFAULT_CONFIG = {
        "timestamp_formats": {
            "second": 10,
            "millisecond": 13,
            "microsecond": 16,
            "nanosecond": 19
        },
        "time_ranges": [
            {
                "range": [0, 15],
                "data": {"HR": 11, "RR": 15, "Temp": 32.3,"TimeZoneOffset": 28800,"TimeZoneName": "Asia/Shanghai"}, #数据都要传，可以传固定值 TimeZoneName不配置 TimeZoneName字段不传，Temp体温数据 Raw Temp 全部修改
                "Note": "Cass1",
                "priority": "high"
            },
            {
                "range": [15, 30],
                "data": {"HR": 11, "RR": 15, "Temp": 32.3,"TimeZoneOffset": 28800,"TimeZoneName": "Asia/Shanghai"},
                "Note": "Cass1",
                "priority": "medium"
            },
            {
                "range": [30, 45],
                "data": {"HR": 11, "RR": 15, "Temp": 32.3,"TimeZoneOffset": 28800,"TimeZoneName": "Asia/Shanghai"},
                "Note": "Cass1",
                "priority": "low"
            },
            {
                "range": [45, 60],
                "data": {"HR": 11, "RR": 15, "Temp": 32.3,"TimeZoneOffset": 28800,"TimeZoneName": "Asia/Shanghai"},
                "Note": "Cass1",
                "priority": "lowest"
            }
        ]
    }

    # 初始化数据路由
    data_router=TimestampDataRouter(config=DEFAULT_CONFIG)

    # 实例化20个Sender对象
    obj_list = []
    for t in range(1):
        # J001
        # ecg_sensor = f"ECGRec_202409/E318130"
        # J022
        # ecg_sensor = f"ECGRec_JUN/E310614"
        # temp_sensor = f"Temp_AOJ-20F_A4C138310065"
        """
        #J019测试病人设备
        ecg_sensor = f"ECGRec_JUN/E310001"
        temp_sensor = f"Temp_AOJ-20F_AJUN00000002"
        bp_sensor = "BP5S_00J00000002"
        spo2_sensor = "O2_C208S_J87C6F900001"
        """

        """
        #J018测试病人设备
                """
        ProjectId="Sunshine_J001"
        SubjectId="J018"
        ecg_sensor = f"ECGRec_JUN/E310003"
        temp_sensor = f"Temp_AOJ-20F_AJUN00000003"
        bp_sensor = "BP5S_00J00000003"
        spo2_sensor = "O2_C208S_J87C6F900003"


        """
        #J018测试病人设备
        ecg_sensor = f"ECGRec_JUN/E310004"
        temp_sensor = f"Temp_AOJ-20F_AJUN00000004"
        bp_sensor = "BP5S_00J00000004"
        spo2_sensor = "O2_C208S_J87C6F900004"
        """

        # 目前不读取，不要使用
        normal_HR_list = [i for i in range(51,91)]
        normal_RR_list = [i for i in range(19, 20)]
        abnormal_HR_list = [i for i in range(111,131)]
        abnormal_RR_list = [i for i in range(25, 35)]
        # temp = random.choice([i for i in range(35, 38)])
        # normal_temp_list = [round(random.uniform(36.1, 37.2), 1) for _ in range(10)]
        normal_temp_list = [round(random.uniform(38.1, 39.2), 1) for _ in range(3)]
        abnormal_temp_list = [round(random.uniform(36.1, 37.2), 1) for _ in range(10)]

        normal_spo2_list = [i for i in range(95, 100)]
        abnormal_spo2_list = [i for i in range(90, 95)]

        normal_bp_dia_list = [i for i in range(80, 90)]
        normal_bp_sys = [i for i in range(130, 140)]



        data_sender = DataSender(ECG_Patch=ecg_sensor, Temperature_Patch=temp_sensor, SpO2_Patch=spo2_sensor,
                                 BP_Patch=bp_sensor, ProjectId=ProjectId, SubjectId=SubjectId,
                                 normal_HR_list=normal_HR_list, normal_RR_list=normal_RR_list,
                                 abnormal_HR_list=abnormal_HR_list,
                                 abnormal_RR_list=abnormal_RR_list, normal_temp_list=normal_temp_list,
                                 abnormal_temp_list=abnormal_temp_list, normal_spo2_list=normal_spo2_list,
                                 abnormal_spo2_list=abnormal_spo2_list, normal_bp_dia_list=normal_bp_dia_list
                                 , normal_bp_sys=normal_bp_sys,data_list=data_list8,alert_case_list=alert_case_list,data_router=data_router,)
        obj_list.append(data_sender)


    timer = BlockingScheduler()
    timer.add_job(sender_timer, 'interval', seconds=5, args=(obj_list,))
    timer.start()
