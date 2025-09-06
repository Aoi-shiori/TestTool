"""
Time : 2021/2/4 13:16
Author : Rex
File : SendAllMultiUser.py
Software: PyCharm
"""
from threading import Lock
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue
import requests
import time
import random
from getEcgData import *
from apscheduler.schedulers.background import BlockingScheduler

pool = ThreadPoolExecutor(max_workers=50)


class DataSender():
    token = None
    interval_s = None
    ECG_Patch = None

    def __init__(self, ECG_Patch=None, Temperature_Patch=None, SpO2_Patch=None, BP_Patch=None, ProjectId=None,
                 SubjectId=None):
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

    def get_token(self):
        """
        获取token，用于后续发送接口请求
        :return: token信息
        """
        url = "https://vcloud2.vivalink.com/auth"

        payload = json.dumps({
            "id": "6170706009be6b1f2045cbac77",
            "key": "Whn_Nla;UtMLt@uUsQL]PDLx^?h46n<ri?v`K[@D"
        })
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        return response.json()['data']['token']

        # url = "https://vcloud-test.vivalink.com/auth"
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

    def assemble_ECG_data(self, recordTime, ECG, HR, RR, index):
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
            "deviceBattery": 100,
            "networkType": "WiFi",
            "sdkVersion": "2.2.4_beta1",
            "timezone": "28800",
            "carrier": "中国电信",
            "deviceOsVersion": "14.7.1",
            "recordTime": recordTime,
            "collectTime": collect,
            "profileId": "cyan",
            "data": {
                "deviceId": "CA:B2:78:25:17:88",
                "receiveTime": collect,
                "temperature": "26.00",
                "accAccuracy": 2048,
                "sf": "128",
                "deviceName": self.ECG_Patch,
                "dataMode": "fullDual",
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
                "battery": 100,
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
                "hr": HR
                # "hr": 108
            }
        }
        dict_350 = {
            "app_id": "com.vivalnk.vitalsmonitor",
            "category": "N/A",
            "collectTime": recordTime,
            "customData": {
                "sdkVersionName": "3.2.0",
                "product": "taimen",
                "appVersion": "3.2.6.303",
                "sdkCommonCode": "486",
                "appVersionName": "3.2.6.303",
                "appVersionCode": "303",
                "subjectId": "R",
                "manufacturer": "Google",
                "sdkVersionCode": "2463",
                "sdkCommonName": "1.3.1",
                "model": "Pixel 2 XL",
                "ID": "PPR1.180610.009",
                "projectId": "Hydrix",
                "device": "taimen",
                "brand": "google"
            },
            "data": {
                "acc": [{
                    "x": -586,
                    "y": -1970,
                    "z": -190
                }, {
                    "x": -568,
                    "y": -1931,
                    "z": -185
                }, {
                    "x": -554,
                    "y": -1992,
                    "z": -263
                }, {
                    "x": -578,
                    "y": -1943,
                    "z": -227
                }, {
                    "x": -561,
                    "y": -1976,
                    "z": -161
                }],
                "accAccuracy": 2048,
                "accActivity": 0,
                "accFrequency": 5,
                "accStepOffset": 0,
                "accStepTotal": 0,
                "activity": 0,
                "activityScore": 0,
                "avRR": -10001,
                "battery": 70,
                "calibratedTemp": "-1.0",
                "crc": 1,
                "dataMode": "FullDualMode",
                "deviceId": "C3:50:D0:03:56:BE",
                "deviceName":self.ECG_Patch,
                "deviceSN": "202239/E100069",
                "deviceType": "VV330_1",
                "ecg": [659, 679, 669, 661, 676, 689, 675, 689, 687, 672, 701, 672, 666, 676, 676, 671, 680, 665, 683,
                        669, 664, 678, 672, 661, 658, 686, 666, 656, 652, 658, 668, 669, 655, 683, 678, 682, 688, 683,
                        694, 691, 703, 705, 712, 711, 708, 683, 736, 722, 708, 728, 733, 716, 721, 717, 690, 708, 710,
                        689, 704, 703, 695, 683, 689, 686, 680, 664, 683, 663, 678, 676, 660, 679, 678, 661, 669, 662,
                        657, 674, 659, 656, 661, 645, 628, 631, 652, 642, 629, 637, 638, 642, 629, 623, 628, 629, 624,
                        648, 635, 611, 604, 611, 611, 611, 604, 601, 617, 589, 579, 595, 621, 613, 593, 591, 577, 585,
                        594, 583, 581, 570, 563, 571, 566, 561, 563, 570, 582, 564, 563, 559],
                "sf": 128,
                "flash": 0,
                "hr": HR,
                "leadOn": 1,
                "magnification": 1000,
                "noise": 1,
                "posture": 6,
                "rawTemp": "-1.0",
                "rmssd": 178,
                "rr": RR,
                "rri": [1078, 0, 0, 0, 0],
                "rssi": -63,
                "rwl": [106, -1, -1, -1, -1],
                "snr": 0.9878022560202077,
                "temperature": "-1.0",
                "recordTime": recordTime
            },
            "deviceBattery": 100,
            "deviceOsType": "Android",
            "deviceOsVersion": "9",
            "deviceToken": "N/A",
            "deviceType": "Pixel 2 XL",
            "language": "zh-CN",
            "name": "N/A",
            "networkType": "WIFI",
            "patchMessage": "{\"samplingMultiple\":1,\"hasHR\":0,\"channelNumber\":\"\",\"manufacturer\":\"VIVALNK\",\"leadOffAccEnable\":0,\"accSamplingFrequency\":5,\"encryption\":1,\"patchLeadStatus\":1,\"flashNum\":0,\"options\":{\"autoConnect\":1,\"connectRetry\":3,\"connectTimeout\":10000,\"extras\":{},\"rssiThreshold\":-95,\"serviceDiscoverRetry\":3,\"serviceDiscoverTimeout\":30000},\"patchSamplingStatus\":1,\"model\":\"VV330_1\",\"ecgSamplingFrequency\":128,\"accSamplingAccuracy\":2048,\"fwVersion\":\"3.0.0.0026\",\"sn\":\"202239/E100069\",\"cpuStatus\":2,\"productType\":1,\"batteryLevel\":1,\"accSamplingEnable\":1,\"magnification\":1000,\"accChipTempCalibration\":0,\"hwVersion\":\"01\",\"timeStamp\":1696067366,\"ackStatus\":1,\"batteryStatus\":\"NOT_INCHARGING\"}",
            "phoneCache": 0,
            "profileId": "N/A",
            "recordTime": recordTime,
            "sdkVersion": "3.2.0",
            "sensorBlocks": 0,
            "sensorId": self.ECG_Patch,
            "timezone": "28800",
            "type": "EcgRaw"
        },
        return dict

    def assemble_Temp_data(self, recordTime):
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
            "deviceBattery": 50,
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
                "displayTemp": random.choice(["37.01", "36.50", "36.56", "36.65", "36.70", "36.30", "36.21"]),
                "deviceType": "VV200",
                "flash": 0,
                "recordTime": recordTime,
                "battery": 20,
                "deviceId": "",
                "rawTemp": random.choice(["37.01", "36.50", "36.56", "36.65", "36.70", "36.30", "36.21"])
                # "rawTemp": raw
                # "rawTemp": "-37.11"
            }
        }

        return dict

    def assemble_SpO2_data(self, recordTime):
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
        dict["app_id"] = "com.vivalnk.mvm"
        dict["type"] = "SpO2Raw"
        dict["customData"] = {
            "subjectId": self.SubjectId,
            "projectId": self.ProjectId
        }
        dict["deviceOsType"] = "iOS"
        dict["sensorId"] = self.SpO2_Patch
        dict["deviceBattery"] = 100
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
        spo2 = [97, 98, 99, 95, 96]
        data["spo2"] = random.choice(spo2)
        # data["spo2"] = 99
        data["pr"] = 77
        data["pi"] = "1.5"
        data["battery"] = 71
        data["deviceType"] = "Checkme_O2"
        data["deviceSN"] = self.SpO2_Patch
        data["deviceName"] = self.SpO2_Patch
        data["chargerStatus"] = 0
        data["waveform"] = [
            137,
            137,
            136,
            136,
            135,
            135,
            134,
            133,
            131,
            130,
            127,
            124,
            121,
            117,
            112,
            108,
            103,
            99,
            96,
            93,
            90,
            89,
            88,
            88,
            88,
            89,
            90,
            92,
            93,
            95,
            96,
            98,
            99,
            101,
            102,
            103,
            105,
            106,
            107,
            108,
            108,
            111,
            112,
            114,
            115,
            116,
            117,
            118,
            118,
            119,
            120,
            120,
            121,
            121,
            122,
            123,
            123,
            124,
            125,
            126,
            128,
            129,
            130,
            131,
            132,
            134
        ]
        data["recordTime"] = recordTime
        data["steps"] = -1
        dict['data'] = data
        return dict

    def assemble_BP_data(self, recordTime):
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
                "dia": random.choice([71, 72, 73, 75, 77, 78, 80]),
                "deviceName": self.BP_Patch,
                "deviceType": "BP5C",
                "heartRate": 68,
                "flash": 0,
                "recordTime": recordTime,
                "hsdValue": 0,
                "battery": 100,
                "sys": random.choice([110, 111, 112, 115, 120, 131, 123, 125, 128]),
                "arrhythmia": 0
            }
        }

        return dict

    def send_realtime_data(self, payload):
        try:
            # url = "https://vcloud-test.vivalnk.com/v2/tenants/VivaLNK/events?type=dataEvent"
            url = "https://vcloud2.vivalnk.com/v2/tenants/VivaLNK/events?type=dataEvent"

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
            # url = "https://vcloud-test.vivalnk.com/v2/tenants/VivaLNK/events?type=dataEvent"
            url = "https://vcloud2.vivalnk.com/v2/tenants/VivaLNK/events?type=dataEvent"

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

    def assemble_send(self, startTime, ECG_list):
        """
        """

        self.send_array = []
        # self.t1 = time.time()
        for i in range(5):
            index = (self.count * 5) + i
            if index >= len(ECG_list):
                self.count = 0
                ECG = ECG_list[i]['ecg']
                HR = ECG_list[i]['hr']
                RR = ECG_list[i]['rr']
            else:
                ECG = ECG_list[index]['ecg']
                HR = ECG_list[index]['hr']
                RR = ECG_list[index]['rr']


            data_dict_ECG = self.assemble_ECG_data(startTime, ECG, HR, RR, index)
            self.send_array.append(data_dict_ECG)
            # if (index + 1) % 12 == 0:
            #     data_dict_Temp = self.assemble_Temp_data(startTime)
            #     self.delay_data_list.append(data_dict_Temp)
            # if (index + 1) % 4 == 0:
            #     data_dict_SpO2 = self.assemble_SpO2_data(startTime)
            #     self.delay_data_list.append(data_dict_SpO2)
            # if (index + 1) % 300 == 0:
            #     data_dict_BP = self.assemble_BP_data(startTime)
            #     self.delay_data_list.append(data_dict_BP)



            startTime += 1000
        self.count += 1

        # 5s 实时数据组装完成，发送数据
        # 使用线程池发送数据上传请求
        pool.submit(self.send_realtime_data, self.send_array)

def get_data_based_on_second():
    # 获取当前Unix时间戳
    current_timestamp = time.time()

    # 计算当前是一分钟的第几秒
    second_in_minute = int(current_timestamp % 60)

    # 根据不同的秒数返回不同的数据
    if 0 <= second_in_minute < 15:
        return f"当前是第 {second_in_minute} 秒，传输第一组数据"
    elif 15 <= second_in_minute < 30:
        return f"当前是第 {second_in_minute} 秒，传输第二组数据"
    elif 30 <= second_in_minute < 45:
        return f"当前是第 {second_in_minute} 秒，传输第三组数据"
    else:
        return f"当前是第 {second_in_minute} 秒，传输第四组数据"


# 创建一个方法定时调用对象的组装和发送
def sender_timer(obj_list, data_list10):
    startTime = int(time.time() * 1000) - 4000
    # print(startTime)
    for obj in obj_list:
        pool.submit(obj.assemble_send, startTime, data_list10)


if __name__ == '__main__':
    # V1 每个Sender对象拥有各自的定时器，20个对象就需要启动20个定时器
    # V2 创建1个定时器，管理20个对象

    # 创建ECG需要的数据
    data_list10 = getEcgData10()
    # 实例化20个Sender对象
    obj_list = []

    for t in range(5):
        number = 501 + t
        ecg_sensor = f"ECGRec_rpm236/060{number}"
        # ecg_sensor = "ECGRec_200000/E100001"
        number = 601 + t
        spo2_sensor = f"O2 rpm23{number}"
        number = 601 + t
        bp_sensor = f"BP5C_004Drpm23{number}"

        if t < 9:

            temp_sensor = f"F01.rpm2360{t + 1}"
            subjectID = f"P00{number}"
            data_sender = DataSender(ECG_Patch=ecg_sensor, Temperature_Patch=temp_sensor, SpO2_Patch=spo2_sensor,
                                     BP_Patch=bp_sensor, ProjectId="Sales", SubjectId=subjectID)
            print(f"generate:{t + 1} obj complete")
            obj_list.append(data_sender)
        else:
            temp_sensor = f"F01.rpm236{t + 1}"
            subjectID = f"P0{number}"
            data_sender = DataSender(ECG_Patch=ecg_sensor, Temperature_Patch=temp_sensor, SpO2_Patch=spo2_sensor,
                                     BP_Patch=bp_sensor, ProjectId="VL-Demo", SubjectId=subjectID)
            print(f"generate:{t + 1} obj complete")
            obj_list.append(data_sender)
    timer = BlockingScheduler()
    timer.add_job(sender_timer, 'interval', seconds=5, args=(obj_list, data_list10))
    timer.start()
