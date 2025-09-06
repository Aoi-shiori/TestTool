# -*- ecoding: utf-8 -*-
# @ModuleName: send_data
# @Author: Rex
# @Time: 2023/11/21 16:57
# 要绑定的ECG数量
import gzip
import json
import random
import requests
from concurrent.futures import ThreadPoolExecutor,as_completed
import time
from queue import Queue

q = Queue()
def create_ecg(ecg_count,ECGPatch):
    # 获取ECG设备号/后的值
    ECG_list = []
    for item in range(ecg_count):
        if item < 10:
            ECG_Patch = f"{ECGPatch[0:-1]}{str(item)}"
            ECG_list.append(ECG_Patch)
        elif item<100:
            ECG_Patch = f"{ECGPatch[0:-2]}{str(item)}"

            ECG_list.append(ECG_Patch)
        elif item<1000:
            ECG_Patch = f"{ECGPatch[0:-3]}{str(item)}"
            ECG_list.append(ECG_Patch)
        elif item<10000:
            ECG_Patch = f"{ECGPatch[0:-4]}{str(item)}"
            ECG_list.append(ECG_Patch)
        else:
            print("暂不支持这么多设备,或者设备数量为0")
    return ECG_list

def create_spo2(spo2_count, SpO2Patch):
    # 获取ECG设备号/后的值
    SpO2_list = []
    for item in range(spo2_count):
        if item < 10:
            SpO2_Patch = f"{SpO2Patch[0:-1]}{str(item)}"
            SpO2_list.append (SpO2_Patch)
        elif item<100:
            SpO2_Patch = f"{SpO2Patch[0:-2]}{str(item)}"
            SpO2_list.append (SpO2_Patch)
        elif item<1000:
            SpO2_Patch = f"{SpO2Patch[0:-3]}{str(item)}"
            SpO2_list.append (SpO2_Patch)
        elif item<10000:
            SpO2_Patch = f"{SpO2Patch[0:-4]}{str(item)}"
            SpO2_list.append (SpO2_Patch)
        else:
            print("暂不支持这么多设备,或者设备数量为0")
    return SpO2_list

def create_bp(bp_count, BPPatch,):
    # 获取ECG设备号/后的值
    BP_list = []
    for item in range(bp_count):
        if item < 10:
            BP_Patch = f"{BPPatch[0:-1]}{str(item)}"
            BP_list.append(BP_Patch)
        elif item<100:
            BP_Patch = f"{BPPatch[0:-2]}{str(item)}"
            BP_list.append(BP_Patch)
        elif item<1000:
            BP_Patch = f"{BPPatch[0:-3]}{str(item)}"
            BP_list.append(BP_Patch)
        elif item<10000:
            BP_Patch = f"{BPPatch[0:-4]}{str(item)}"
            BP_list.append(BP_Patch)
        else:
            print("暂不支持这么多设备,或者设备数量为0")
    return BP_list



def create_total_patch(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch):
    # 拼接sql中需要的values
    ecg_list =create_ecg(ecg_count, ECGPatch)
    spo2_list = create_spo2(spo2_count, SpO2Patch)
    bp_list = create_bp(bp_count, BPPatch)
    total_list = ecg_list+spo2_list+bp_list
    return total_list

def get_token():
    """
    获取token，用于后续发送接口请求
    :return: token信息
    """
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
    #
    # return response.json()['data']['token']

    url = "https://test.vivalink.com/internal/auth"

    payload = json.dumps({
        "id": "617070e40daf63ba334ece90d1",
        "key": "@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF"
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()['data']['token']

def assemble_ECG_data(deviceId,recordTime):
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
            "subjectId": "R-001",
            "projectId": "Second"
        },
        "deviceOsType": "iOS",
        "sensorId": deviceId,
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
            "deviceName": deviceId,
            "dataMode": "fullDual",
            "rr": random.choice([i for i in range(40,80)]),
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
            "deviceSN": deviceId,
            "leadOn": 1,
            "rri": [
                242,
                257,
                242,
                242,
                0
            ],
            "ecg": [7, 11, 22, 15, 16, 21, 20, 6, 15, 11, 14, 11, 4, 4, 6, 6, -2, 5, 0, 2, 8, 9, 4, 7, 0, 2, -3, -1,
                    -2, 1,
                    0,
                    -3, -5, 0, 3, -9, -7, -4, -13, -7, -13, -11, -9, -16, -12, -7, -15, -15, -12, -13, -22, -17,
                    -19, -15,
                    -14,
                    -13, -8, 3, 9, 17, 16, 4, -5, 2, 9, 4, -11, -19, -30, -32, -24, -25, -32, -44, -63, 110, 405,
                    544, 99,
                    -327,
                    -355, -314, -203, -115, -91, -78, -64, -59, -52, -40, -32, -22, 1, 21, 58, 97, 149, 190, 227,
                    236, 231,
                    207,
                    170, 124, 82, 43, 17, -6, -16, -27, -29, -41, -37, -35, -39, -37, -41, -36, -41, -42, -43, -52,
                    -54, -48,
                    -50, -47, -48, -45],
            # "ecg": ECG,
            "recordTime": recordTime,
            "flash": 0,
            "modeType": "mode4",
            "accFrequency":10,
            "hr": random.choice([i for i in range(65,130)])
            # "hr": 108
        }
    }
    # print(dict)
    return dict

def assemble_Temp_data(deviceId,recordTime):
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
            "subjectId": "R-001",
            "projectId": "Second"
        },
        "deviceOsType": "iOS",
        "sensorId": deviceId,
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
            "deviceSN": deviceId,
            "fw": "N\/A",
            "rssi": -35,
            "deviceName": deviceId,
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

def assemble_SpO2_data(deviceId,recordTime):
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
        "subjectId": "R-001",
        "projectId": "Second"
    }
    dict["deviceOsType"] = "iOS"
    dict["sensorId"] = deviceId
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
    data["deviceSN"] = deviceId
    data["deviceName"] = deviceId
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

def assemble_BP_data(deviceId,recordTime):
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
            "subjectId": "R-001",
            "projectId": "Second"
        },
        "deviceOsType": "iOS",
        "sensorId": deviceId,
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
            "deviceName": deviceId,
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

def sendDataToVCloud(payload, recordTime, token):
    # pass
    try:
        # url = "https://vcloud-test.vivalnk.com/v2/tenants/VivaLNK/events?type=dataEvent"
        # url = "https://vcloud2.vivalnk.com/v2/tenants/VivaLNK/events?type=dataEvent"
        # url = "https://vcloud-california-test.vivalink.com/tenants/VivaLNK/events?type=dataEvent"
        url = "https://test.vivalink.com/internal/tenants/VivaLNK/events?type=dataEvent"

        headers = {
            'Content-Type': 'application/json',
            'Authorization': token,
            # "Accept-Encoding": "gzip",
            "Connection": "keep-alive"
        }

        response = requests.post(url, json=payload, headers=headers)

        # print(response.json())
        sensorId=payload[0]["sensorId"]

        # if response.headers.get("Content-Encoding")=="gzip":
        #     response=gzip.decompress(response.content).decode()
        # else:
        #     response=response

        if json.loads(response.content)['code'] == 200 and json.loads(response.content)[
            'message'] == 'Batch ingestion done':
            print("{}  ; 设备'{}'：{}--{}数据发送成功".format(time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime()),sensorId,recordTime - 300000,recordTime),sep="\n")
            print("{}当前队列剩余数量：{}".format(time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime()),q.qsize()))
            return True
        else:
            print('发送失败')
            return False
    except Exception as e:
        print("数据发送出错")
        sendDataToVCloud(payload,recordTime,token)
        print(e)
        return False


def send():
    token = get_token()

    with ThreadPoolExecutor(max_workers=200) as t:
        while not q.empty():
                data = q.get()
                ecg = data['ecg']
                recordTime = data['recordTime']
                # if not q.empty():
                #     deviceType = data["ecg"][0]['type']
                # else:
                #     # print("*"*50)
                #     pass

                obj_list = []
                begin = time.time()
                for e in ecg:
                    e=[e]
                    obj = t.submit(sendDataToVCloud,e, recordTime,token)
                    # print(e)
                    obj_list.append(obj)
                for future in as_completed(obj_list):
                    data = future.result()
                    # print(data)
                    # print('*' * 50)

                times = time.time() - begin
                # print("{}发送消耗时间:{}".format(deviceType,times))




def assembly_data(count, deviceId, record, stampEndTime, stampStartTime):
    global recordTime
    # ecg_list = getEcgData()
    # 心电设备组装数据
    if "ECG" in deviceId:
        while True:
            # 组装15秒数据
            if record + 300000 < stampEndTime:
                ecgDataList = []
                for i in range(count * 300, count * 300 + 300):
                    recordTime = stampStartTime + (i * 1000)
                    ecgData = assemble_ECG_data(deviceId, recordTime)
                    # ecgData = assemble_data_ECG(deviceId, recordTime, ecg_list[i], 65, 28)
                    record = recordTime
                    ecgDataList.append(ecgData)
                    # if len(ecgDataList)==9:
                    #     break
                result = {"ecg": ecgDataList, "recordTime": recordTime}
                # print(result)
                q.put(result)
                count += 1
                # record += 6000
            else:
                ecgDataList = []
                t = int((stampEndTime - record) // 1000)
                for i in range(count * 60, count * 60 + t):
                    recordTime = stampStartTime + (i * 1000)
                    ecgData = assemble_ECG_data(deviceId, recordTime)
                    record = recordTime
                    ecgDataList.append(ecgData)
                result = {"ecg": ecgDataList, "recordTime": recordTime}
                q.put(result)
                print(f"设备:{deviceId} 数据组装完毕",sep="\n")
                # send()
                return
    # 血氧设备组装数据
    elif "O2" in deviceId:
        while True:
            # 组装15秒数据
            if record + 60000 < stampEndTime:
                spo2DataList = []
                for i in range(count * 30, count * 30 + 30):
                    # print(ecg_list[i])
                    recordTime = stampStartTime + (i * 4000)
                    # time_array = time.localtime(recordTime // 1000)
                    # format_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
                    # print(format_time)
                    spo2Data = assemble_SpO2_data(deviceId, recordTime)
                    record = recordTime
                    spo2DataList.append(spo2Data)
                result = {"ecg": spo2DataList, "recordTime": recordTime}
                # print(result)
                # q.put(result)
                count += 1
            else:
                spo2DataList = []
                t = int((stampEndTime - record) // 1000 // 4)
                for i in range(count * 30, count * 30 + t + 1):
                    recordTime = stampStartTime + (i * 4000)
                    # time_array = time.localtime(record // 1000)
                    # format_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
                    # print(format_time)
                    spo2Data = assemble_SpO2_data(deviceId, recordTime)
                    record = recordTime
                    spo2DataList.append(spo2Data)
                result = {"ecg": spo2DataList, "recordTime": recordTime}
                # q.put(result)
                # print(f"设备:{deviceId} 数据组装完毕",sep="\n")
                # send()

                return
    # BP设备组织数据
    elif "BP" in deviceId:
        while True:
            # 组装15秒数据
            if record + 9000000 < stampEndTime:
                bpDataList = []
                for i in range(count * 15, count * 15 + 15):
                    # print(ecg_list[i])
                    recordTime = stampStartTime + (i * 600000)
                    bpData = assemble_BP_data(deviceId, recordTime)
                    record = recordTime
                    bpDataList.append(bpData)
                result = {"ecg": bpDataList, "recordTime": recordTime}
                # print(result)
                q.put(result)
                count += 1
            else:
                bpDataList = []

                t = int((stampEndTime - record) // 600000)
                for i in range(count * 15, count * 15 + t):
                    recordTime = stampStartTime + (i * 600000)
                    bpData = assemble_BP_data(deviceId, recordTime)
                    record = recordTime
                    bpDataList.append(bpData)
                result = {"ecg": bpDataList, "recordTime": recordTime}
                # q.put(result)
                # print(f"设备：{deviceId} 数据组装完毕",sep="\n")
                # send()

                return


def main(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,startTime,endTime):
    total_list = create_total_patch(ecg_count, ECGPatch, spo2_count, SpO2Patch, bp_count, BPPatch)
    begin = time.time()
    with ThreadPoolExecutor(max_workers=10) as t:
        obj_list = []
        for device in total_list:
            count = 0
            record = 0
            obj = t.submit(assembly_data,count, device, record, endTime, startTime)
            obj_list.append(obj)
        # for future in as_completed(obj_list):
        #     data = future.result()
        #     print(data)
        #     print('*' * 50)

    times = time.time() - begin
    print("*"*100)
    print("数据组装消耗时间:{}".format(times))

    begin = time.time()
    print(f"所有数据组装完毕，开始发送数据", sep="\n")
    # 数据全部组装完成调用发送数据
    send()
    times = time.time() - begin
    print("*"*100)
    print("数据发送消耗时间:{}".format(times))
    print("*"*50)

if __name__ == '__main__':

    ecg_count = 200
    # ECG设备号
    ECGPatch = "ECGRec_100000/C100000"
    # 要绑定的SpO2数量
    spo2_count =0
    # SpO2  设备号
    SpO2Patch = "O2 1000000000"
    # 要绑定的BP数量
    bp_count = 0
    # BP  设备号
    BPPatch = "BP5S_1000000000000"
    # 2023-12-29 14:00:00
    startTime = 1703829600000
    # 2023-12-29 16:00:00
    endTime = 1703836800000
    main(ecg_count,ECGPatch,spo2_count,SpO2Patch,bp_count,BPPatch,startTime,endTime)