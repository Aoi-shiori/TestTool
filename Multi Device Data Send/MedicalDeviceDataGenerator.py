import datetime
import time
import random
import logging
import re
import json
from typing import List, Union, Dict, Optional


class MedicalDeviceDataGenerator:
    # 设备类型映射
    DEVICE_TYPES = {
        100: "ECG",
        101: "SpO2",
        102: "BP",
        103: "Temperature",
        104: "CGM"
    }

    # 默认设备名称映射
    DEFAULT_DEVICE_NAMES = {
        "ECG": "ECGRec_202509/J050001",
        "SpO2": "O2 25JD090501",
        "BP": "BP5C_J2025090501",
        "Temperature": "Temp_J2025090501",
        "CGM": "CGL_J2025090501"
    }

    # 设备发送频率（秒）
    DEVICE_FREQUENCIES = {
        "ECG": 1,  # 每秒1条
        "BP": 300,  # 每300秒（5分钟）1条
        "SpO2": 4,  # 每4秒1条
        "Temperature": 12,  # 每12秒1条
        "CGM": 300  # 每300秒（5分钟）1条（假设值）
    }

    def __init__(self, project_id: str, subject_id: str, log_output: bool = False, is_flash: int = 0):
        """
        初始化医疗设备数据生成器

        Args:
            project_id: 项目ID
            subject_id: 受试者ID
            log_output: 是否输出日志
            is_flash: 是否历史数据
        """
        self.ProjectId = project_id
        self.SubjectId = subject_id
        self.log_output = log_output
        self.is_flash = is_flash


        # 初始化正常/异常数据范围
        self.normal_HR_list = [i for i in range(51, 91)]
        self.normal_RR_list = [i for i in range(19, 20)]
        self.abnormal_HR_list = [i for i in range(111, 131)]
        self.abnormal_RR_list = [i for i in range(25, 35)]
        self.normal_temp_list = [round(random.uniform(36.1, 37.2), 1) for _ in range(10)]
        self.abnormal_temp_list = [round(random.uniform(38.1, 39.2), 1) for _ in range(3)]
        self.normal_spo2_list = [i for i in range(95, 100)]
        self.abnormal_spo2_list = [i for i in range(90, 95)]
        self.normal_bp_dia_list = [i for i in range(80, 90)]
        self.normal_bp_sys = [i for i in range(130, 140)]

        # 配置日志
        if log_output:
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def detect_device_type(self, device_name: str) -> str:
        """
        根据设备名称判断设备类型

        Args:
            device_name: 设备名称

        Returns:
            设备类型字符串
        """
        # ECG设备：以"ECGRec_"开头
        if device_name.startswith("ECGRec_"):
            return "ECG"

        # SpO2设备：以"O2 "开头（注意有空格）
        if device_name.startswith("O2 "):
            return "SpO2"

        # BP设备：以"BP"开头
        if device_name.startswith("BP"):
            return "BP"

        # CGM设备：以"CGL_"开头
        if device_name.startswith("CGL_"):
            return "CGM"

        # 体温设备：以"Temp_"开头 或 字母+两位数字+.开头（如F33.）
        if device_name.startswith("Temp_") or re.match(r'^[A-Za-z]\d{2}\.', device_name):
            return "Temperature"

        # 如果无法识别，返回None
        return None

    def generate_data(self, start_time: Union[int, str], end_time: Union[int, str] = None,
                      device_types: List[Union[int, str]] = None, timezone_offset: Optional[int] = 28800,
                      timezone_name: Optional[str] = "Asia/Shanghai") -> List[Dict]:
        """
        生成指定时间范围和设备类型的数据

        Args:
            start_time: 开始时间（Unix时间戳毫秒或字符串格式时间）
            end_time: 结束时间（Unix时间戳毫秒或字符串格式时间），如果为None则与start_time相同
            device_types: 设备类型列表（可以是类型代码、类型名称或设备名称），如果为None则生成所有设备数据
            timezone_offset: 时区偏移量（秒），None表示移除该字段
            timezone_name: 时区名称，None表示移除该字段

        Returns:
            生成的数据列表
        """
        # 处理默认参数
        if end_time is None:
            end_time = start_time
        if device_types is None:
            device_types = list(self.DEVICE_TYPES.keys())  # 默认生成所有设备数据

        # 转换时间为Unix时间戳（毫秒）
        start_ts_ms = self._parse_time(start_time)
        end_ts_ms = self._parse_time(end_time)

        if start_ts_ms > end_ts_ms:
            raise ValueError("开始时间必须早于或等于结束时间")

        # 标准化设备类型和设备名称
        device_info_list = []  # 存储(设备类型, 设备名称)元组
        for device in device_types:
            device_type, device_name = self._normalize_device(device)
            if device_type:
                device_info_list.append((device_type, device_name))
            else:
                raise ValueError(f"无法识别的设备: {device}")

        # 生成数据
        all_data = []
        device_type_data={}
        for device_type, device_name in device_info_list:
            device_data = self._generate_device_data(device_type, device_name, start_ts_ms, end_ts_ms, timezone_offset,
                                                     timezone_name)

            device_type_data={f"{device_type}":device_data,"count":len(device_data)}
            all_data.append(device_type_data)
            # all_data.extend(device_data)

            if self.log_output:
                logging.info(f"生成 {device_type} 数据 {len(device_data)} 条 (设备: {device_name})")

            # 按时间排序
            device_type_data[f"{device_type}"].sort(key=lambda x: x.get('recordTime', 0))

        length = 0
        if self.log_output:
            for line in all_data:
                length +=  line["count"]

            logging.info(f"总共生成 {length} 条数据")

        return all_data

    def _normalize_device(self, device: Union[int, str]) -> tuple:
        """
        标准化设备参数，返回(设备类型, 设备名称)元组

        Args:
            device: 设备参数（类型代码、类型名称或设备名称）

        Returns:
            (设备类型, 设备名称)元组
        """
        # 如果是整数类型代码
        if isinstance(device, int) and device in self.DEVICE_TYPES:
            device_type = self.DEVICE_TYPES[device]
            device_name = self.DEFAULT_DEVICE_NAMES[device_type]
            return (device_type, device_name)

        # 如果是字符串类型名称
        if isinstance(device, str) and device in self.DEVICE_TYPES.values():
            device_type = device
            device_name = self.DEFAULT_DEVICE_NAMES[device_type]
            return (device_type, device_name)

        # 如果是设备名称字符串
        if isinstance(device, str):
            device_type = self.detect_device_type(device)
            if device_type:
                return (device_type, device)

        # 无法识别
        return (None, None)

    def _parse_time(self, time_value: Union[int, str]) -> int:
        """解析时间参数为Unix时间戳（毫秒）"""
        if isinstance(time_value, int):
            # 默认认为是毫秒时间戳
            return time_value
        elif isinstance(time_value, str):
            # 尝试解析字符串时间
            try:
                # 尝试多种时间格式
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                    try:
                        dt = datetime.datetime.strptime(time_value, fmt)
                        return int(dt.timestamp() * 1000)
                    except ValueError:
                        continue
                raise ValueError(f"无法解析时间字符串: {time_value}")
            except Exception as e:
                raise ValueError(f"时间字符串格式错误: {e}")
        else:
            raise ValueError("时间参数应为整数时间戳或字符串格式时间")

    def _generate_device_data(self, device_type: str, device_name: str, start_ts_ms: int, end_ts_ms: int,
                              timezone_offset: Optional[int], timezone_name: Optional[str]) -> List[Dict]:
        """生成特定设备类型的数据"""
        # 如果开始和结束时间相同，只生成一条数据
        if start_ts_ms == end_ts_ms:
            return [
                self._generate_single_data_point(device_type, device_name, start_ts_ms, timezone_offset, timezone_name)]

        # 否则按频率生成多条数据
        frequency_sec = self.DEVICE_FREQUENCIES[device_type]
        frequency_ms = frequency_sec * 1000  # 转换为毫秒
        data = []

        current_ts_ms = start_ts_ms
        while current_ts_ms <= end_ts_ms:
            data.append(self._generate_single_data_point(device_type, device_name, current_ts_ms, timezone_offset,
                                                         timezone_name))
            current_ts_ms += frequency_ms

        return data

    def _generate_single_data_point(self, device_type: str, device_name: str, record_time_ms: int,
                                    timezone_offset: Optional[int], timezone_name: Optional[str]) -> Dict:
        """生成单个时间点的设备数据"""
        if device_type == "ECG":
            # 随机选择正常或异常数据
            use_abnormal = random.random() < 0.1  # 10%的概率使用异常数据
            hr = random.choice(self.abnormal_HR_list if use_abnormal else self.normal_HR_list)
            rr = random.choice(self.abnormal_RR_list if use_abnormal else self.normal_RR_list)
            temp = random.choice(self.abnormal_temp_list if use_abnormal else self.normal_temp_list)

            # 生成ECG波形数据（简化处理）
            ecg_wave = [random.randint(-100, 100) for _ in range(100)]

            return self.assemble_ECG_data(record_time_ms, ecg_wave, hr, rr, temp, timezone_offset, timezone_name,
                                          device_name)

        elif device_type == "SpO2":
            use_abnormal = random.random() < 0.1  # 10%的概率使用异常数据
            spo2 = random.choice(self.abnormal_spo2_list if use_abnormal else self.normal_spo2_list)
            return self.assemble_SpO2_data(record_time_ms, spo2, device_name, timezone_offset, timezone_name)

        elif device_type == "BP":
            dia = random.choice(self.normal_bp_dia_list)
            sys = random.choice(self.normal_bp_sys)
            return self.assemble_BP_data(record_time_ms, dia, sys, device_name, timezone_offset, timezone_name)

        elif device_type == "Temperature":
            use_abnormal = random.random() < 0.1  # 10%的概率使用异常数据
            temp = random.choice(self.abnormal_temp_list if use_abnormal else self.normal_temp_list)
            return self.assemble_Temp_data(record_time_ms, temp, device_name, timezone_offset, timezone_name)

        elif device_type == "CGM":
            # CGM数据生成暂未实现
            return {}

        return {}

    def assemble_SpO2_data(self, recordTime: int, spo2: int, device_name: str  ,
                          TimeZoneOffset: Optional[int], TimeZoneName: Optional[str]) -> Dict:
        """组装SpO2数据"""
        data = {
            "longtitude": 120.22000122070312,
            "language": "zh-Hans-CN",
            "receiveTime": 0,
            "deviceType": "iPhone",
            "patchMessage": "{\"fwVersion\":\"2.2.0.0041\",\"cpuStatus\":0,\"batteryStatus\":\"NOT_INCHARGEING\",\"timeStamp\":1648020881,\"magnification\":1000,\"accSamplingEnable\":1,\"hwVersion\":\"08\",\"ackStatus\":0,\"flashNum\":326,\"leadOffAccEnable\":0}",
            "deviceToken": "6f6b8a0683b1c2735cd5e5c70c9fef531b63682d",
            "category": "not know",
            "latitude": 30.180000305175781,
            "deviceIp": "10.10.1.174",
            "name": "",
            "app_id": "com.vivalnk.vitalsmonitor",
            "type": "SpO2Raw",
            "customData": {
                "subjectId": self.SubjectId,
                "projectId": self.ProjectId,
                "appVersion": "3.3.0.319"
            },
            "deviceOsType": "iOS",
            "sensorId": device_name,
            "deviceBattery": 88,
            "networkType": "WiFi",
            "sdkVersion": "2.2.4_beta1",
            "timezone": "28800",
            "carrier": "中国电信",
            "deviceOsVersion": "14.7.1",
            "recordTime": recordTime,
            "collectTime": recordTime,
            "profileId": "TestTeam",
            "data": {
                "flash": self.is_flash,
                "spo2": spo2,
                "pr": 77,
                "pi": "1.5",
                "battery": 71,
                "deviceType": "Checkme_O2",
                "deviceSN": device_name,
                "deviceName": device_name,
                "chargerStatus": 0,
                "waveform": [],
                "recordTime": recordTime,
                "steps": -1
            }
        }
        # 处理时区字段
        if TimeZoneOffset is not None:
            data["timezone"] = f"{TimeZoneOffset}"
        if TimeZoneName is not None:
            data["timezoneName"] = TimeZoneName

        return data

    def assemble_ECG_data(self, recordTime: int, ECG: List[int], HR: int, RR: int, TEMP: float,
                          TimeZoneOffset: Optional[int], TimeZoneName: Optional[str], device_name: str) -> Dict:
        """组装ECG数据"""
        collect = int(time.time() * 1000)
        data = {
            "app_id": "com.vivalnk.mvm",
            "longtitude": 120.22000122070312,
            "language": "zh-Hans-CN",
            "receiveTime": 0,
            "deviceType": "iPhone",
            "patchMessage": "{\"fwVersion\":\"2.2.0.0041\",\"cpuStatus\":0,\"batteryStatus\":\"NOT_INCHARGEING\",\"timeStamp\":1648020731,\"magnification\":1000,\"accSamplingEnable\":1,\"hwVersion\":\"08\",\"ackStatus\":0,\"flashNum\":860,\"leadOffAccEnable\":0}",
            "deviceToken": "6f6b8a0683b1c2735cd5e5c70c9fef531b63682d",
            "category": "not know",
            "latitude": 30.180000305175781,
            "deviceIp": "10.10.1.174",
            "name": "EcgRaw",
            "type": "EcgRaw",
            "customData": {
                "subjectId": self.SubjectId,
                "projectId": self.ProjectId
            },
            "deviceOsType": "iOS",
            "sensorId": device_name,
            "deviceBattery": 90,
            "networkType": "WiFi",
            "sdkVersion": "2.2.4_beta1",
            "carrier": "中国电信",
            "deviceOsVersion": "14.7.1",
            "recordTime": recordTime,
            "collectTime": collect,
            "profileId": "TestTeam",
            "data": {
                "deviceId": "CA:B2:78:25:17:88",
                "receiveTime": collect,
                "temperature": TEMP,
                "rawTemp": TEMP,
                "accAccuracy": 2048,
                "sf": "128",
                "deviceName": device_name,
                "dataMode": "fullDual",
                "rr": RR,
                "activity": random.choice([0, 1]),
                "rwl": [27, 71, 106, -1, -1],
                "magnification": 1000,
                "avRR": 25,
                "rmssd": random.choice([55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65]),
                "battery": 15,
                "acc": [
                    {"x": -15, "y": 67, "z": -2022},
                    {"x": -8, "y": 60, "z": -2030},
                    {"x": -18, "y": 63, "z": -2028},
                    {"x": -15, "y": 65, "z": -2036},
                    {"x": -16, "y": 64, "z": -2027}
                ],
                "deviceSN": device_name,
                "leadOn": 1,
                "rri": [242, 257, 242, 242, 0],
                "ecg": ECG,
                "recordTime": recordTime,
                "flash": self.is_flash,
                "modeType": "mode4",
                "hr": HR
            }
        }

        # 处理时区字段
        if TimeZoneOffset is not None:
            data["timezone"] = f"{TimeZoneOffset}"
        if TimeZoneName is not None:
            data["timezoneName"] = TimeZoneName
        return data

    def assemble_Temp_data(self, recordTime: int, temp: float, device_name: str,
                          TimeZoneOffset: Optional[int], TimeZoneName: Optional[str])-> Dict:
        """组装体温数据"""
        data = {
            "app_id": "com.vivalnk.mvm",
            "longtitude": 0,
            "language": "en-CN",
            "receiveTime": 0,
            "deviceType": "iPhone",
            "patchMessage": "{\"firmware\":\"N/A\",\"chargerFw\":\"N\\\\/A\",\"chargeBatteryStatus\":\"N/A\"}",
            "deviceToken": "80447b01d0a5b38e1e864ca746cfadcc616464fb",
            "category": "notknow",
            "latitude": 0,
            "deviceIp": "10.10.1.117",
            "name": "TemperatureRaw",
            "type": "TemperatureRaw",
            "customData": {
                "appVersion": "3.0.0.9",
                "subjectId": self.SubjectId,
                "projectId": self.ProjectId
            },
            "deviceOsType": "iOS",
            "sensorId": device_name,
            "deviceBattery": 70,
            "networkType": "WiFi",
            "sdkVersion": "3.0.1_beta3",
            "timezone": "28800",
            "carrier": "中国电信",
            "deviceOsVersion": "15.7",
            "recordTime": recordTime,
            "collectTime": recordTime,
            "profileId": "TestTeam",
            "data": {
                "deviceSN": device_name,
                "fw": "N/A",
                "rssi": -35,
                "deviceName": device_name,
                "displayTemp": f"{temp:.1f}",
                "deviceType": "VV200",
                "flash": self.is_flash,
                "recordTime": recordTime,
                "battery": 55,
                "deviceId": "",
                "rawTemp": f"{temp:.1f}"
            }
        }
        # 处理时区字段
        if TimeZoneOffset is not None:
            data["timezone"] = f"{TimeZoneOffset}"
        if TimeZoneName is not None:
            data["timezoneName"] = TimeZoneName
        return data

    def assemble_BP_data(self, recordTime: int, bp_dia: int, bp_sys: int, device_name: str,
                          TimeZoneOffset: Optional[int], TimeZoneName: Optional[str]) -> Dict:
        """组装血压数据"""
        data = {
            "app_id": "com.vivalnk.mvm",
            "longtitude": 120.22000122070312,
            "language": "zh-Hans-CN",
            "receiveTime": 0,
            "deviceType": "iPhone",
            "patchMessage": "",
            "deviceToken": "6f6b8a0683b1c2735cd5e5c70c9fef531b63682d",
            "category": "not know",
            "latitude": 30.180000305175781,
            "deviceIp": "10.10.1.174",
            "name": "BPRaw",
            "type": "BPRaw",
            "customData": {
                "subjectId": self.SubjectId,
                "projectId": self.ProjectId
            },
            "deviceOsType": "iOS",
            "sensorId": device_name,
            "deviceBattery": 100,
            "networkType": "WiFi",
            "sdkVersion": "2.2.4_beta1",
            "timezone": "28800",
            "carrier": "中国电信",
            "deviceOsVersion": "14.7.1",
            "recordTime": recordTime,
            "collectTime": recordTime,
            "profileId": "TestTeam",
            "data": {
                "deviceId": "00:4D:32:0F:4A:9E",
                "dia": bp_dia,
                "deviceName": device_name,
                "deviceType": "BP5C",
                "heartRate": 68,
                "flash": self.is_flash,
                "recordTime": recordTime,
                "hsdValue": 0,
                "battery": 100,
                "sys": bp_sys,
                "arrhythmia": 0
            }
        }
            # 处理时区字段
        if TimeZoneOffset is not None:
            data["timezone"] = f"{TimeZoneOffset}"
        if TimeZoneName is not None:
            data["timezoneName"] = TimeZoneName
        return data




# 使用示例
if __name__ == '__main__':
    # 创建数据生成器实例
    generator = MedicalDeviceDataGenerator(
        project_id="UAT340_V2",
        subject_id="J20250910001",
        log_output=True,  # 启用日志输出
        is_flash = 1,     # 是否传历史数据 -> 1：历史，0：实时
    )

    # # 测试设备类型检测功能
    # test_devices = [
    #     "ECGRec_202509/J050001",  # ECG设备
    #     "O2 25JD090501",  # SpO2设备
    #     "BP5C_J2025090501",  # BP设备
    #     "Temp_J2025090501",  # 体温设备 (Temp_开头)
    #     "F22.12345",  # 体温设备 (字母+数字+.开头)
    #     "CGL_J2025090501"  # CGM设备
    # ]
    #
    # for device in test_devices:
    #     device_type = generator.detect_device_type(device)
    #     print(f"设备 '{device}' 的类型是: {device_type}")

    # 示例1：使用设备名称生成数据
    start_time = int(time.time() * 1000)  # 当前时间毫秒时间戳
    device_names = [
        "ECGRec_JUN/E310004",  # ECG设备
        "O2 C208S_J87C6F900004",  # SpO2设备 (注意: 这里使用了空格)
        "BP5S_00J00000004",  # BP设备
        "Temp_AOJ-20F_AJUN00000003"  # 体温设备
    ]

    data = generator.generate_data(
        start_time=start_time,
        end_time=start_time+300*1000,
        device_types=device_names, # 直接使用设备名称

        timezone_name = "Asia/Hong_Kong",
        timezone_offset = 18800
    )

    print(f"使用设备名称生成数据: {len(data)} 条")
    data=json.dumps(data)
    print(f"{data}")

    #
    # # 示例2：混合使用设备类型和设备名称
    # mixed_devices = [
    #     100,  # ECG设备类型代码
    #     "SpO2",  # SpO2设备类型名称
    #     "BP5S_00J00000004",  # BP设备名称
    #     "F33.12345"  # 体温设备名称
    # ]
    #
    # data2 = generator.generate_data(
    #     start_time=start_time,
    #     device_types=mixed_devices
    # )
    #
    # print(f"混合使用设备类型和设备名称生成数据: {len(data2)} 条")
    #
    # # 打印第一条数据作为示例
    # if data:
    #     print(f"第一条数据: {data[0]}")