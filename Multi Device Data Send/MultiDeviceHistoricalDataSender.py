# -*- coding: utf-8 -*-
"""
# @Creation time: 2025/11/02 12:24
# @Author       : 郭军
# @Email        : 391350540@qq.com
# @FileName     : MultiDeviceHistoricalDataSender.py
# @Software     : PyCharm
# @Project      : TestTool
# @PythonVersion: python 3.14
# @Version      : 3.0
# @Description  : 发送多设备历史数据到云端
# @Update Time  :
# @UpdateContent:
v3.0-20251102: 代码结构优化，功能整合，异步发送增强，支持多设备上传
"""

import datetime
import time
import random
import threading
# import logging
import re
import json
from typing import Dict, List, Union, Optional, Any
import requests
from queue import Queue
from logger import logger
import asyncio
from datetime import datetime, timedelta, UTC, timezone
import pytz
from dataclasses import dataclass
from getEcgData import *  # 保持原有导入
import aiohttp

@dataclass
class EnvParameterinfo:
    """环境参数配置类"""
    name: str
    env_type: str
    url: str
    id: str
    value: str
    description: str
    example: Any = None
    enum: List[Any] = None
    default: Any = None


class TimestampDataRouter:
    """时间戳数据路由器 - 根据时间戳获取对应的数据配置"""

    def __init__(self, config: Dict[str, Any] = None):
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

        # 如果没有匹配的范围，返回默认数据
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


class AccStep:
    """加速度步数计算器"""

    def __init__(self, recordTime: int, timezone_offset: int):
        self.recordTime = recordTime
        self.timezone_offset = timezone_offset
        self.steps = 0

    def __str__(self):
        return f"recordTime: {self.recordTime}, steps: {self.steps}"

    def get_time_str(self, timshift: int, timezone_offset: int) -> tuple:
        """获取时区调整后的时间字符串"""
        timshift_10 = timshift / 1000
        utc_time = datetime.fromtimestamp(timshift_10, tz=UTC)

        # 根据时区偏移量调整时间
        timezone_time = utc_time + timedelta(seconds=timezone_offset)

        time_str_hour = timezone_time.strftime("%H")
        time_str_min = timezone_time.strftime("%M")
        time_str_sec = timezone_time.strftime("%S")

        return time_str_hour, time_str_min, time_str_sec

    def get_acc_step_total(self) -> int:
        """计算总步数"""
        hour, min, sec = self.get_time_str(self.recordTime, self.timezone_offset)
        steps = int(hour) * 3600 + int(min) * 60 + int(sec) + 1
        self.steps = steps
        return steps


class ModifyTime:
    """时间修改器 - 用于调整时间范围"""

    def __init__(self, str_time: str, days: int = 0, hours: int = 0, minutes: int = 0, seconds: int = 0):
        self.str_time = str_time
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds

    def date_plus(self) -> str:
        """时间加法"""
        dt = datetime.strptime(self.str_time, '%Y-%m-%d %H:%M:%S')
        dt_plus = dt + timedelta(days=self.days, hours=self.hours, minutes=self.minutes, seconds=self.seconds)
        return dt_plus.strftime('%Y-%m-%d %H:%M:%S')

    def date_minus(self) -> str:
        """时间减法"""
        dt = datetime.strptime(self.str_time, '%Y-%m-%d %H:%M:%S')
        dt_minus = dt - timedelta(days=self.days, hours=self.hours, minutes=self.minutes, seconds=self.seconds)
        return dt_minus.strftime('%Y-%m-%d %H:%M:%S')


class MedicalDeviceDataGenerator:
    """医疗设备数据生成器 - 整合了TimestampDataRouter和AccStep功能"""

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
        "CGM": 300  # 每300秒（5分钟）1条
    }

    def __init__(self, project_id: str, subject_id: str, log_output: bool = False,
                 is_flash: int = 0, data_config: Dict[str, Any] = None):
        """
        初始化医疗设备数据生成器

        Args:
            project_id: 项目ID
            subject_id: 受试者ID
            log_output: 是否输出日志
            is_flash: 是否历史数据
            data_config: 数据配置
        """
        self.ProjectId = project_id
        self.SubjectId = subject_id
        self.log_output = log_output
        self.is_flash = is_flash

        # 初始化数据路由器
        self.data_router = TimestampDataRouter(data_config)

        # 初始化正常/异常数据范围
        self._init_data_ranges()

        # # 配置日志
        # if log_output:
        #     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def _init_data_ranges(self):
        """初始化数据范围"""
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

    def detect_device_type(self, device_name: str) -> Optional[str]:
        """根据设备名称判断设备类型"""
        device_patterns = {
            "ECG": lambda x: x.startswith("ECGRec_"),
            "SpO2": lambda x: x.startswith("O2 "),
            "BP": lambda x: x.startswith("BP"),
            "CGM": lambda x: x.startswith("CGL_"),
            "Temperature": lambda x: x.startswith("Temp_") or re.match(r'^[A-Za-z]\d{2}\.', x)
        }

        for device_type, pattern_func in device_patterns.items():
            if pattern_func(device_name):
                return device_type
        return None

    def generate_data(self, start_time: Union[int, str], end_time: Union[int, str] = None,
                      device_types: List[Union[int, str]] = None, timezone_offset: Optional[int] = 28800,
                      timezone_name: Optional[str] = "Asia/Shanghai") -> List[Dict]:
        """生成指定时间范围和设备类型的数据"""
        # 处理默认参数
        if end_time is None:
            end_time = start_time

        if device_types is None:
            device_types = list(self.DEVICE_TYPES.keys())

        # 转换时间
        start_ts_ms = self._parse_time(start_time)
        end_ts_ms = self._parse_time(end_time)

        if start_ts_ms > end_ts_ms:
            raise ValueError("开始时间必须早于或等于结束时间")

        # 标准化设备类型
        device_info_list = self._normalize_device_list(device_types)

        # 生成数据
        all_data = []
        total_count = 0

        for device_type, device_name in device_info_list:
            device_data = self._generate_device_data(
                device_type, device_name, start_ts_ms, end_ts_ms,
                timezone_offset, timezone_name
            )

            device_summary = {
                device_type: device_data,
                "count": len(device_data),
                "device_name": device_name
            }
            all_data.append(device_summary)
            total_count += len(device_data)

            if self.log_output:
                logger.info(f"生成 {device_type} 数据 {len(device_data)} 条 (设备: {device_name})")

        if self.log_output:
            logger.info(f"总共生成 {total_count} 条数据")

        return all_data

    def _normalize_device_list(self, device_types: List[Union[int, str]]) -> List[tuple]:
        """标准化设备列表"""
        device_info_list = []
        for device in device_types:
            device_type, device_name = self._normalize_device(device)
            if device_type:
                device_info_list.append((device_type, device_name))
            else:
                raise ValueError(f"无法识别的设备: {device}")
        return device_info_list

    def _normalize_device(self, device: Union[int, str]) -> tuple:
        """标准化单个设备参数"""
        # 整数类型代码
        if isinstance(device, int) and device in self.DEVICE_TYPES:
            device_type = self.DEVICE_TYPES[device]
            device_name = self.DEFAULT_DEVICE_NAMES[device_type]
            return (device_type, device_name)

        # 字符串类型名称
        if isinstance(device, str) and device in self.DEVICE_TYPES.values():
            device_type = device
            device_name = self.DEFAULT_DEVICE_NAMES[device_type]
            return (device_type, device_name)

        # 设备名称字符串
        if isinstance(device, str):
            device_type = self.detect_device_type(device)
            if device_type:
                return (device_type, device)

        return (None, None)

    def _parse_time(self, time_value: Union[int, str]) -> int:
        """解析时间参数为Unix时间戳（毫秒）"""
        if isinstance(time_value, int):
            return time_value
        elif isinstance(time_value, str):
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(time_value, fmt)
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    continue
            raise ValueError(f"无法解析时间字符串: {time_value}")
        else:
            raise ValueError("时间参数应为整数时间戳或字符串格式时间")

    def _generate_device_data(self, device_type: str, device_name: str, start_ts_ms: int,
                              end_ts_ms: int, timezone_offset: Optional[int],
                              timezone_name: Optional[str]) -> List[Dict]:
        """生成特定设备类型的数据"""
        if start_ts_ms == end_ts_ms:
            return [self._generate_single_data_point(
                device_type, device_name, start_ts_ms, timezone_offset, timezone_name
            )]

        frequency_ms = self.DEVICE_FREQUENCIES[device_type] * 1000
        data = []
        current_ts_ms = start_ts_ms

        while current_ts_ms <= end_ts_ms:
            data.append(self._generate_single_data_point(
                device_type, device_name, current_ts_ms, timezone_offset, timezone_name
            ))
            current_ts_ms += frequency_ms

        return data

    def _generate_single_data_point(self, device_type: str, device_name: str,
                                    record_time_ms: int, timezone_offset: Optional[int],
                                    timezone_name: Optional[str]) -> Dict:
        """生成单个时间点的设备数据"""
        # 使用数据路由器获取配置数据
        # router_data = self.data_router.get_data_for_timestamp(record_time_ms / 1000)
        router_data = self.data_router.get_data_for_timestamp(record_time_ms)

        if device_type == "ECG":
            return self._generate_ecg_data(record_time_ms, device_name, router_data,
                                           timezone_offset, timezone_name)
        elif device_type == "SpO2":
            return self._generate_spo2_data(record_time_ms, device_name, router_data,
                                            timezone_offset, timezone_name)
        elif device_type == "BP":
            return self._generate_bp_data(record_time_ms, device_name, router_data,
                                          timezone_offset, timezone_name)
        elif device_type == "Temperature":
            return self._generate_temp_data(record_time_ms, device_name, router_data,
                                            timezone_offset, timezone_name)
        elif device_type == "CGM":
            return self._generate_cgm_data(record_time_ms, device_name, router_data,
                                           timezone_offset, timezone_name)
        else:
            return {}

    def _generate_ecg_data(self, record_time_ms: int, device_name: str,
                           router_data: Dict, timezone_offset: Optional[int],
                           timezone_name: Optional[str]) -> Dict:
        """生成ECG数据"""
        use_abnormal = random.random() < 0.1
        hr_data = router_data["data"].get("HR")
        rr_data = router_data["data"].get("RR")
        temp_data = router_data["data"].get("Temp")

        hr = hr_data if hr_data else random.choice(self.abnormal_HR_list if use_abnormal else self.normal_HR_list)
        rr = rr_data if rr_data else random.choice(self.abnormal_RR_list if use_abnormal else self.normal_RR_list)
        temp = temp_data if temp_data else random.choice(
            self.abnormal_temp_list if use_abnormal else self.normal_temp_list)


        ecg_wave = [random.randint(-100, 100) for _ in range(100)]

        ecgData = self.assemble_ECG_data(record_time_ms, ecg_wave, hr, rr, temp,
                               timezone_offset, timezone_name, device_name)
        result = router_data
        logger.info(
            # f"数据处理 - 用例[{result.get('Note')}-P{result.get('priority')}] - "
            # f"数据处理 - 用例:[{result.get('Note')}] - "
            f"ECG数据处理 - 用例[{router_data.get('Note')}-第{router_data.get('second')}秒] - "
            f"时间戳[{ecgData['recordTime']}]:{'一致' if router_data.get('timestamp') == ecgData.get('recordTime') else '不一致'} - "
            f"路由→组装: HR({result['data'].get('HR', 'N/A')}→{ecgData.get('data', {}).get('hr', 'N/A')}) | "
            f"RR({result['data'].get('RR', 'N/A')}→{ecgData['data']['rr']}) | "
            f"Temp({result['data'].get('Temp', 'N/A')}→{ecgData['data']['temperature']}) - "
            # f"{'✓数据一致' if result['data'].get('HR') == ecgData.get('data', {}).get('hr') and result['data'].get('RR') == ecgData['data'].get('rr') and result['data'].get('Temp') == ecgData['data'].get('temperature') else '✗数据不一致'} - "
            f"{'✓数据一致' if result['data'].get('HR') == ecgData.get('data', {}).get('hr') and result['data'].get('RR') == ecgData['data'].get('rr') and result['data'].get('Temp') == ecgData['data'].get('temperature') else '✗不一致:' + ''.join([f'HR' if result['data'].get('HR') != ecgData.get('data', {}).get('hr') else '', f',RR' if result['data'].get('RR') != ecgData['data'].get('rr') else '', f',Temp' if result['data'].get('Temp') != ecgData['data'].get('temperature') else '']).lstrip(',')} - "
            f"设备[{ecgData['sensorId']}] 时区:{ecgData['timezone']} 时区名：{ecgData.get('timezoneName', 'N/A')} 路由说明信息：[{result.get('message')}]"
        )
        return ecgData

    def _generate_spo2_data(self, record_time_ms: int, device_name: str,
                            router_data: Dict, timezone_offset: Optional[int],
                            timezone_name: Optional[str]) -> Dict:
        """生成SpO2数据"""
        use_abnormal = random.random() < 0.1
        spo2 = random.choice(self.abnormal_spo2_list if use_abnormal else self.normal_spo2_list)

        # 使用AccStep计算步数
        acc_step = AccStep(record_time_ms, timezone_offset or 28800)
        steps = acc_step.get_acc_step_total()

        data = self.assemble_SpO2_data(record_time_ms, spo2, device_name,
                                       timezone_offset, timezone_name)
        data["data"]["steps"] = steps  # 更新步数

        return data

    def _generate_bp_data(self, record_time_ms: int, device_name: str,
                          router_data: Dict, timezone_offset: Optional[int],
                          timezone_name: Optional[str]) -> Dict:
        """生成血压数据"""
        dia = random.choice(self.normal_bp_dia_list)
        sys = random.choice(self.normal_bp_sys)
        return self.assemble_BP_data(record_time_ms, dia, sys, device_name,
                                     timezone_offset, timezone_name)

    def _generate_temp_data(self, record_time_ms: int, device_name: str,
                            router_data: Dict, timezone_offset: Optional[int],
                            timezone_name: Optional[str]) -> Dict:
        """生成体温数据"""
        use_abnormal = random.random() < 0.1
        temp_data = router_data["data"].get("Temp")
        temp = temp_data if temp_data else random.choice(
            self.abnormal_temp_list if use_abnormal else self.normal_temp_list)
        return self.assemble_Temp_data(record_time_ms, temp, device_name,
                                       timezone_offset, timezone_name)

    def _generate_cgm_data(self, record_time_ms: int, device_name: str,
                           router_data: Dict, timezone_offset: Optional[int],
                           timezone_name: Optional[str]) -> Dict:
        """生成CGM数据（待实现）"""
        # TODO: 实现CGM数据生成
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




class SendToVcloud:
    """云端数据发送器"""

    def __init__(self, patient_data: List[Dict], env: EnvParameterinfo, group_size: int = 30):
        self.patient_data = patient_data
        self.Env = env
        self.group_size = group_size
        self.data_queue = Queue()

        # 处理数据分组
        self.process_patient_data(patient_data)

    def process_patient_data(self, patient_data: List[Dict]) -> Queue:
        """处理病人数据并分组放入队列"""
        while not self.data_queue.empty():
            self.data_queue.get()

        for device_data in patient_data:
            device_name = device_data.get("device_name", "Unknown")

            for data_type, data_list in device_data.items():
                if data_type in ["device_name", "count"]:
                    continue

                if isinstance(data_list, list):
                    self._group_and_queue_data(device_name, data_type, data_list)

        logger.info(f"数据分组完成，队列大小: {self.data_queue.qsize()}")
        return self.data_queue

    def _group_and_queue_data(self, device_name: str, data_type: str, data_list: List[Any]):
        """将数据分组并放入队列"""
        total_count = len(data_list)

        for i in range(0, total_count, self.group_size):
            group_data = data_list[i:i + self.group_size]
            group_count = len(group_data)

            group_item = {
                "device_name": device_name,
                "data_type": data_type,
                "data": group_data,
                "count": group_count,
                "group_index": i // self.group_size + 1,
                "total_groups": (total_count + self.group_size - 1) // self.group_size
            }

            self.data_queue.put(group_item)

    def get_queue_info(self) -> Dict[str, Any]:
        """获取队列信息"""
        queue_size = self.data_queue.qsize()
        temp_queue = Queue()
        groups_info = []

        while not self.data_queue.empty():
            item = self.data_queue.get()
            groups_info.append({
                "device": item["device_name"],
                "data_type": item["data_type"],
                "group_size": item["count"],
                "group_index": item["group_index"]
            })
            temp_queue.put(item)

        while not temp_queue.empty():
            self.data_queue.put(temp_queue.get())

        return {
            "total_groups": queue_size,
            "group_size_setting": self.group_size,
            "groups_detail": groups_info
        }

    def get_app_token(self) -> str:
        """获取应用令牌"""
        url = f"{self.Env.url}/auth"
        payload = json.dumps({
            "id": self.Env.id,
            "key": self.Env.value
        })
        headers = {'Content-Type': 'application/json'}

        try:
            response = requests.post(url, headers=headers, data=payload, timeout=10)
            response.raise_for_status()
            token = response.json()['data']['token']
            logger.info("成功获取令牌(token)")
            return token
        except requests.RequestException as e:
            logger.error(f"获取令牌(token)失败: {e}")
            raise

    async def send(self):
        """异步发送数据到云端"""
        token = self.get_app_token()
        tasks = []
        total_groups = self.data_queue.qsize()
        sent_groups = 0

        logger.info(f"开始发送数据，总分组数: {total_groups}")

        while not self.data_queue.empty():
            data_group = self.data_queue.get()
            task = asyncio.create_task(
                self.send_data_group(data_group, token)
            )
            tasks.append(task)
            sent_groups += 1

            # 每10组等待一次，避免过多并发
            if len(tasks) >= 10:
                await asyncio.gather(*tasks)
                tasks.clear()
                logger.info(f"发送进度: {sent_groups}/{total_groups}")

        # 等待剩余任务完成
        if tasks:
            await asyncio.gather(*tasks)

        logger.info(f"数据发送完成，总共发送 {sent_groups} 组数据")


    # async def send_data_group(self, data_group: Dict, token: str):
    #     """发送单个数据分组（使用aiohttp）"""
    #     try:
    #         payload = data_group["data"]
    #         record_times = [item.get("recordTime", 0) for item in payload if isinstance(item, dict)]
    #         start_time = min(record_times) if record_times else 0
    #         end_time = max(record_times) if record_times else 0
    #
    #         url = f"{self.Env.url}/v2/tenants/VivaLNK/events?type=dataEvent"
    #         headers = {
    #             'Content-Type': 'application/json',
    #             'Authorization': token
    #         }
    #
    #         # 使用 aiohttp 进行异步HTTP请求
    #         timeout = aiohttp.ClientTimeout(total=30)
    #         async with aiohttp.ClientSession(timeout=timeout) as session:
    #             try:
    #                 async with session.post(url, json=payload, headers=headers) as response:
    #                     response_text = await response.text()
    #
    #                     if response.status == 200:
    #                         response_data = json.loads(response_text)
    #                         if response_data.get('code') == 200 and response_data.get(
    #                                 'message') == 'Batch ingestion done':
    #                             logger.info(f"✓ {data_group['device_name']} {data_group['data_type']} "
    #                                         f"分组 {data_group['group_index']}/{data_group['total_groups']} "
    #                                         f"({start_time}~{end_time}) 发送成功")
    #                             return True
    #                         else:
    #                             logger.error(f"✗ 发送失败 - 响应: {response_data}")
    #                             return False
    #                     else:
    #                         logger.error(f"✗ HTTP错误 - 状态码: {response.status}, 响应: {response_text}")
    #                         return False
    #
    #             except asyncio.TimeoutError:
    #                 logger.error(
    #                     f"✗ 发送超时 - {data_group['device_name']} {data_group['data_type']} 分组 {data_group['group_index']}")
    #                 return False
    #             except Exception as e:
    #                 logger.error(f"✗ 请求异常 - {data_group['device_name']} {data_group['data_type']}: {e}")
    #                 return False
    #
    #     except Exception as e:
    #         logger.error(f"✗ 发送异常 - {data_group['device_name']} {data_group['data_type']}: {e}")
    #         return False


    async def send_data_group(self, data_group: Dict, token: str):
        """发送单个数据分组"""
        try:
            payload = data_group["data"]
            record_times = [item.get("recordTime", 0) for item in payload if isinstance(item, dict)]
            start_time = min(record_times) if record_times else 0
            end_time = max(record_times) if record_times else 0

            # 数据上传URL
            # url = 'https://vcloud-test.vivalnk.com/v2/tenants/VivaLNK/events?type=dataEvent'
            url = f"{self.Env.url}/v2/tenants/VivaLNK/events?type=dataEvent"

            headers = {
                'Content-Type': 'application/json',
                'Authorization': token,
                # 注意：不要添加 Content-Length，aiohttp 会自动计算
            }

            logger.debug(f"发送请求到: {url}")
            logger.debug(f"使用Token: {token[:50]}...")
            logger.debug(f"Payload大小: {len(json.dumps(payload))} 字节")

            # 使用 aiohttp 进行异步HTTP请求
            timeout = aiohttp.ClientTimeout(total=30)

            # 创建自定义连接器，禁用 SSL 验证（如果需要）
            connector = aiohttp.TCPConnector(ssl=False)

            async with aiohttp.ClientSession(
                    timeout=timeout,
                    connector=connector,
                    headers=headers  # 设置默认headers
            ) as session:
                try:
                    # 确保使用与 requests 相同的方式发送数据
                    async with session.post(url, json=payload) as response:
                        response_text = await response.text()

                        logger.debug(f"响应状态: {response.status}")
                        logger.debug(f"响应头: {dict(response.headers)}")
                        logger.debug(f"响应内容: {response_text}")

                        if response.status == 200:
                            try:
                                response_data = json.loads(response_text)
                                if response_data.get('code') == 200 and response_data.get(
                                        'message') == 'Batch ingestion done':
                                    logger.info(f"✓ {data_group['device_name']} {data_group['data_type']} "
                                                f"分组 {data_group['group_index']}/{data_group['total_groups']} "
                                                f"({start_time}~{end_time}) 发送成功")
                                    return True
                                else:
                                    logger.error(f"✗ 发送失败 - 响应: {response_data}")
                                    return False
                            except json.JSONDecodeError:
                                logger.error(f"✗ 响应JSON解析失败: {response_text}")
                                return False
                        else:
                            logger.error(f"✗ HTTP错误 - 状态码: {response.status}, 响应: {response_text}")
                            return False

                except asyncio.TimeoutError:
                    logger.error(
                        f"✗ 发送超时 - {data_group['device_name']} {data_group['data_type']} 分组 {data_group['group_index']}")
                    return False
                except Exception as e:
                    logger.error(f"✗ 请求异常 - {data_group['device_name']} {data_group['data_type']}: {e}")
                    return False

        except Exception as e:
            logger.error(f"✗ 发送异常 - {data_group['device_name']} {data_group['data_type']}: {e}")
            return False

def get_timezone_offset(timezone_name: str, unix_timestamp: float) -> tuple:
    """
    根据时区名和Unix时间戳获取时区偏移量

    Returns:
        tuple: (格式化的时区偏移量, 以秒为单位的偏移量)
    """
    try:
        utc_time = datetime.fromtimestamp(unix_timestamp, timezone.utc)
        tz = pytz.timezone(timezone_name)
        local_time = utc_time.astimezone(tz)
        total_seconds = local_time.utcoffset().total_seconds()

        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        sign = '+' if hours >= 0 else '-'
        offset_str = f"{sign}{abs(hours):02d}:{minutes:02d}"

        return offset_str, int(total_seconds)
    except pytz.UnknownTimeZoneError:
        raise ValueError(f"未知的时区名称: {timezone_name}")
    except Exception as e:
        raise ValueError(f"时区偏移量获取错误: {str(e)}")


async def main(startTime: str, endTime: str, device_names: List[str], timeZone_offset: int,
               timezoneName: str, ProjectId: str, SubjectId: str, data_config: Dict,
               is_get_timezone_offset: bool, env: EnvParameterinfo):
    """主函数 - 生成并发送数据"""
    logger.info(f"{'=' * 60}")
    logger.info("开始执行数据生成和发送任务")
    logger.info(f"{'=' * 60}")

    # 转换时间
    arrayStartTime = time.strptime(startTime, "%Y-%m-%d %H:%M:%S")
    arrayEndTime = time.strptime(endTime, "%Y-%m-%d %H:%M:%S")
    stampStartTime = int(time.mktime(arrayStartTime) * 1000)
    stampEndTime = int(time.mktime(arrayEndTime) * 1000)

    logger.info(f"时间范围: {startTime} -> {endTime}")
    logger.info(f"设备列表: {device_names}")

    # 处理时区偏移量
    if is_get_timezone_offset:
        try:
            _, timeZone_offset = get_timezone_offset(timezoneName, stampStartTime / 1000)
            logger.info(f"自动获取时区偏移量: {timeZone_offset}秒")
        except Exception as e:
            logger.warning(f"自动获取时区偏移量失败: {e}, 使用默认值: {timeZone_offset}")

    # 创建数据生成器
    generator = MedicalDeviceDataGenerator(
        project_id=ProjectId,
        subject_id=SubjectId,
        log_output=True,
        is_flash=1,
        data_config=data_config
    )

    # 生成数据
    logger.info("开始生成设备数据...")
    start_gen_time = time.time()

    data = generator.generate_data(
        start_time=stampStartTime,
        end_time=stampEndTime,
        device_types=device_names,
        timezone_name=timezoneName,
        timezone_offset=timeZone_offset
    )

    gen_time = time.time() - start_gen_time
    logger.info(f"数据生成完成，耗时: {gen_time:.2f}秒")
    # logger.info(f"生成的数据：{data}")

    # 统计和数据验证
    total_count = 0
    for device_data in data:
        device_type = list(device_data.keys())[0]
        if device_type != "count" and device_type != "device_name":
            count = device_data["count"]
            total_count += count
            logger.info(f"  - {device_type}: {count} 条数据")

    logger.info(f"数据统计: 总共 {total_count} 条数据")

    # 发送数据
    logger.info("开始发送数据到云端...")
    start_send_time = time.time()

    sender = SendToVcloud(data, env, group_size=30)
    queue_info = sender.get_queue_info()
    logger.info(f"数据分组信息: {queue_info['total_groups']} 个分组")

    await sender.send()

    send_time = time.time() - start_send_time
    logger.info(f"数据发送完成，总耗时: {send_time:.2f}秒")
    logger.info(f"{'=' * 60}")
    logger.info("任务执行完成")
    logger.info(f"{'=' * 60}")


# 默认配置
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
            "data": {"HR": 11, "RR": 15, "Temp": 32.3},
            "Note": "Case1",
            "priority": "high"
        },
        {
            "range": [15, 30],
            "data": {"HR": 11, "RR": 15, "Temp": 32.3},
            "Note": "Case1",
            "priority": "medium"
        },
        {
            "range": [30, 45],
            "data": {"HR": 11, "RR": 15, "Temp": 32.3},
            "Note": "Case1",
            "priority": "low"
        },
        {
            "range": [45, 60],
            "data": {"HR": 11, "RR": 15, "Temp": 32.3},
            "Note": "Case1",
            "priority": "lowest"
        }
    ]
}

if __name__ == '__main__':
    # 环境配置
    env_config = EnvParameterinfo(
        url='https://vcloud-test.vivalnk.com',
        name="测试环境",
        env_type="Dev",
        id="617070e40daf63ba334ece90d1",
        value="@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF",
        description="孟买测试环境"
    )
    device_names = [
        "ECGRec_JUN/E310004",  # ECG设备
        "O2 C208S_J87C6F900004",  # SpO2设备 (注意: 这里使用了空格)
        "BP5S_00J00000004",  # BP设备
        "Temp_AOJ-20F_AJUN00000003"  # 体温设备
    ]
    # 病人配置
    Patient_Profile = {
        "ProjectId": "Test_310",
        "SubjectId": "J250317005",
        "DeviceId": device_names,
        "Data_Config": DEFAULT_CONFIG,
        "TimeZoneName": "America/New_York",
        "TimeZoneOffset": 28800,
        "StartTime": "2025-10-09 00:00:00",
        "is_get_timezone_offset": True,
        "Days": 0
    }

    # 执行主程序
    start_total_time = time.time()
    a = "-" * 15

    try:
        for device in Patient_Profile['DeviceId']:
            logger.info(f"{'@' * 80}")
            P = Patient_Profile

            for i in range(P["Days"], -1, -1):
                modify_Time = ModifyTime(P["StartTime"], days=i).date_minus()
                start_time = ModifyTime(modify_Time, hours=0, minutes=00, seconds=0).date_plus()
                end_time = ModifyTime(start_time, hours=0, minutes=1, seconds=0).date_plus()

                logger.info(f"{a}↓↓↓ 发送Device：{device} {start_time}-->{end_time} 的数据 ↓↓↓{a}")

                # 运行异步主函数
                asyncio.run(
                    main(start_time, end_time, [device], P["TimeZoneOffset"],
                         P["TimeZoneName"], P["ProjectId"], P["SubjectId"],
                         P["Data_Config"], P["is_get_timezone_offset"], env_config)
                )

                logger.info(f"{a}↑↑↑ Device：{device} {start_time}->{end_time} 数据发送完成 ↑↑↑{a}\n")

        total_time = time.time() - start_total_time
        logger.info(f"{'&' * 50}")
        logger.info(f"总计发送完成")
        logger.info(f"{a}本次发送总耗时：{total_time:.2f}秒 {a}")
        logger.info(f"{'@' * 80}")

    except Exception as e:
        logger.error(f"程序执行异常: {e}")
        raise