# -*- coding: utf-8 -*-
"""
# @Creation time: 2026/02/03 11:32
# @Author       : 郭军
# @Email        : 391350540@qq.com
# @FileName     : MultiDeviceHistoricalDataSender_V2.py
# @Software     : PyCharm
# @Project      : TestTool
# @PythonVersion: python 3.14
# @Version      : 0.1
# @Description  : 支持 V2 和 V1发送多设备历史数据到云端
# @Update Time  :
# @UpdateContent:
"""

import datetime
import logging
import time
import random
import re
import json
from typing import Dict, List, Union, Optional, Any,Tuple
import requests
from queue import Queue
from logger import logger
import asyncio
from datetime import datetime, timedelta, UTC, timezone
import pytz
from dataclasses import dataclass
from getEcgData import *
import aiohttp
import jwt

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

@dataclass
class PatientProfile:
    """患者信息类"""
    # 常规信息
    projectId: str
    subjectId: str
    siteName: str
    deviceName: list[Any]
    timeZoneName: str
    timeZoneOffset: int
    data_Config: Dict[str, Any]
    startTime: str
    is_get_timezone_offset: bool
    version: str
    days: int

    # v2 用信息
    tenantId: str
    siteId: str
    deviceId: str
    sensorId: str
    sessionId: str
    patientId: str
    deviceSecret: str



class AuthManager:
    """JWT认证令牌生成管理类"""

    # 类常量
    JWT_ALGORITHM = 'HS256'
    TOKEN_TTL = timedelta(hours=1)
    TOKEN_EXPIRY_BUFFER = 60  # 提前60秒刷新令牌

    def __init__(
            self,
            tenant_name: str,
            device_id: str,
            device_secret: str,
            site_id: Optional[str] = None,
            tenant_id: Optional[str] = None
    ) -> None:
        """
        初始化认证管理器

        Args:
            tenant_name: 租户名称
            device_id: 设备ID
            device_secret: 设备密钥
            site_id: 站点ID (可选)
            tenant_id: 租户ID (可选)
        """
        self.tenant_name = tenant_name
        self.site_id = site_id
        self.device_id = device_id
        self.device_secret = device_secret
        self.tenant_id = tenant_id

        self._token: Optional[str] = None

    def _is_token_valid(self) -> bool:
        """
        检查当前令牌是否有效

        Returns:
            bool: 令牌是否有效
        """
        if not self._token:
            return False

        try:
            # 解码JWT令牌（不验证过期时间，我们自己检查）
            decoded = jwt.decode(
                jwt=self._token,
                key=self.device_secret,
                algorithms=[self.JWT_ALGORITHM],
                options={"verify_exp": False}
            )

            # 检查是否即将过期（提前过期缓冲时间）
            exp_timestamp = decoded.get('exp')
            if not exp_timestamp:
                return False

            current_timestamp = datetime.now().timestamp()
            time_until_expiry = exp_timestamp - current_timestamp

            return time_until_expiry > self.TOKEN_EXPIRY_BUFFER

        except (jwt.InvalidTokenError, KeyError):
            return False

    def refresh_token(self) -> str:
        """
        刷新JWT令牌

        Returns:
            str: 新的JWT令牌
        """
        # 确保必要参数存在
        if not all([self.tenant_id, self.site_id, self.device_id]):
            raise ValueError("tenant_id, site_id和device_id不能为空")

        current_time = datetime.now()

        # 构建JWT载荷
        payload = {
            'iss': self.tenant_id,
            'sub': f"{self.tenant_id}/{self.site_id}/{self.device_id}",
            'iat': int(current_time.timestamp()),
            'exp': int((current_time + self.TOKEN_TTL).timestamp()),
            'deviceId': self.device_id,
            'siteId': self.site_id,
            'tenantId': self.tenant_id
        }

        # 生成JWT令牌
        self._token = jwt.encode(
            payload=payload,
            key=self.device_secret,
            algorithm=self.JWT_ALGORITHM
        )

        return self._token

    def get_token(self) -> str:
        """
        获取有效令牌（如果当前令牌无效则刷新）

        Returns:
            str: 有效的JWT令牌
        """
        if not self._is_token_valid():
            self.refresh_token()

        return self._token

    @property
    def token(self) -> Optional[str]:
        """获取当前令牌（不自动刷新）"""
        return self._token

    @token.setter
    def token(self, value: str) -> None:
        """设置令牌"""
        self._token = value



class TimestampDataRouter:
    """时间戳数据路由器 - 根据时间戳获取对应的数据配置"""

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or DEFAULT_CONFIG
        self.month_configs = self.config.get('month_configs', {})
        self.weekday_configs = self.config.get('weekday_configs', {})
        self.day_configs = self.config.get('day_configs', {})
        self.hour_configs = self.config.get('hour_configs', {})
        self.minute_configs = self.config.get('minute_configs', {})
        self.second_configs = self.config.get('second_configs', [])

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

    def get_datetime_components(self, timestamp: float) -> Dict[str, any]:
        """获取时间戳的年、月、日、时、分、秒等组件"""
        normalized_ts = self.normalize_timestamp(timestamp)
        dt = datetime.fromtimestamp(normalized_ts)

        return {
            'year': dt.year,
            'month': dt.month,  # 1-12
            'day': dt.day,  # 1-31
            'hour': dt.hour,  # 0-23
            'minute': dt.minute,  # 0-59
            'second': dt.second,  # 0-59
            'weekday': dt.weekday(),  # 0=周一, 6=周日
            'day_of_year': dt.timetuple().tm_yday,
            'timestamp': timestamp,
            'normalized_timestamp': normalized_ts
        }

    def find_range_match(self, value: int, ranges_config: List[Dict]) -> Optional[Dict]:
        """在范围列表中查找匹配的范围"""
        for range_config in ranges_config:
            range_start, range_end = range_config['range']
            if range_start <= value < range_end:
                return range_config
        return None

    def get_data_for_timestamp(self, timestamp: float) -> Dict[str, Any]:
        """根据时间戳获取对应的数据配置"""
        components = self.get_datetime_components(timestamp)
        month = components['month']
        weekday = components['weekday']
        day = components['day']
        hour = components['hour']
        minute = components['minute']
        second = components['second']

        result_info = {
            'components': components,
            'timestamp': timestamp,
            'normalized_timestamp': components['normalized_timestamp'],
            'message': '默认配置数据',
            'matched_levels': []  # 记录匹配到的配置层级
        }

        matched_config = None
        final_data = None

        # 1. 检查月配置
        month_ranges = self.month_configs.get('ranges', [])
        if month_ranges:
            month_match = self.find_range_match(month, month_ranges)
            if month_match:
                matched_config = month_match
                final_data = month_match['data']
                result_info['matched_levels'].append('month')
                result_info['month_range'] = month_match['range']

        # 2. 检查星期配置（如果存在且月配置也匹配或没有月配置）
        weekday_ranges = self.weekday_configs.get('ranges', [])
        if weekday_ranges:
            weekday_match = self.find_range_match(weekday, weekday_ranges)
            if weekday_match:
                # 如果已经有月配置匹配，检查星期配置是否覆盖月配置
                if matched_config:
                    # 根据优先级决定是否覆盖：如果星期配置有更高优先级或月配置允许被覆盖
                    if weekday_match.get('priority', 0) > matched_config.get('priority', 0):
                        matched_config = weekday_match
                        final_data = weekday_match['data']
                        result_info['matched_levels'].append('weekday')
                        result_info['weekday_range'] = weekday_match['range']
                else:
                    matched_config = weekday_match
                    final_data = weekday_match['data']
                    result_info['matched_levels'].append('weekday')
                    result_info['weekday_range'] = weekday_match['range']

        # 3. 检查日配置
        day_ranges = self.day_configs.get('ranges', [])
        if day_ranges:
            day_match = self.find_range_match(day, day_ranges)
            if day_match:
                # 检查优先级
                if not matched_config or day_match.get('priority', 0) > matched_config.get('priority', 0):
                    matched_config = day_match
                    final_data = day_match['data']
                    result_info['matched_levels'].append('day')
                    result_info['day_range'] = day_match['range']

        # 4. 检查小时配置
        hour_ranges = self.hour_configs.get('ranges', [])
        if hour_ranges:
            hour_match = self.find_range_match(hour, hour_ranges)
            if hour_match:
                # 检查优先级
                if not matched_config or hour_match.get('priority', 0) > matched_config.get('priority', 0):
                    matched_config = hour_match
                    final_data = hour_match['data']
                    result_info['matched_levels'].append('hour')
                    result_info['hour_range'] = hour_match['range']

        # 5. 检查分钟配置
        minute_ranges = self.minute_configs.get('ranges', [])
        if minute_ranges:
            minute_match = self.find_range_match(minute, minute_ranges)
            if minute_match:
                # 检查优先级
                if not matched_config or minute_match.get('priority', 0) > matched_config.get('priority', 0):
                    matched_config = minute_match
                    final_data = minute_match['data']
                    result_info['matched_levels'].append('minute')
                    result_info['minute_range'] = minute_match['range']

        # 6. 检查秒配置
        if self.second_configs:
            second_match = self.find_range_match(second, self.second_configs)
            if second_match:
                # 检查优先级
                if not matched_config or second_match.get('priority', 0) > matched_config.get('priority', 0):
                    matched_config = second_match
                    final_data = second_match['data']
                    result_info['matched_levels'].append('second')
                    result_info['second_range'] = second_match['range']

        # 7. 如果有匹配的配置，返回对应的数据
        if matched_config and final_data:
            return {
                **result_info,
                'second': second,
                'hour': hour,
                'minute': minute,
                'day': day,
                'month': month,
                'weekday': weekday,
                'data': final_data,
                'Note': matched_config.get('Note', ''),
                'priority': matched_config.get('priority', 'normal'),
                'config_level': result_info['matched_levels'][-1] if result_info['matched_levels'] else 'none',
                'message': f"匹配到{result_info['matched_levels'][-1]}级配置" if result_info[
                    'matched_levels'] else '无匹配配置'
            }

        # 8. 如果没有匹配任何配置，返回默认随机数据
        HR = random.choice(
            [-101, -201, -301, -316, -401, 0, 1, 11, 22, 33, 44, 55, 66, 77, 88, 99, 100, 151, 181, 199, 200, 300])
        RR = random.choice([15, 18, 19, 20])
        Temp = random.choice([33.2, 20, 44])
        sys = random.choice(
            [120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250, 260, 270, 280, 290, 300])
        dia = random.choice(
            [80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250, 260, 270, 280, 290,
             300])

        return {
            **result_info,
            'second': second,
            'hour': hour,
            'minute': minute,
            'day': day,
            'month': month,
            'weekday': weekday,
            "data": {"HR": HR, "RR": RR, "Temp": Temp, "sys": sys, "dia": dia},
            'Note': "默认随机列表数据",
            'priority': '默认',
            'config_level': 'default',
            'message': '没有匹配的时间范围,返回默认随机数据'
        }

    def get_data_for_time(self, year: int, month: int, day: int,
                          hour: int, minute: int, second: int) -> Dict[str, Any]:
        """根据具体时间获取数据配置（方便测试）"""
        dt = datetime(year, month, day, hour, minute, second)
        timestamp = dt.timestamp() * 1000  # 转换为毫秒时间戳
        return self.get_data_for_timestamp(timestamp)


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

class CommonTools:
    """公共工具类"""

    def __init__(self):
        self.ts = time.time()

    """返回带毫秒的字符串时间"""
    def _format_timestamp(self,ts):
        return datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] # %f: 毫秒（六位小数的微秒


    def format_time_range(self, start_ms, end_ms, offset_ms=0):
        """格式化时间范围，支持偏移量"""
        start_str = self._format_timestamp((start_ms + offset_ms))
        end_str = self._format_timestamp((end_ms + offset_ms))
        return f"{start_str} ~ {end_str}"

    # 根据时区名格式时间范围
    @staticmethod
    def format_time_range_by_timezone(start_ms, end_ms, timezone_name):
        """根据时区名格式时间范围"""
        timezone = pytz.timezone(timezone_name)
        start_str = timezone.localize(datetime.fromtimestamp(start_ms/1000)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        end_str = timezone.localize(datetime.fromtimestamp(end_ms/1000)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        if start_ms == end_ms:
            return start_str
        else:
            return f"{start_str} ~ {end_str}"



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
        "BP": 3600,  # 每300秒（5分钟）1条
        "SpO2": 4,  # 每4秒1条
        "Temperature": 12,  # 每12秒1条
        "CGM": 300  # 每300秒（5分钟）1条
    }
    """
                  (120, 120, "Non-Dipper"),
                  (120, 119.9, "Non-Dipper"),
                  (120, 108, "Dipper"),
                  (120, 108.012, "Non-Dipper"),
                  (120, 96, "Extreme"),
                  (120, 96.012, "Dipper"),
                  (120, 126, "Reverse"),
                  (130, 104, "Extreme"),
                  (100, 90, "Dipper"),
                  (140, 126, "Dipper"),
          """


    def __init__(self,patientprofile: PatientProfile, project_id: str, subject_id: str, log_output: bool = False,
                 is_flash: int = 0, data_config: Dict[str, Any] = None,dict_bp: List[Tuple[int, int, int, str]] = None):
        """
        初始化医疗设备数据生成器

        Args:
            project_id: 项目ID
            subject_id: 受试者ID
            log_output: 是否输出日志
            is_flash: 是否历史数据

            data_config: 数据配置
        """
        self.patientProfile = patientprofile
        self.DICT_BP = dict_bp
        self.ProjectId = project_id
        self.SubjectId = subject_id
        self.log_output = log_output
        self.is_flash = is_flash
        self.version = patientProfile.version

        ecg_list, hr_list, rr_list = getEcgData()

        # 保存所有ECG数据
        self.ecg_data = ecg_list
        self.hr_data = hr_list
        self.rr_data = rr_list

        # 初始化索引和当前数据
        self.current_index = 0
        self.ECG_waveform = self.ecg_data[self.current_index]


        # 初始化随机毫秒值
        self._random_ms = None

        # 初始化数据路由器
        self.data_router = TimestampDataRouter(data_config)

        # 初始化正常/异常数据范围
        self._init_data_ranges()

        # 初始化时间工具
        self.common_tools = CommonTools()

        # # 配置日志
        # if log_output:
        #     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


    # 循环获取BP数据
    def get_next_bp(self):
        """获取下一个ECG数据"""
        self.BP_DATA = self.DICT_BP[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.DICT_BP)
        return self.BP_DATA

    # 循环获取波形数据
    def get_next_ecg(self):
        """获取下一个ECG数据"""
        self.current_index = (self.current_index + 1) % len(self.ecg_data)
        self.ECG_waveform = self.ecg_data[self.current_index]
        return self.ECG_waveform

    def get_previous_ecg(self):
        """获取上一个ECG数据"""
        self.current_index = (self.current_index - 1) % len(self.ecg_data)
        self.ECG_waveform = self.ecg_data[self.current_index]
        return self.ECG_waveform

    @property
    def random_ms(self):
        """只在首次访问时生成随机值"""
        if self._random_ms is None:
            self._random_ms = random.randint(100, 999)
            # print(f"生成毫秒随机值: {self.random_ms}")
            logger.info(f"生成初始毫秒随机值: {self._random_ms}")
        return self._random_ms

    def _init_data_ranges(self):
        """初始化数据范围"""
        self.normal_HR_list = [i for i in range(51, 91)] # BP 中的 HR也读取本列表
        # self.normal_HR_list = ["NA"] # 传 NA
        self.normal_RR_list = [i for i in range(19, 20)]
        self.abnormal_HR_list = [i for i in range(111, 131)]
        self.abnormal_RR_list = [i for i in range(25, 35)]
        self.normal_temp_list = [round(random.uniform(36.1, 37.2), 1) for _ in range(10)]
        self.abnormal_temp_list = [round(random.uniform(38.1, 39.2), 1) for _ in range(3)]
        self.normal_spo2_list = [i for i in range(95, 100)]
        self.abnormal_spo2_list = [i for i in range(90, 95)]
        # self.normal_bp_dia_list = [i for i in range(60, 90)] # 正常舒张压范围：60~90 mmHg
        # self.normal_bp_sys = [i for i in range(90, 139)] # 正常收缩压范围：90~139 mmHg



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
                device_type, device_name, current_ts_ms+self.random_ms, timezone_offset, timezone_name
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
        temp = temp_data if temp_data else random.choice(self.abnormal_temp_list if use_abnormal else self.normal_temp_list)


        # ecg_wave = [random.randint(-100, 100) for _ in range(100)]

        ecg_wave = self.get_next_ecg()

        # 使用AccStep计算步数
        acc_step = AccStep(record_time_ms, timezone_offset or 28800)
        steps = acc_step.get_acc_step_total()


        ecgData = self.assemble_ECG_data(record_time_ms, ecg_wave, hr, rr, temp,steps,
                               timezone_offset, timezone_name, device_name)
        ecgData["data"]["steps"] = steps  # 更新步数
        result = router_data

        logger.debug(
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



        data = self.assemble_SpO2_data(record_time_ms, spo2, device_name,
                                       timezone_offset, timezone_name)


        return data


    def _generate_bp_data(self, record_time_ms: int, device_name: str,
                          router_data: Dict, timezone_offset: Optional[int],
                          timezone_name: Optional[str]) -> Dict:
        """ 生成血压数据 """
        if self.DICT_BP != None:
            sys = self.DICT_BP[0]
            dia = self.DICT_BP[1]
            hr = self.DICT_BP[2]
        elif router_data.get("data") != None:
            sys = router_data.get("data").get("BP").get("sys")
            dia = router_data.get("data").get("BP").get("dia")
            hr = router_data.get("data").get("HR")
        else:
            dia = random.choice(self.normal_bp_dia_list)
            sys = random.choice(self.normal_bp_sys)
            hr = random.choice(self.normal_HR_list)

        """ ABPM设备 随机生成零值血压数据"""
        if "TM-2441" in device_name:
            if random.choice([0,1,2])<1:
                dia = 0
                sys = 0
                hr = 0
            else:
                pass

        result = self.assemble_BP_data(record_time_ms, dia, sys, hr,device_name,
                                     timezone_offset, timezone_name)
        logger.info(f"{result.get('recordTime')}--{result.get("data")["deviceName"]}--{result.get("data")["sys"]}--{result.get("data")["dia"]} | {timezone_name}-->{self.common_tools.format_time_range_by_timezone(result.get("recordTime"),result.get("recordTime"),timezone_name)}")

        return result

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


    def assemble_ECG_data(self, recordTime: int, ECG: List[int], HR: int, RR: int, TEMP: float,Steps: int,
                          TimeZoneOffset: Optional[int], TimeZoneName: Optional[str], device_name: str,) -> Dict:
        """ 组装 ECG 数据 """
        #精简设备名称
        _device_name=device_name.split("_")[-1]

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
                "subjectId": self.patientProfile.subjectId,
                "projectId": self.patientProfile.projectId
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
                "accStepOffset": 1,
                "accStepTotal": Steps,
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

        data_v2 = {
            "localId": "faf82c75-7f2e-42d1-85e3-ad7a1f7b62fc_Jun",
            "sensorMac": "EA:2D:DD:E1:DB:0D",
            "sensorHardware": "03",
            "sensorVendor": "VivaLink",
            "uploadTime": recordTime,
            "tenantId": self.patientProfile.tenantId,
            "siteId": self.patientProfile.siteId,
            "deviceId": self.patientProfile.deviceId,
            "sensorId": self.patientProfile.sensorId,
            "recordTime": recordTime,
            "appId": "com.vivalink.vcloud2",
            "appVersion": "1.0.0+1",
            "sdkVersion": "v3.6.0-mvm.3309",
            "subjectId": self.patientProfile.subjectId,
            "sessionId": self.patientProfile.sessionId,
            "patientId": self.patientProfile.patientId,
            "tenantName": self.patientProfile.projectId,
            "siteName": self.patientProfile.siteName,
            "customData": "",
            "sensorType": 100,
            "sensorName": device_name,
            "sensorSn": _device_name,
            "sensorModel": "VV330_1",
            "sensorVersion": "03",
            "sensorFirmware": "3.0.0.0028T4",
            "sensorBattery": 51,
            "sensorRssi": -10001,
            "deviceBattery": 51,
            "deviceIp": "112.16.81.96",
            "deviceOsType": "Android",
            "deviceOsVersion": "UP1A.231005.007.S901U1UES8EYC1",
            "deviceToken": "QWMvMicgrjUcg7i4azmen1DDRMAuYSJF",
            "deviceType": "Phone",
            "carrier": "--",
            "networkType": "Unknown",
            "latitude": 0,
            "longitude": 0,
            "collectTime": recordTime,
            "timezone": 8,
            "timezoneName": TimeZoneName,
            "autoTimezone": True,
            "type": "EcgRaw",
            "language": "en_US",
            "deviceName": "14day-1",
            "deviceUniqueId": "14day-1",
            "sensorTypeName": "ECG",
            "data": {
                "dataStreamMode": "FullDualMode",
                "accFrequency": 25,
                "denoiseEcg":"",
                "ecgFrequency": 128,
                "effective": -1,
                "activityScore": 0,
                "recordTime": recordTime,
                "accAccuracy": 2048,
                "magnification": 1000,
                "accStepTotal": Steps,
                "sf": 128,
                "rssi": -10001,
                "acc": [
                    {
                        "x": -9,
                        "y": -6,
                        "z": -2094
                    },
                    {
                        "x": -8,
                        "y": 4,
                        "z": -2100
                    },
                    {
                        "x": -13,
                        "y": -2,
                        "z": -2088
                    },
                    {
                        "x": -18,
                        "y": 6,
                        "z": -2086
                    },
                    {
                        "x": -18,
                        "y": -11,
                        "z": -2080
                    },
                    {
                        "x": 1,
                        "y": -4,
                        "z": -2094
                    },
                    {
                        "x": -5,
                        "y": 1,
                        "z": -2085
                    },
                    {
                        "x": -9,
                        "y": 3,
                        "z": -2107
                    },
                    {
                        "x": -16,
                        "y": 15,
                        "z": -2088
                    },
                    {
                        "x": -8,
                        "y": 3,
                        "z": -2093
                    },
                    {
                        "x": -15,
                        "y": 9,
                        "z": -2085
                    },
                    {
                        "x": -1,
                        "y": 4,
                        "z": -2093
                    },
                    {
                        "x": -14,
                        "y": 17,
                        "z": -2084
                    },
                    {
                        "x": -14,
                        "y": 11,
                        "z": -2090
                    },
                    {
                        "x": -16,
                        "y": 7,
                        "z": -2089
                    },
                    {
                        "x": -9,
                        "y": 3,
                        "z": -2086
                    },
                    {
                        "x": -8,
                        "y": -5,
                        "z": -2095
                    },
                    {
                        "x": -7,
                        "y": 14,
                        "z": -2087
                    },
                    {
                        "x": -3,
                        "y": 12,
                        "z": -2089
                    },
                    {
                        "x": -10,
                        "y": -3,
                        "z": -2079
                    },
                    {
                        "x": -9,
                        "y": 4,
                        "z": -2084
                    },
                    {
                        "x": -5,
                        "y": -9,
                        "z": -2090
                    },
                    {
                        "x": -6,
                        "y": 15,
                        "z": -2088
                    },
                    {
                        "x": -14,
                        "y": 12,
                        "z": -2084
                    },
                    {
                        "x": -5,
                        "y": 5,
                        "z": -2076
                    }
                ],
                "temperature":TEMP,
                "flash": self.is_flash,
                "accActivity": 0,
                "ecg": ECG,
                "deviceId": "EA:2D:DD:E1:DB:0D",
                "rmssd": 529,
                "deviceSN": _device_name,
                "noise": -1,
                "battery": 51,
                "deviceType": "ECG",
                "rri": [
                    390,
                    804,
                    0,
                    0,
                    0
                ],
                "activity": True,
                "posture": 2,
                "crc": True,
                "leadOn": True,
                "deviceName": _device_name,
                "avRR": -10001,
                "accStepOffset": 0,
                "rwl": [
                    113,
                    -1,
                    -1,
                    -1,
                    -1
                ],
                "snr": -1,
                "dataMode": "FullDualMode",
                "rr": RR,
                "receiveTime": recordTime,
                "hr": HR,
                "deviceInfo": {
                    "accSamplingFrequency": 25,
                    "manufacturer": "VIVALNK",
                    "batteryStatus": "NOT_INCHARGING",
                    "fwVersion": "3.0.0.0028T4",
                    "accSamplingEnable": 1,
                    "hwVersion": "03",
                    "samplingMultiple": 1,
                    "model": "VV330_1",
                    "patchLeadStatus": 1,
                    "patchSamplingStatus": 1,
                    "leadOffAccEnable": 0,
                    "ecgSamplingFrequency": 128,
                    "accSamplingAccuracy": 2048,
                    "channelNumber": ""
                },
                "rawTemp": TEMP,
                "calibratedTemp": -1
            }
        }
        # 根据版本返回组装数据
        if self.version == "v2":
            return  data_v2
        else:
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

    def assemble_BP_data(self, recordTime: int, bp_dia: int, bp_sys: int,bp_hr: int, device_name: str,
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
            "name": "BPRaw", #SpO2Raw
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
                "deviceType": "BleSig_BP",
                "heartRate": bp_hr,
                "flash": self.is_flash,
                "recordTime": recordTime,
                "hsdValue": 0,
                "battery": 100,
                "sys": bp_sys,
                "arrhythmia": 0,
                "unit": "mmHg"
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

    def __init__(self, patient_data: List[Dict], env: EnvParameterinfo,timezone_name: str = "Asia/Shanghai", group_size: int = 30, patientprofile: PatientProfile = None,sequential_upload: bool = False):
        self.patient_data = patient_data
        self.Env = env
        self.group_size = group_size
        self.data_queue = Queue()
        self.timezone_name = timezone_name
        self.patientProfile=patientprofile
        self.sequential_upload = sequential_upload


        # 处理数据分组
        self.process_patient_data(patient_data)

    def is_v2_checked(self):
        if self.patientProfile.version == "v2":
            return True
        else:
            return False

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

            if self.is_v2_checked() :
                group_data= {"items":group_data}
            else:
                pass

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

    def get_v2_token(self) -> str:
        """获取v2令牌"""
        auth_manager = AuthManager(
            tenant_name=patientProfile.projectId,
            device_id=patientProfile.deviceId,
            device_secret=patientProfile.deviceSecret,
            site_id=patientProfile.siteId,
            tenant_id=patientProfile.tenantId
        )

        # 刷新并获取新令牌
        token = auth_manager.refresh_token()
        logger.info(f"成功获取V2令牌: {token}")
        return token

    # async def send(self):
    #     """异步发送数据到云端"""
    #     # 判断是否为v2
    #     if self.is_v2_checked():
    #         token = self.get_v2_token()
    #     else:
    #         token = self.get_app_token()
    #
    #     tasks = []
    #     total_groups = self.data_queue.qsize()
    #     sent_groups = 0
    #
    #     logger.info(f"开始发送数据，总分组数: {total_groups}")
    #
    #     while not self.data_queue.empty():
    #         data_group = self.data_queue.get()
    #         task = asyncio.create_task(
    #             self.send_data_group(data_group, token)
    #         )
    #         tasks.append(task)
    #         sent_groups += 1
    #
    #         # 每10组等待一次，避免过多并发
    #         if len(tasks) >= 10:
    #             await asyncio.gather(*tasks)
    #             tasks.clear()
    #             logger.info(f"发送进度: {sent_groups}/{total_groups}")
    #
    #     # 等待剩余任务完成
    #     if tasks:
    #         await asyncio.gather(*tasks)
    #
    #     logger.info(f"数据发送完成，总共发送 {sent_groups} 组数据")
    async def send(self):
        """异步发送数据到云端"""
        # 判断是否为v2
        if self.is_v2_checked():
            token = self.get_v2_token()
        else:
            token = self.get_app_token()

        total_groups = self.data_queue.qsize()
        sent_groups = 0

        logger.info(f"开始发送数据，总分组数: {total_groups}")

        # 如果是顺序上传，逐个发送
        if self.sequential_upload:
            while not self.data_queue.empty():
                data_group = self.data_queue.get()
                try:
                    # 等待当前分组发送完成
                    result = await self.send_data_group(data_group, token)
                    if result:
                        sent_groups += 1
                        logger.info(f"发送进度: {sent_groups}/{total_groups}")
                    else:
                        logger.error(f"分组 {data_group['group_index']} 发送失败，停止后续发送")
                        break  # 如果某个分组发送失败，停止后续发送
                except Exception as e:
                    logger.error(f"发送分组时发生异常: {e}")
                    break
        else:
            # 保持原有的并发上传逻辑
            tasks = []

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

    async def send_data_group(self, data_group: Dict, token: str):
        """分组发送数据"""

        # 创建CommonTools实例
        common_tools=CommonTools()

        """发送单个数据分组"""
        try:
            payload = data_group["data"]
            if self.patientProfile.version == "v2":
                record_times = [item.get("recordTime", 0) for item in payload["items"] if isinstance(item, dict)]
                start_time = min(record_times) if record_times else 0
                end_time = max(record_times) if record_times else 0
            else:
                record_times = [item.get("recordTime", 0) for item in payload if isinstance(item, dict)]
                start_time = min(record_times) if record_times else 0
                end_time = max(record_times) if record_times else 0

            # 根据版本选择不同的URL
            if patientProfile.version == "v2":
                url = f"{self.Env.url}/api/producer/telemetry-events"
                token=f"Bearer {token}"
            else:

                if "site2" in self.Env.url:
                    url = f"{self.Env.url}/internal/tenants/VivaLNK/events?type=dataEvent"       #https://site2-vcloud-test.vivalink.com/internal/tenants/VivaLNK/events?type=dataEven
                else:
                    url = f"{self.Env.url}/v2/tenants/VivaLNK/events?type=dataEvent"
            headers = {
                'Content-Type': 'application/json',
                'Authorization': token
                # 注意：不要添加 Content-Length，aiohttp 会自动计算
            }

            logger.debug(f"发送请求到: {url}")
            logger.debug(f"使用Token: {token[:50]}...")
            logger.debug(f"Payload大小: {len(json.dumps(payload))} 字节")
            # 打印数据
            logger.debug(f"数据: {payload}")

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
                                if response_data.get('code') == 200 and (response_data.get(
                                        'message') == 'Batch ingestion done' or response_data.get("code") == 200):
                                    logger.info(f"✓ {data_group['device_name']} {data_group['data_type']} "
                                                f"分组 {data_group['group_index']}/{data_group['total_groups']} "
                                                f"({start_time}~{end_time} - {self.timezone_name} - {common_tools.format_time_range_by_timezone(start_time, end_time, self.timezone_name)}) 发送成功")

                                    return True
                                else:

                                    logger.error(f"✗ 发送失败 - 响应: {response_data},入参：{payload}")
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


async def main(startTime: str, endTime: str, device_names: List[str], patientprofile: PatientProfile, env: EnvParameterinfo,bp_dict:List[Tuple[int, int, int, str]]):

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
    logger.info(f"发送URL: {env.url}")

    # 处理时区偏移量
    if patientProfile.is_get_timezone_offset:
        try:
            _, timeZone_offset = get_timezone_offset(patientProfile.timeZoneName, stampStartTime / 1000)
            logger.info(f"自动获取时区偏移量: {timeZone_offset}秒")
        except Exception as e:
            logger.warning(f"自动获取时区偏移量失败: {e}, 使用默认值: {patientProfile.timeZoneOffset}")



    # 创建数据生成器
    generator = MedicalDeviceDataGenerator(
        patientprofile=patientprofile,
        project_id=patientProfile.projectId,
        subject_id=patientProfile.subjectId,
        log_output=True,
        is_flash=1,
        data_config=patientProfile.data_Config,
        dict_bp = bp_dict,

    )

    # 生成数据
    logger.info("开始生成设备数据...")
    start_gen_time = time.time()

    data = generator.generate_data(
        start_time=stampStartTime,
        end_time=stampEndTime,
        device_types=device_names,
        timezone_name=patientProfile.timeZoneName,
        timezone_offset=patientProfile.timeZoneOffset
    )

    # 入参信息统计
    gen_time = time.time() - start_gen_time
    logger.info(f"数据生成完成，耗时: {gen_time:.2f}秒")
    init_ms = generator.random_ms
    common_tools = CommonTools()
    logger.info(
        "时间参数信息 - "
        f"初始化偏移: {init_ms}ms | "
        f"入参范围: {stampStartTime}~{stampEndTime}({common_tools.format_time_range(stampStartTime, stampEndTime)})"" | "
        f"实际范围: {stampStartTime + init_ms}~{stampEndTime + init_ms}({common_tools.format_time_range(stampStartTime+init_ms, stampEndTime+init_ms)})"
        f"时区本地时间: {patientProfile.timeZoneName}-->{common_tools.format_time_range_by_timezone(stampStartTime+init_ms, stampEndTime+init_ms, patientProfile.timeZoneName)}"
    )

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

    sender = SendToVcloud(data, env,patientProfile.timeZoneName, group_size=200, patientprofile=patientProfile,sequential_upload=True)
    queue_info = sender.get_queue_info()
    logger.info(f"数据分组信息: {queue_info['total_groups']} 个分组")

    await sender.send()

    send_time = time.time() - start_send_time
    logger.info(f"数据发送完成，总耗时: {send_time:.2f}秒")
    logger.info(f"{'=' * 60}")
    logger.info("任务执行完成")
    logger.info(f"{'=' * 60}")


def get_timeranges():
    """生成默认数据列表 """
    # 测试默认 HR
    _FHR = 89
    _ZHR = 89
    data_list=[]
    for i in range(0, 60):

        data = {
            "range": [i, i+1],
            "data":{
                "HR": _FHR if i % 2 == 0 else _ZHR,
                "RR": 15,
                "Temp": 32.3,
                "sys": 120, # Systolic 收缩压
                "dia": 120, # Diastolic 舒张压
            },
            "Note": "Case1",
            "priority": "medium"
        }
        data_list.append(data)
    return data_list

# # 默认配置
# DEFAULT_CONFIG = {
#     "timestamp_formats": {
#         "second": 10,
#         "millisecond": 13,
#         "microsecond": 16,
#         "nanosecond": 19
#     },
#     # 用 lamada 生成一个 0~60 秒，一秒_FHR,一秒_ZHR
#     "time_ranges": get_timeranges(),
# }

DEFAULT_CONFIG = {
    # 月配置：每个月1-10日，返回数据A，10-15，返回数据B，15~20 返回数据 C，20~25，返回数据 D,25~31，返回数据 E
    'day_configs': {
        'ranges': [
            {
                'range': [1, 11],  # 1-10日（包含1，不包含11）
                'data': {"HR": 70, "RR": 16, "Temp": 36.2, "sys": 120, "dia": 80},
                'Note': '每月1-10日数据A',
                'priority': 10
            },
            {
                'range': [10, 16],  # 10-15日
                'data': {"HR": 75, "RR": 18, "Temp": 36.5, "sys": 125, "dia": 85},
                'Note': '每月10-15日数据B',
                'priority': 10
            },
            {
                'range': [15, 21],  # 15-20日
                'data': {"HR": 80, "RR": 20, "Temp": 36.8, "sys": 130, "dia": 90},
                'Note': '每月15-20日数据C',
                'priority': 10
            },
            {
                'range': [20, 26],  # 20-25日
                'data': {"HR": 85, "RR": 22, "Temp": 37.0, "sys": 135, "dia": 95},
                'Note': '每月20-25日数据D',
                'priority': 10
            },
            {
                'range': [25, 32],  # 25-31日
                'data': {"HR": 90, "RR": 24, "Temp": 37.2, "sys": 140, "dia": 100},
                'Note': '每月25-31日数据E',
                'priority': 10
            }
        ]
    },

    # 小时配置示例
    'hour_configs': {
        'ranges': [
            {
                'range': [0, 6],  # 0-5点
                'data': {"HR": 65, "RR": 14, "Temp": 36.0, "sys": 115, "dia": 75},
                'Note': '凌晨数据',
                'priority': 20  # 优先级高于日配置
            },
            {
                'range': [8, 12],  # 8-11点
                'data': {"HR": 75, "RR": 18, "Temp": 36.6, "sys": 125, "dia": 85},
                'Note': '上午数据',
                'priority': 20
            }
        ]
    },

    # 分钟配置示例
    'minute_configs': {
        'ranges': [
            {
                'range': [0, 15],  # 0-14分
                'data': {"HR": 72, "RR": 17, "Temp": 36.4, "sys": 122, "dia": 82},
                'Note': '每刻钟前15分钟',
                'priority': 30  # 优先级高于小时配置
            }
        ]
    },

    # 秒配置示例（原来的time_ranges）
    'second_configs': [
        {
            'range': [0, 10],
            'data': {"HR": 69, "RR": 18, "Temp": 36.3, "sys": 120, "dia": 80},
            'Note': '每分钟前10秒',
            'priority': 40  # 优先级最高
        },
        {
            'range': [50, 60],
            'data': {"HR": 89, "RR": 22, "Temp": 36.9, "sys": 135, "dia": 95},
            'Note': '每分钟最后10秒',
            'priority': 40
        }
    ],

    # 星期配置示例
    'weekday_configs': {
        'ranges': [
            {
                'range': [0, 5],  # 周一至周五 (0=周一, 4=周五)
                'data': {"HR": 75, "RR": 18, "Temp": 36.5, "sys": 125, "dia": 85},
                'Note': '工作日数据',
                'priority': 5  # 优先级低于日配置
            },
            {
                'range': [5, 7],  # 周六至周日 (5=周六, 6=周日)
                'data': {"HR": 68, "RR": 16, "Temp": 36.3, "sys": 118, "dia": 77},
                'Note': '周末数据',
                'priority': 5
            }
        ]
    },

    # 月配置示例
    'month_configs': {
        'ranges': [
            {
                'range': [12, 13],  # 12月 (1-12月)
                'data': {"HR": 78, "RR": 19, "Temp": 36.7, "sys": 128, "dia": 88},
                'Note': '十二月数据',
                'priority': 1  # 最低优先级
            }
        ]
    }
}

if __name__ == '__main__':
    # 设置日志的等级 #logging.DEBUG
    # logger.setLevel(logging.DEBUG)
    logger.setLevel(logging.INFO)


    """V1环境配置"""
    # env_config = EnvParameterinfo(
    #     url='https://vcloud-test.vivalnk.com',
    #     name="测试环境-孟买",
    #     env_type="Dev",
    #     id="617070e40daf63ba334ece90d1",
    #     value="@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF",
    #     description="孟买测试环境"
    # )

    """V2环境配置"""
    env_config = EnvParameterinfo(
        url='https://test2.uat.ai.vivalink.com',
        name="V2-UAT环境-孟买",
        env_type="UAT",
        id="617070e40daf63ba334ece90d1",
        value="@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF",
        description="V2-UAT环境"
    )

    # env_config = EnvParameterinfo(
    #     url='https://site2-vcloud-test.vivalink.com',
    #     name="测试环境-加州",
    #     env_type="Dev",
    #     id="617070e40daf63ba334ece90d1",
    #     value="@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF",
    #     description="加州测试环境"
    # )

    device_names = [
        "ECGRec_202420/E310614",  # ECG设备
        # "BP_TM-2441_J26012401",
        # "O2 J20251213001",
        # "F53.25121301"
        # "O2 C208S_J87C6F900004",  # SpO2设备 (注意: 这里使用了空格)
        # "BP5S_00J00000004",  # BP设备
        # "Temp_AOJ-20F_AJUN00000003"  # 体温设备
    ]

    # 患者信息
    # patientProfile = PatientProfile(
    #     # 常规信息 v1
    #     projectId="test2",
    #     subjectId="J002",
    #     siteName="test2",
    #     deviceName=device_names,
    #     timeZoneName="Asia/Shanghai",
    #     timeZoneOffset=39600,
    #     data_Config=DEFAULT_CONFIG,
    #     startTime="2026-01-26 00:00:00",
    #     is_get_timezone_offset=True,
    #     version="v2",
    #     days= 0,
    #
    #
    #     # v2 用信息
    #     tenantId="019bef40-f47a-7807-8e37-d02998a83d9d",
    #     siteId="019bef40-f47a-780e-b840-62565b7fba0f",
    #     deviceId="019c1d98-332c-71d0-b487-50efee55d8c7",
    #     sensorId="019c2737-c62a-70e0-a03d-f7ca175e4ce7",
    #     sessionId="019c2740-db5c-7b42-819a-f6ebf84bcca0",
    #     patientId="019c2739-ccf2-74f7-8790-f1fd4352fe90",
    #     deviceSecret="zkqGJRewFFMT6qw2ssJoKlcolgvT4F8E"
    # )

    patientProfile = PatientProfile(
        # 常规信息 v1
        projectId="test2",
        subjectId="J003",
        siteName="test2",
        deviceName=device_names,
        timeZoneName="Asia/Shanghai",
        timeZoneOffset=39600,
        data_Config=DEFAULT_CONFIG,
        startTime="2026-01-01 00:00:00",
        is_get_timezone_offset=True,
        version="v2",
        days=15,

        # v2 用信息
        tenantId="019bef40-f47a-7807-8e37-d02998a83d9d",
        siteId="019bef40-f47a-780e-b840-62565b7fba0f",
        deviceId="019c1d98-332c-71d0-b487-50efee55d8c7",
        sensorId="019c27d6-1dc7-7e11-b702-83e7f8792da1",
        sessionId="019c27d6-9216-7a18-81f6-ce7ccfe8547d",
        patientId="019c27d3-b005-71bf-9722-b3b85c8cbe19",
        deviceSecret="zkqGJRewFFMT6qw2ssJoKlcolgvT4F8E"
    )

    # 执行主程序
    start_total_time = time.time()
    a = "-" * 15

    try:
        for device in patientProfile.deviceName:
            logger.info(f"{'@' * 80}")
            k = 0
            for i in range(patientProfile.days, -1, -1):

                modify_Time = ModifyTime(patientProfile.startTime, days=i).date_minus()
                start_time = ModifyTime(modify_Time, hours=0, minutes=00, seconds=0).date_plus()
                end_time = ModifyTime(start_time, hours=23, minutes=59, seconds=59).date_plus()

                logger.info(f"{a}↓↓↓ 发送Device：{device} {start_time}-->{end_time} 的数据 ↓↓↓{a}")
                BP_DICT = None

                # 运行异步主函数
                asyncio.run(
                    main(start_time, end_time, [device], patientProfile, env_config, BP_DICT)
                )
                k += 1

                logger.info(f"{a}↑↑↑ Device：{device} {start_time}->{end_time} 数据发送完成 ↑↑↑{a}\n")

        total_time = time.time() - start_total_time
        logger.info(f"{'&' * 50}")
        logger.info(f"总计发送完成")
        logger.info(f"{a}本次发送总耗时：{total_time:.2f}秒 {a}")
        logger.info(f"{'@' * 80}")

    except Exception as e:
        logger.error(f"程序执行异常: {e}")
        raise