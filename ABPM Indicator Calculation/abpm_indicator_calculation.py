#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Project: TestTool
@Author: guojun
@Email: 391350540@qq.com
@Date: 2026/1/7 11:04
@File: abpm_indicator_calculation.py
@IDE: PyCharm
@Description: 计算 ABPM 指标
"""
from datetime import datetime, timedelta, time
import decimal
import json
import sys
import logging
from decimal import Decimal
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import numpy as np
import requests
from zoneinfo import ZoneInfo
from typing import List
import math


class DataType(Enum):
    """数据类型枚举"""
    BP_RAW = "BPRaw"
    ECG_RAW = "ECGRaw"
    SPO2_RAW = "SpO2Raw"


class TimePeriod(Enum):
    """时间周期枚举"""
    DAY = "day"
    NIGHT = "night"
    TOTAL = "total"


@dataclass
class Measurement:
    """单次测量数据"""
    time: int  # 时间戳(毫秒)
    sbp: int  # 收缩压
    dbp: int  # 舒张压
    pr: int  # 脉搏率

    @property
    def mbp(self) -> int:
        """计算公式-平均动脉压(MBP) = DBP + 1/3(SBP − DBP)- 先取整"""
        mbp = Decimal(self.dbp + 1 / 3 * (self.sbp - self.dbp)).quantize(
            Decimal('0'),
            rounding=decimal.ROUND_HALF_UP
        )
        if mbp < 0:
            mbp = 0
        return int(mbp)

    @property
    def pulse_pressure(self) -> int:
        """计算公式-SBP-DPD差值"""
        pulse_pressure = Decimal(self.sbp - self.dbp).quantize(
            Decimal('0'),
            rounding=decimal.ROUND_HALF_UP
        )
        return int(pulse_pressure)


# 血压分布
@dataclass
class BPDistribution:
    # 血压分布默认值
    bp_distribution: List[float] = None

    def __post_init__(self):
        """初始化并设置默认值"""
        if self.bp_distribution is None:
            self.bp_distribution = [0.0, 0.0]

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'bp_distribution': self.bp_distribution
        }


# 测量计划
@dataclass
class MeasurementPlan:
    """测量计划对象"""
    start_time: str
    end_time: str
    interval: int
    enabled: bool

    def __init__(self, start_time: str, end_time: str, interval: int, enabled: bool):
        self.start_time = start_time
        self.end_time = end_time
        self.interval = interval
        self.enabled = enabled


@dataclass
class Statistics:
    """统计指标"""
    min_val: Any
    max_val: Any
    avg: float
    std: float
    cv: float
    data: List[float]
    min_time: Optional[int] = None
    max_time: Optional[int] = None

    def to_dict(self) -> Dict:
        """转换为字典"""
        result = asdict(self)
        # 移除大数据字段以避免日志过大
        result.pop('data', None)
        return result


@dataclass
@dataclass
class SummaryResult:
    """分析结果汇总"""
    qc_results: List[Dict] = None
    total_capture_rate: float = 0.0
    day_capture_rate: float = 0.0
    night_capture_rate: float = 0.0
    day_measurements: float = 0.0
    night_measurements: int = 0
    expected_day_measurements: int = 0
    expected_night_measurements: int = 0
    sbp_distribution: BPDistribution = None
    dbp_distribution: BPDistribution = None
    period: TimePeriod = None
    sbp_stats: Statistics = None
    dbp_stats: Statistics = None
    mbp_stats: Statistics = None
    pr_stats: Statistics = None
    pulse_pressure_stats: Statistics = None
    total_records: int = 0
    above_limits_sbp: Optional[float] = None
    above_limits_dbp: Optional[float] = None
    nocturnal_fall_sbp: Optional[float] = None
    nocturnal_fall_dbp: Optional[float] = None

    def __post_init__(self):
        """初始化默认值"""
        if self.qc_results is None:
            self.qc_results = [{"result": "fail", "day": 0, "Night": 0, "total": 0}]
        if self.sbp_distribution is None:
            self.sbp_distribution = BPDistribution()
        if self.dbp_distribution is None:
            self.dbp_distribution = BPDistribution()
        if self.period is None:
            self.period = TimePeriod.TOTAL
        if self.sbp_stats is None:
            self.sbp_stats = Statistics(
                min_val=0,
                max_val=0,
                avg=0.0,
                std=0.0,
                cv=0.0,
                data=[]
            )
        if self.dbp_stats is None:
            self.dbp_stats = Statistics(
                min_val=0,
                max_val=0,
                avg=0.0,
                std=0.0,
                cv=0.0,
                data=[]
            )
        if self.mbp_stats is None:
            self.mbp_stats = Statistics(
                min_val=0,
                max_val=0,
                avg=0.0,
                std=0.0,
                cv=0.0,
                data=[]
            )
        if self.pr_stats is None:
            self.pr_stats = Statistics(
                min_val=0,
                max_val=0,
                avg=0.0,
                std=0.0,
                cv=0.0,
                data=[]
            )
        if self.pulse_pressure_stats is None:
            self.pulse_pressure_stats = Statistics(
                min_val=0,
                max_val=0,
                avg=0.0,
                std=0.0,
                cv=0.0,
                data=[]
            )

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'total_capture_rate': self.total_capture_rate,
            'day_capture_rate': self.day_capture_rate,
            'night_capture_rate': self.night_capture_rate,
            'day_measurements': self.day_measurements,
            'night_measurements': self.night_measurements,
            'expected_day_measurements': self.expected_day_measurements,
            'expected_night_measurements': self.expected_night_measurements,
            'sbp_distribution': self.sbp_distribution.to_dict(),
            'dbp_distribution': self.dbp_distribution.to_dict(),
            'period': self.period.value,
            'sbp_stats': self.sbp_stats.to_dict(),
            'dbp_stats': self.dbp_stats.to_dict(),
            'mbp_stats': self.mbp_stats.to_dict(),
            'pr_stats': self.pr_stats.to_dict(),
            'pulse_pressure': self.pulse_pressure_stats.to_dict(),
            'total_records': self.total_records,
            'above_limits_sbp': self.above_limits_sbp,
            'above_limits_dbp': self.above_limits_dbp,
            'nocturnal_fall_sbp': self.nocturnal_fall_sbp,
            'nocturnal_fall_dbp': self.nocturnal_fall_dbp
        }


class LoggerManager:
    """日志管理器"""

    @staticmethod
    def setup_logger(name: str = __name__,
                     log_file: str = None,
                     level: int = logging.INFO) -> logging.Logger:
        """
        设置日志记录器

        Args:
            name: 日志记录器名称
            log_file: 日志文件路径
            level: 日志级别

        Returns:
            配置好的日志记录器
        """
        logger = logging.getLogger(name)

        # 避免重复添加处理器
        if logger.handlers:
            return logger

        logger.setLevel(level)

        # 创建格式化器
        formatter = logging.Formatter(
            '%(asctime)s -%(name)s[line:%(lineno)d]->%(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 文件处理器（如果指定了日志文件）
        if log_file:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger


class BPDataAnalyzer:
    """血压数据分析器"""

    # 阈值定义
    DAY_SBP_THRESHOLD = 135
    DAY_DBP_THRESHOLD = 85
    NIGHT_SBP_THRESHOLD = 120
    NIGHT_DBP_THRESHOLD = 70
    TOTAL_SBP_THRESHOLD = 130
    TOTAL_DBP_THRESHOLD = 80

    def __init__(self, start_time: int, end_time: int, measurement_plan: MeasurementPlan,
                 timezone_name: str, timezone_offset: int = 0, day_start_hour: Any = 8,
                 night_start_hour: Any = 20, logger: logging.Logger = None):
        """
        初始化分析器

        Args:
            timezone_offset: 时区偏移（秒）
            logger: 日志记录器
            day_start_hour: 白天开始时间，可以是整数(8)或字符串("8:00", "8:05", "8:45")
            night_start_hour: 夜间开始时间，可以是整数(20)或字符串("20:05", "20:45", "20:30")
        """
        self.TIMEZONE_OFFSET = timezone_offset
        self.TIMEZONE_NAME = timezone_name
        self.START_TIME = start_time
        self.END_TIME = end_time
        self.MEASUREMENT_PLAN = measurement_plan
        self.logger = logger or LoggerManager.setup_logger('BPDataAnalyzer')

        # 解析白天开始时间
        self.DAY_START_HOUR, self.DAY_START_MINUTE = self._parse_time_to_hour_minute(day_start_hour)
        # 解析夜间开始时间
        self.NIGHT_START_HOUR, self.NIGHT_START_MINUTE = self._parse_time_to_hour_minute(night_start_hour)

        # 将时间转换为分钟数以便比较
        self.DAY_START_MINUTES = self.DAY_START_HOUR * 60 + self.DAY_START_MINUTE
        self.NIGHT_START_MINUTES = self.NIGHT_START_HOUR * 60 + self.NIGHT_START_MINUTE

        self.logger.info(f"初始化BPDataAnalyzer, 时区名称: {timezone_name}")
        self.logger.info(
            f"白天开始时间: {self.DAY_START_HOUR:02d}:{self.DAY_START_MINUTE:02d} ({self.DAY_START_MINUTES}分钟)")
        self.logger.info(
            f"夜间开始时间: {self.NIGHT_START_HOUR:02d}:{self.NIGHT_START_MINUTE:02d} ({self.NIGHT_START_MINUTES}分钟)")

    def _parse_time_to_hour_minute(self, time_input: Any) -> Tuple[int, int]:
        """
        将时间输入转换为小时和分钟

        Args:
            time_input: 可以是整数(8)或字符串("8:00", "8:05", "8:45")

        Returns:
            (小时, 分钟)
        """
        try:
            if isinstance(time_input, (int, float)):
                # 如果是整数或浮点数，假定是小时
                hour = int(time_input)
                minute = int((time_input - hour) * 60) if isinstance(time_input, float) else 0
                return hour, minute
            elif isinstance(time_input, str):
                # 如果是字符串，尝试解析
                time_str = time_input.strip()

                # 处理"8:00", "8:05", "8:45"等格式
                if ':' in time_str:
                    parts = time_str.split(':')
                    hour = int(parts[0])
                    minute = int(parts[1]) if len(parts) > 1 else 0
                # 处理"8.00", "8.05"等格式（如果有的话）
                elif '.' in time_str:
                    parts = time_str.split('.')
                    hour = int(parts[0])
                    minute = int(parts[1]) if len(parts) > 1 else 0
                    # 如果分钟部分小于10，可能是"8.05"表示8:05
                    if minute < 10 and len(parts[1]) > 1:
                        minute = int(parts[1].ljust(2, '0'))
                else:
                    # 纯数字字符串
                    hour = int(time_str)
                    minute = 0

                return hour, minute
            else:
                self.logger.warning(f"无法解析时间输入: {time_input}，使用默认值8:00")
                return 8, 0
        except Exception as e:
            self.logger.error(f"解析时间输入失败: {time_input}, error={str(e)}，使用默认值8:00")
            return 8, 0

    # 修改classify_day_night方法以支持分钟级别的判断
    def classify_day_night(self, timestamp_ms: int, timezone_name: str) -> TimePeriod:
        """
        根据时间戳判断是白天还是夜间

        Args:
            timestamp_ms: 毫秒时间戳

        Returns:
            TimePeriod.DAY 或 TimePeriod.NIGHT
        """
        try:
            timestamp_sec = timestamp_ms // 1000 if timestamp_ms > 1e12 else timestamp_ms
            utc_time = datetime.fromtimestamp(timestamp_sec)
            # 根据时区名转换为当地时间
            local_time = utc_time.astimezone(ZoneInfo(timezone_name))
            hour = local_time.hour
            minute = local_time.minute

            # 计算当前时间的总分钟数
            current_minutes = hour * 60 + minute

            # 白天: day_start_time到night_start_time, 夜间: night_start_time到次日day_start_time
            if self.DAY_START_MINUTES <= current_minutes < self.NIGHT_START_MINUTES:
                return TimePeriod.DAY
            else:
                return TimePeriod.NIGHT

        except Exception as e:
            self.logger.error(f"时间分类失败: timestamp={timestamp_ms}, error={str(e)}")
            return TimePeriod.DAY  # 默认返回白天

    # 计算捕获率
    def calculate_capture_rate(self, data_list: List[Measurement]) -> SummaryResult:
        """
        计算白天和夜间时间段的测量捕获率，考虑夏令时影响

        Args:
            data_list: 测量数据列表

        Returns:
            SummaryResult: 包含捕获率统计结果的数据类对象
        """
        self.logger.info("开始计算测量数据捕获率")
        self.logger.info(f"输入参数: start_timestamp={self.START_TIME}, end_timestamp={self.END_TIME}")

        self.logger.info(
            f"时区: {self.TIMEZONE_NAME}, 白天开始: {self.DAY_START_HOUR:02d}:{self.DAY_START_MINUTE:02d}, "
            f"夜间开始: {self.NIGHT_START_HOUR:02d}:{self.NIGHT_START_MINUTE:02d}")

        if not data_list:
            self.logger.warning("数据列表为空")
            return SummaryResult(
                total_records=len(data_list)
            )

        measurement_plan = self.MEASUREMENT_PLAN

        # 检查测量计划是否启用
        if not measurement_plan.enabled:
            self.logger.warning("测量计划未启用")
            return SummaryResult(
                total_records=len(data_list)
            )

        self.logger.info(f"测量计划: 开始时间={measurement_plan.start_time}, "
                         f"结束时间={measurement_plan.end_time}, "
                         f"间隔={measurement_plan.interval}分钟")

        # 转换时间戳到时区时间
        start_dt = datetime.fromtimestamp(self.START_TIME / 1000, tz=ZoneInfo(self.TIMEZONE_NAME))
        end_dt = datetime.fromtimestamp(self.END_TIME / 1000, tz=ZoneInfo(self.TIMEZONE_NAME))

        self.logger.info(f'时间范围：{start_dt} --> {end_dt}')
        self.logger.info(f"时间范围持续时间: {(self.END_TIME /1000 -self.START_TIME / 1000) /3600:.2f}小时")

        # 时区转换后的时间，夏令时会少 1 小时 计算不准
        # self.logger.info(f'时间范围持续时间: {(end_dt - start_dt).total_seconds() / 3600:.2f}小时')

        # 解析测量计划的时间
        plan_start_time = self._parse_time_str(measurement_plan.start_time)
        plan_end_time = self._parse_time_str(measurement_plan.end_time)

        # 检查是否是跨天的测量计划（结束时间在开始时间之前或相等）
        plan_is_overnight = (plan_end_time <= plan_start_time)

        if plan_is_overnight:
            self.logger.info(f"测量计划跨天: {plan_start_time} -> 第二天 {plan_end_time} (左闭右开)")
        else:
            self.logger.info(f"测量计划当天内: {plan_start_time} -> {plan_end_time} (左闭右开)")

        # 统计实际测量次数（按白天/夜间分类）
        day_actual_count = 0
        night_actual_count = 0

        # 将数据按时间戳排序
        sorted_data = sorted(data_list, key=lambda x: x.time)

        for measurement in sorted_data:
            try:
                measure_dt = datetime.fromtimestamp(measurement.time / 1000,
                                                    tz=ZoneInfo(self.TIMEZONE_NAME))
                hour = measure_dt.hour
                minute = measure_dt.minute
                current_minutes = hour * 60 + minute

                if self.DAY_START_MINUTES <= current_minutes < self.NIGHT_START_MINUTES:
                    day_actual_count += 1
                else:
                    night_actual_count += 1
            except Exception as e:
                self.logger.error(f"处理测量数据时出错: {e}")
                continue

        self.logger.info(f"实际测量统计: 白天={day_actual_count}, 夜间={night_actual_count}")

        # 计算计划测量次数
        day_planned_count = 0
        night_planned_count = 0

        # 使用日期对象进行比较
        current_date = start_dt.date()
        end_date = end_dt.date()

        self.logger.info(f"日期范围: {current_date} 到 {end_date}")

        # 循环处理每一天
        day_index = 1
        tz = ZoneInfo(self.TIMEZONE_NAME)

        # 创建一个辅助函数来打印测量点
        def log_measurement_point(dt: datetime, period: str):
            """打印测量点信息，包含夏令时状态"""
            time_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            tz_offset = dt.strftime('%z')
            is_dst = dt.dst()
            dst_status = "夏令时" if is_dst and is_dst.total_seconds() > 0 else "标准时间"
            self.logger.info(f"测量计划时间点: {time_str}{tz_offset} ---- {period} ---- {dst_status}")

        # 对于跨天测量计划，需要特殊处理
        while current_date <= end_date:
            self.logger.info(f"处理第 {day_index} 天: {current_date}")

            if plan_is_overnight:
                # 跨天测量计划分为两部分：
                # 1. 当天00:00到plan_end_time（属于前一天的测量计划）
                if plan_end_time != time(0, 0, 0):
                    period1_start = datetime.combine(current_date, time(0, 0, 0), tzinfo=tz)
                    period1_end = datetime.combine(current_date, plan_end_time, tzinfo=tz)

                    # 计算与时间范围的交集
                    actual_start1 = max(period1_start, start_dt)
                    actual_end1 = min(period1_end, end_dt)

                    if actual_start1 < actual_end1:
                        # 使用UTC时间进行计算，以确保正确处理夏令时转换
                        current_utc_time = actual_start1.astimezone(ZoneInfo('UTC'))
                        end_utc_time = actual_end1.astimezone(ZoneInfo('UTC'))

                        # 计算持续时间（秒）
                        duration_seconds = (end_utc_time - current_utc_time).total_seconds()
                        total_minutes = int(duration_seconds / 60)

                        # 按照间隔计算所有测量点
                        for i in range(0, total_minutes + 1, measurement_plan.interval):
                            if i > total_minutes:
                                break

                            # 计算当前UTC时间
                            current_measurement_utc = actual_start1.astimezone(ZoneInfo('UTC')) + timedelta(minutes=i)

                            # 转换回本地时间
                            current_measurement_time = current_measurement_utc.astimezone(tz)

                            # 确保时间在范围内
                            if current_measurement_time >= actual_start1 and current_measurement_time < actual_end1:
                                hour = current_measurement_time.hour
                                minute = current_measurement_time.minute
                                current_minutes = hour * 60 + minute

                                if self.DAY_START_MINUTES <= current_minutes < self.NIGHT_START_MINUTES:
                                    day_planned_count += 1
                                    log_measurement_point(current_measurement_time, "白天")
                                else:
                                    night_planned_count += 1
                                    log_measurement_point(current_measurement_time, "夜间")

                # 第二部分：当天plan_start_time到23:59:59.999999
                period2_start = datetime.combine(current_date, plan_start_time, tzinfo=tz)
                period2_end = datetime.combine(current_date, time(23, 59, 59, 999999), tzinfo=tz)

                # 计算与时间范围的交集
                actual_start2 = max(period2_start, start_dt)
                actual_end2 = min(period2_end, end_dt)

                if actual_start2 < actual_end2:
                    # 使用UTC时间进行计算
                    current_utc_time = actual_start2.astimezone(ZoneInfo('UTC'))
                    end_utc_time = actual_end2.astimezone(ZoneInfo('UTC'))

                    # 计算持续时间（秒）
                    duration_seconds = (end_utc_time - current_utc_time).total_seconds()
                    total_minutes = int(duration_seconds / 60)

                    # 按照间隔计算所有测量点
                    for i in range(0, total_minutes + 1, measurement_plan.interval):
                        if i > total_minutes:
                            break

                        # 计算当前UTC时间
                        current_measurement_utc = actual_start2.astimezone(ZoneInfo('UTC')) + timedelta(minutes=i)

                        # 转换回本地时间
                        current_measurement_time = current_measurement_utc.astimezone(tz)

                        # 确保时间在范围内
                        if current_measurement_time >= actual_start2 and current_measurement_time < actual_end2:
                            hour = current_measurement_time.hour
                            minute = current_measurement_time.minute
                            current_minutes = hour * 60 + minute

                            if self.DAY_START_MINUTES <= current_minutes < self.NIGHT_START_MINUTES:
                                day_planned_count += 1
                                log_measurement_point(current_measurement_time, "白天")
                            else:
                                night_planned_count += 1
                                log_measurement_point(current_measurement_time, "夜间")
            else:
                # 当天内的测量计划（左闭右开）
                plan_start_dt = datetime.combine(current_date, plan_start_time, tzinfo=tz)
                plan_end_dt = datetime.combine(current_date, plan_end_time, tzinfo=tz)

                # 计算与时间范围的交集
                actual_start = max(plan_start_dt, start_dt)
                actual_end = min(plan_end_dt, end_dt)

                if actual_start < actual_end:
                    # 使用UTC时间进行计算
                    current_utc_time = actual_start.astimezone(ZoneInfo('UTC'))
                    end_utc_time = actual_end.astimezone(ZoneInfo('UTC'))

                    # 计算持续时间（秒）
                    duration_seconds = (end_utc_time - current_utc_time).total_seconds()
                    total_minutes = int(duration_seconds / 60)

                    # 按照间隔计算所有测量点
                    for i in range(0, total_minutes + 1, measurement_plan.interval):
                        if i > total_minutes:
                            break

                        # 计算当前UTC时间
                        current_measurement_utc = actual_start.astimezone(ZoneInfo('UTC')) + timedelta(minutes=i)

                        # 转换回本地时间
                        current_measurement_time = current_measurement_utc.astimezone(tz)

                        # 确保时间在范围内
                        if current_measurement_time >= actual_start and current_measurement_time < actual_end:
                            hour = current_measurement_time.hour
                            minute = current_measurement_time.minute
                            current_minutes = hour * 60 + minute

                            if self.DAY_START_MINUTES <= current_minutes < self.NIGHT_START_MINUTES:
                                day_planned_count += 1
                                log_measurement_point(current_measurement_time, "白天")
                            else:
                                night_planned_count += 1
                                log_measurement_point(current_measurement_time, "夜间")

            # 下一天
            current_date += timedelta(days=1)
            day_index += 1

        self.logger.info(f"计划测量统计: 白天={day_planned_count}, 夜间={night_planned_count}")

        # 计算总计划测量次数
        total_planned_count = day_planned_count + night_planned_count

        # 计算捕获率
        total_actual_count = day_actual_count + night_actual_count

        # 添加详细的调试信息
        self.logger.info(f"实际测量总数: {total_actual_count}, 计划测量总数: {total_planned_count}")

        total_capture_rate = total_actual_count / total_planned_count if total_planned_count > 0 else 0
        day_capture_rate = day_actual_count / day_planned_count if day_planned_count > 0 else 0
        night_capture_rate = night_actual_count / night_planned_count if night_planned_count > 0 else 0

        result = SummaryResult(
            total_capture_rate=round(total_capture_rate, 4),
            day_capture_rate=round(day_capture_rate, 4),
            night_capture_rate=round(night_capture_rate, 4),
            day_measurements=day_actual_count,
            night_measurements=night_actual_count,
            expected_day_measurements=day_planned_count,
            expected_night_measurements=night_planned_count,
            total_records=len(data_list)
        )

        # 调试信息
        self.logger.info(f"计算结果: 总捕获率={result.total_capture_rate}, "
                         f"白天捕获率={result.day_capture_rate}, "
                         f"夜间捕获率={result.night_capture_rate}")

        return result

    # 计算 QC
    def calculate_qc_result(self, analyzer, data_list: List[Measurement], timezone_name: str,
                            capture_rate_reslut: SummaryResult):
        day_data = []
        night_data = []

        for measurement in data_list:
            if analyzer.classify_day_night(measurement.time, timezone_name) == TimePeriod.DAY:
                day_data.append(measurement)
            else:
                night_data.append(measurement)
        if len(day_data) >= 20 and len(night_data) >= 7 and capture_rate_reslut.total_capture_rate >= 0.7:
            qc_result = [{"result": "pass", "day": len(day_data), "Night": len(night_data),
                          "total": capture_rate_reslut.total_capture_rate}]
        elif len(day_data) < 20 or len(night_data) < 7 or capture_rate_reslut.total_capture_rate < 0.7:
            qc_result = [{"result": "fail", "day": len(day_data), "Night": len(night_data),
                          "total": capture_rate_reslut.total_capture_rate}]
        else:
            qc_result = [{"result": "fail", "day": 0, "Night": 0, "total": 0}]
        return SummaryResult(
            qc_results=qc_result,
        )

    def calculate_nocturnal_fall(self, day_result: SummaryResult,
                                 night_result: SummaryResult) -> Tuple[float, float]:
        """
        计算夜间血压下降比值

        Args:
            day_result: 白天分析结果
            night_result: 夜间分析结果

        Returns:
            (SBP下降比值, DBP下降比值)
        """
        try:
            if day_result.sbp_stats.avg == 0:
                nocturnal_fall_sbp = 0
                self.logger.warning("白天SBP平均值为0，无法计算夜间下降比值")
            else:
                nocturnal_fall_sbp = (
                                             day_result.sbp_stats.avg - night_result.sbp_stats.avg) / day_result.sbp_stats.avg * 100


            if day_result.dbp_stats.avg == 0:
                nocturnal_fall_dbp = 0
                self.logger.warning("白天DBP平均值为0，无法计算夜间下降比值")
            else:
                nocturnal_fall_dbp = (
                                             day_result.dbp_stats.avg - night_result.dbp_stats.avg) / day_result.dbp_stats.avg * 100

            self.logger.debug(f"夜间血压下降比值: SBP={nocturnal_fall_sbp:.1f}%, DBP={nocturnal_fall_dbp:.1f}%")
            return nocturnal_fall_sbp, nocturnal_fall_dbp

        except Exception as e:
            self.logger.error(f"计算夜间血压下降比值失败: error={str(e)}")
            return 0, 0

    # 时间戳转换
    def convert_timestamp_to_local(self, timestamp_ms: int, include_date: bool = False) -> str:
        """
        将时间戳转换为本地时间字符串

        Args:
            timestamp_ms: 毫秒时间戳
            include_date: 是否包含日期

        Returns:
            格式化时间字符串
        """
        try:
            # 转换为秒
            timestamp_sec = timestamp_ms // 1000 if timestamp_ms > 1e12 else timestamp_ms

            # 应用时区偏移
            utc_time = datetime.fromtimestamp(timestamp_sec, tz=ZoneInfo("UTC"))
            # 根据时区名转换为当地时间
            local_time = utc_time.astimezone(ZoneInfo(self.TIMEZONE_NAME))

            # 格式化
            if include_date:
                return local_time.strftime("%m/%d/%y %I:%M %p").lower()
            else:
                return local_time.strftime("%I:%M %p").lower()

        except Exception as e:
            self.logger.error(f"时间转换失败: timestamp={timestamp_ms}, error={str(e)}")
            return "时间转换错误"

    def _parse_time_str(self, time_str: str) -> datetime.time:
        """解析时间字符串，支持12小时和24小时格式"""
        try:
            # 移除空格和转换为小写
            time_str = time_str.strip().lower()

            # 处理12小时制
            if 'am' in time_str or 'pm' in time_str:
                # 移除am/pm
                time_part = time_str.replace('am', '').replace('pm', '').strip()
                hour, minute = map(int, time_part.split(':'))

                # 调整小时
                if 'pm' in time_str and hour != 12:
                    hour += 12
                elif 'am' in time_str and hour == 12:
                    hour = 0

                return time(hour, minute)
            else:
                # 24小时制
                hour, minute = map(int, time_str.split(':'))
                return time(hour, minute)
        except Exception as e:
            self.logger.error(f"解析时间字符串失败: {time_str}, error={str(e)}")
            return time(12, 0)  # 默认返回中午12点

    def analyze_period(self, data_list: List[Measurement],
                       period: TimePeriod) -> SummaryResult:
        """
        分析特定时间段的数据

        Args:
            data_list: 测量数据列表
            period: 时间段类型

        Returns:
            分析结果
        """
        self.logger.info(f"开始分析{period.value}时间段数据, 数据量={len(data_list)}")

        if not data_list:
            self.logger.warning(f"{period.value}数据列表为空")
            raise ValueError(f"{period.value}数据列表不能为空")

        try:
            # 计算各项统计指标
            sbp_stats = self.calculate_statistics(data_list, 'sbp')
            dbp_stats = self.calculate_statistics(data_list, 'dbp')
            mbp_stats = self.calculate_statistics(data_list, 'mbp')
            pr_stats = self.calculate_statistics(data_list, 'pr')
            pulse_pressure_stats = self.calculate_statistics(data_list, 'pulse_pressure')
            sbp_distribution = self.calculate_bp_distribution(data_list, 'sbp')
            dbp_distribution = self.calculate_bp_distribution(data_list, 'dbp')

            result = SummaryResult(
                sbp_distribution=sbp_distribution,
                dbp_distribution=dbp_distribution,
                period=period,
                sbp_stats=sbp_stats,
                dbp_stats=dbp_stats,
                mbp_stats=mbp_stats,
                pr_stats=pr_stats,
                pulse_pressure_stats=pulse_pressure_stats,
                total_records=len(data_list)
            )

            # 根据时间段计算血压负荷
            if period == TimePeriod.DAY:
                # 白天高于阈值的比例
                above_sbp = sum(1 for x in data_list if x.sbp > self.DAY_SBP_THRESHOLD)
                above_dbp = sum(1 for x in data_list if x.dbp > self.DAY_DBP_THRESHOLD)
                result.above_limits_sbp = above_sbp / len(data_list) * 100
                result.above_limits_dbp = above_dbp / len(data_list) * 100
                self.logger.debug(
                    f"白天阈值超标: SBP={result.above_limits_sbp:.1f}%, DBP={result.above_limits_dbp:.1f}%")

            elif period == TimePeriod.NIGHT:
                # 夜间高于阈值的比例
                above_sbp = sum(1 for x in data_list if x.sbp > self.NIGHT_SBP_THRESHOLD)
                above_dbp = sum(1 for x in data_list if x.dbp > self.NIGHT_DBP_THRESHOLD)
                result.above_limits_sbp = above_sbp / len(data_list) * 100
                result.above_limits_dbp = above_dbp / len(data_list) * 100
                self.logger.debug(
                    f"夜间阈值超标: SBP={result.above_limits_sbp:.1f}%, DBP={result.above_limits_dbp:.1f}%")
            elif period == TimePeriod.TOTAL:
                # 全部阈值超标的比例
                """
                BP Load做全天计算时（按 SBP/DBP 分别统计）

                分子：# of readings with SBP ≥ SBP_thr (by day) + # of readings with SBP ≥ SBP_thr (by night)

                分母：# of valid BP readings in 24h

                """
                day_above_sbp = sum(1 for x in data_list if
                                    x.sbp >= self.DAY_SBP_THRESHOLD and self.classify_day_night(x.time,
                                                                                                self.TIMEZONE_NAME) == TimePeriod.DAY)
                day_above_dbp = sum(1 for x in data_list if
                                    x.dbp >= self.DAY_DBP_THRESHOLD and self.classify_day_night(x.time,
                                                                                                self.TIMEZONE_NAME) == TimePeriod.DAY)
                night_above_sbp = sum(1 for x in data_list if
                                      x.sbp >= self.NIGHT_SBP_THRESHOLD and self.classify_day_night(x.time,
                                                                                                    self.TIMEZONE_NAME) == TimePeriod.NIGHT)
                night_above_dbp = sum(1 for x in data_list if
                                      x.dbp >= self.NIGHT_DBP_THRESHOLD and self.classify_day_night(x.time,
                                                                                                    self.TIMEZONE_NAME) == TimePeriod.NIGHT)
                result.above_limits_sbp = (day_above_sbp + night_above_sbp) / len(data_list) * 100
                result.above_limits_dbp = (day_above_dbp + night_above_dbp) / len(data_list) * 100
                self.logger.debug(
                    f"全部阈值超标: SBP={result.above_limits_sbp:.1f}%, DBP={result.above_limits_dbp:.1f}%")

            self.logger.info(f"{period.value}时间段分析完成, 总记录数={result.total_records}")
            return result

        except Exception as e:
            self.logger.error(f"{period.value}时间段分析失败: error={str(e)}")
            raise

    # 计算基本统计指标
    def calculate_statistics(self, data_list: List[Measurement],
                             value_field: str) -> Statistics:
        """
        计算基本统计指标

        Args:
            data_list: 测量数据列表
            value_field: 要统计的字段名 ('sbp', 'dbp', 'mbp', 'pr', "pulse_pressure")

        Returns:
            统计结果
        """
        self.logger.debug(f"开始计算统计指标: value_field={value_field}, 数据量={len(data_list)}")

        if not data_list:
            self.logger.warning("数据列表为空，无法计算统计指标")
            raise ValueError("数据列表不能为空")

        try:
            # 提取数据
            values = []
            for item in data_list:
                if value_field == 'sbp':
                    values.append(item.sbp)
                elif value_field == 'dbp':
                    values.append(item.dbp)
                elif value_field == 'mbp':
                    values.append(item.mbp)
                elif value_field == 'pr':
                    values.append(item.pr)
                elif value_field == 'pulse_pressure':
                    values.append(item.pulse_pressure)
            # 计算统计值
            avg = np.mean(values)
            std = np.std(values, ddof=1)  # 样本标准差

            # 找到最小值和最大值及其时间
            if value_field in ['sbp', 'dbp', 'pr', 'pulse_pressure']:

                # 处理最小值中x小于等于 0 的值
                values = [x for x in values if x > 0]

                # 处理最小值
                min_value = min(values)
                # 找出所有具有最小值的项目
                min_items = [item for item in data_list if getattr(item, value_field) == min_value]
                # 在项目中找出时间最新的（最大的时间戳）
                latest_min_item = max(min_items, key=lambda x: x.time)

                # 处理最大值
                max_value = max(values)
                max_items = [item for item in data_list if getattr(item, value_field) == max_value]
                # 在项目中找出时间最新的（最大的时间戳）
                latest_max_item = max(max_items, key=lambda x: x.time)

                min_time = latest_min_item.time
                max_time = latest_max_item.time
                min_val = getattr(latest_min_item, value_field)
                max_val = getattr(latest_max_item, value_field)
            else:
                # 处理其他情况
                min_val = min(values)
                max_val = max(values)
                min_time = None
                max_time = None

            # 计算变异系数(CV)
            cv = (std / avg * 100) if avg != 0 else 0

            stats = Statistics(
                min_val=min_val,
                max_val=max_val,
                avg=avg,
                std=std,
                cv=cv,
                data=values,
                min_time=min_time,
                max_time=max_time
            )

            self.logger.debug(f"统计指标计算完成: {value_field}={stats.to_dict()}")
            return stats

        except Exception as e:
            self.logger.error(f"统计计算失败: value_field={value_field}, error={str(e)}")
            raise

    # 计算血压分布
    def calculate_bp_distribution(self, data_list: List[Measurement], value_field: str) -> BPDistribution:
        """
        计算血压分布

        Args:
            data_list: 测量数据列表

        Returns:
            血压分布结果
        """
        self.logger.debug(f"开始计算血压分布: value_field={value_field}, 数据量={len(data_list)}")
        if not data_list:
            self.logger.warning("数据列表为空，无法计血压分布指标")
            raise ValueError("数据列表不能为空")

        try:
            # 提取数据
            values = []
            for item in data_list:
                if value_field == 'sbp':
                    values.append(item.sbp)
                elif value_field == 'dbp':
                    values.append(item.dbp)
            total_records = len(data_list)

            """
            BP Distribution(血压分布)
                SBP(收缩压)：显示收缩压在阈值范围内和范围外的百分比，范围内部分以绿色表示，范围外部分以橙色表示
                DBP(舒张压)：显示舒张压在阈值范围内和范围外的百分比，范围内部分以绿色表示，范围外部分以橙色表示
            """
            if value_field == 'sbp':
                # 计算sbp大于等于阈值的数量
                sbp_above_threshold = sum(1 for item in data_list if item.sbp >= self.TOTAL_SBP_THRESHOLD)
                sbp_below_threshold = total_records - sbp_above_threshold
                # [阈值内,超出阈值]
                bp_distribution = [round(sbp_below_threshold / total_records * 100, 1),
                                   round(sbp_above_threshold / total_records * 100, 1)]

            elif value_field == 'dbp':
                # 计算dbp大于等于阈值的数量
                dbp_above_threshold = sum(1 for item in data_list if item.dbp >= self.TOTAL_DBP_THRESHOLD)
                dbp_below_threshold = total_records - dbp_above_threshold
                # [阈值内,超出阈值]
                bp_distribution = [round(dbp_below_threshold / total_records * 100, 1),
                                   round(dbp_above_threshold / total_records * 100, 1)]
                self.logger.info(f"血压分布计算完成: {value_field}={bp_distribution}")
            else:
                raise ValueError(f"value_field参数错误: {value_field}")

            return BPDistribution(
                bp_distribution=bp_distribution,
            )
        except Exception as e:
            self.logger.error(f"统计计算失败: value_field={value_field}, error={str(e)}")
            raise

    def analyze_period(self, data_list: List[Measurement],
                       period: TimePeriod) -> SummaryResult:
        """
        分析特定时间段的数据

        Args:
            data_list: 测量数据列表
            period: 时间段类型

        Returns:
            分析结果
        """
        self.logger.info(f"开始分析{period.value}时间段数据, 数据量={len(data_list)}")

        if not data_list:
            self.logger.warning(f"{period.value}数据列表为空")
            raise ValueError(f"{period.value}数据列表不能为空")

        try:
            # 计算各项统计指标
            sbp_stats = self.calculate_statistics(data_list, 'sbp')
            dbp_stats = self.calculate_statistics(data_list, 'dbp')
            mbp_stats = self.calculate_statistics(data_list, 'mbp')
            pr_stats = self.calculate_statistics(data_list, 'pr')
            pulse_pressure_stats = self.calculate_statistics(data_list, 'pulse_pressure')
            sbp_distribution = self.calculate_bp_distribution(data_list, 'sbp')
            dbp_distribution = self.calculate_bp_distribution(data_list, 'dbp')

            result = SummaryResult(
                sbp_distribution=sbp_distribution,
                dbp_distribution=dbp_distribution,
                period=period,
                sbp_stats=sbp_stats,
                dbp_stats=dbp_stats,
                mbp_stats=mbp_stats,
                pr_stats=pr_stats,
                pulse_pressure_stats=pulse_pressure_stats,
                total_records=len(data_list)
            )

            # 根据时间段计算血压负荷
            if period == TimePeriod.DAY:
                # 白天高于阈值的比例
                above_sbp = sum(1 for x in data_list if x.sbp > self.DAY_SBP_THRESHOLD)
                above_dbp = sum(1 for x in data_list if x.dbp > self.DAY_DBP_THRESHOLD)
                result.above_limits_sbp = above_sbp / len(data_list) * 100
                result.above_limits_dbp = above_dbp / len(data_list) * 100
                self.logger.debug(
                    f"白天阈值超标: SBP={result.above_limits_sbp:.1f}%, DBP={result.above_limits_dbp:.1f}%")

            elif period == TimePeriod.NIGHT:
                # 夜间高于阈值的比例
                above_sbp = sum(1 for x in data_list if x.sbp > self.NIGHT_SBP_THRESHOLD)
                above_dbp = sum(1 for x in data_list if x.dbp > self.NIGHT_DBP_THRESHOLD)
                result.above_limits_sbp = above_sbp / len(data_list) * 100
                result.above_limits_dbp = above_dbp / len(data_list) * 100
                self.logger.debug(
                    f"夜间阈值超标: SBP={result.above_limits_sbp:.1f}%, DBP={result.above_limits_dbp:.1f}%")
            elif period == TimePeriod.TOTAL:
                # 全部阈值超标的比例
                """
                BP Load做全天计算时（按 SBP/DBP 分别统计）

                分子：# of readings with SBP ≥ SBP_thr (by day) + # of readings with SBP ≥ SBP_thr (by night)

                分母：# of valid BP readings in 24h

                """
                day_above_sbp = sum(1 for x in data_list if
                                    x.sbp >= self.DAY_SBP_THRESHOLD and self.classify_day_night(x.time,
                                                                                                self.TIMEZONE_NAME) == TimePeriod.DAY)
                day_above_dbp = sum(1 for x in data_list if
                                    x.dbp >= self.DAY_DBP_THRESHOLD and self.classify_day_night(x.time,
                                                                                                self.TIMEZONE_NAME) == TimePeriod.DAY)
                night_above_sbp = sum(1 for x in data_list if
                                      x.sbp >= self.NIGHT_SBP_THRESHOLD and self.classify_day_night(x.time,
                                                                                                    self.TIMEZONE_NAME) == TimePeriod.NIGHT)
                night_above_dbp = sum(1 for x in data_list if
                                      x.dbp >= self.NIGHT_DBP_THRESHOLD and self.classify_day_night(x.time,
                                                                                                    self.TIMEZONE_NAME) == TimePeriod.NIGHT)
                result.above_limits_sbp = (day_above_sbp + night_above_sbp) / len(data_list) * 100
                result.above_limits_dbp = (day_above_dbp + night_above_dbp) / len(data_list) * 100
                self.logger.debug(
                    f"全部阈值超标: SBP={result.above_limits_sbp:.1f}%, DBP={result.above_limits_dbp:.1f}%")

            self.logger.info(f"{period.value}时间段分析完成, 总记录数={result.total_records}")
            return result

        except Exception as e:
            self.logger.error(f"{period.value}时间段分析失败: error={str(e)}")
            raise
    # 打印结果
    def log_summary(self, result: SummaryResult):
        """记录分析结果到日志"""
        # period_name = result.period.value.capitalize()

        # 根据不同的用途显示不同的标题
        if result.qc_results and result.total_records == 0:  # 这是QC结果
            title = "QC Summary"
        elif result.total_records == 0:  # 这是捕获率结果
            title = "Capture Rate Summary"
        else:
            period_name = result.period.value.capitalize()
            title = f"{period_name} Summary"

        self.logger.info(f"{'=' * 80}")
        self.logger.info(f"{title} Summary")
        self.logger.info(f"{'=' * 80}")

        # 格式化时间显示
        def format_time(timestamp):
            return self.convert_timestamp_to_local(timestamp) if timestamp else "N/A"

        """
            计算逻辑说明
        """
        # SBP
        self.logger.info(f"SBP  Min: {result.sbp_stats.min_val} ({format_time(result.sbp_stats.min_time)})")
        self.logger.info(f"     Max: {result.sbp_stats.max_val} ({format_time(result.sbp_stats.max_time)})")
        self.logger.info(
            f"     Average: {Decimal(result.sbp_stats.avg).quantize(Decimal('0'), rounding=decimal.ROUND_HALF_UP)}")
        self.logger.info(f"     Std.Dev.: {result.sbp_stats.std:.1f}")
        self.logger.info(f"     CV: {result.sbp_stats.cv:.2f}")

        # DBP
        self.logger.info(f"DBP  Min: {result.dbp_stats.min_val} ({format_time(result.dbp_stats.min_time)})")
        self.logger.info(f"     Max: {result.dbp_stats.max_val} ({format_time(result.dbp_stats.max_time)})")
        self.logger.info(
            f"     Average: {Decimal(result.dbp_stats.avg).quantize(Decimal('0'), rounding=decimal.ROUND_HALF_UP)}")
        self.logger.info(f"     Std.Dev.: {result.dbp_stats.std:.1f}")
        self.logger.info(f"     CV: {result.dbp_stats.cv:.2f}")

        # MBP
        self.logger.info(f"MBP  Min: {round(result.mbp_stats.min_val)}")

        self.logger.info(f"     Max: {round(result.mbp_stats.max_val)}")
        self.logger.info(
            f"     Average: {Decimal(result.mbp_stats.avg).quantize(Decimal('0'), rounding=decimal.ROUND_HALF_UP)}")
        self.logger.info(f"     Std.Dev.: {result.mbp_stats.std:.1f}")
        self.logger.info(f"     CV: {result.mbp_stats.cv:.2f}")

        # Pulse Pressure(BPM)
        self.logger.info(
            # 2026年01月20日 14:21 产品需求，BPM改为mmHg
            f"Pulse Pressure(mmHg)  Min: {result.pulse_pressure_stats.min_val} ({format_time(result.pulse_pressure_stats.min_time)})")
        self.logger.info(
            f"                     Max: {result.pulse_pressure_stats.max_val} ({format_time(result.pulse_pressure_stats.max_time)})")
        self.logger.info(
            f"                     Average: {Decimal(result.pulse_pressure_stats.avg).quantize(Decimal('0'), rounding=decimal.ROUND_HALF_UP)}")
        self.logger.info(f"                     Std.Dev.: {result.pulse_pressure_stats.std:.1f}")
        self.logger.info(f"                     CV: {result.pulse_pressure_stats.cv:.2f}")

        # Pulse Rate
        self.logger.info(f"Pulse Rate(BPM)  Min: {result.pr_stats.min_val} ({format_time(result.pr_stats.min_time)})")
        self.logger.info(f"                 Max: {result.pr_stats.max_val} ({format_time(result.pr_stats.max_time)})")
        self.logger.info(
            f"                 Average: {Decimal(result.pr_stats.avg).quantize(Decimal('0'), rounding=decimal.ROUND_HALF_UP)}")
        self.logger.info(f"                 Std.Dev.: {result.pr_stats.std:.1f}")
        self.logger.info(f"                 CV: {result.pr_stats.cv:.2f}")

        # 血压分布
        self.logger.info(f"Blood Pressure Distribution")
        self.logger.info(
            f"    SBP 💚{result.sbp_distribution.bp_distribution[0]} % /🔶{result.sbp_distribution.bp_distribution[1]} %")
        self.logger.info(
            f"    DBP 💚{result.dbp_distribution.bp_distribution[0]} % /🔶{result.dbp_distribution.bp_distribution[1]} %")

        # 捕获率指标
        self.logger.info(f"Total Capture Rate: {result.total_capture_rate * 100:.2f}% （{result.total_records} of {result.expected_day_measurements + result.expected_night_measurements}）")
        self.logger.info(f"Day   Capture Rate: {result.day_capture_rate * 100:.2f}% （{result.day_measurements} of {result.expected_night_measurements}）")
        self.logger.info(f"Night Capture Rate: {result.night_capture_rate * 100:.2f}% （{result.night_measurements} of {result.expected_day_measurements}）")

        # QC指标
        self.logger.info(f"QC Indicators")
        if result.qc_results:
            self.logger.info(f"    QC Result: {result.qc_results}")
            # self.logger.info(f"    QC Result: {result.qc_results[0].get("result")}")
        else:
            self.logger.info(f"    QC Result:  N/A")

        # 记录总数
        self.logger.info(f"Total Records: {result.total_records}")

        # 特定指标
        if result.period == TimePeriod.DAY:
            threshold_sbp = f"SBP > {self.DAY_SBP_THRESHOLD} mmHg"
            threshold_dbp = f"DBP > {self.DAY_DBP_THRESHOLD} mmHg"
            if result.above_limits_sbp is not None:
                above_limit = ">25%" if result.above_limits_sbp > 25 else "<25%"
                self.logger.info(f"{threshold_sbp}: {result.above_limits_sbp:.1f}% {above_limit}")
            if result.above_limits_dbp is not None:
                above_limit = ">25%" if result.above_limits_dbp > 25 else "<25%"
                self.logger.info(f"{threshold_dbp}: {result.above_limits_dbp:.1f}% {above_limit}")

        elif result.period == TimePeriod.NIGHT:
            threshold_sbp = f"SBP > {self.NIGHT_SBP_THRESHOLD} mmHg"
            threshold_dbp = f"DBP > {self.NIGHT_DBP_THRESHOLD} mmHg"
            if result.above_limits_sbp is not None:
                above_limit = "≥25%" if result.above_limits_sbp >= 25 else "<25%"
                self.logger.info(f"{threshold_sbp}: {result.above_limits_sbp:.1f}% {above_limit}")
            if result.above_limits_dbp is not None:
                above_limit = "≥25%" if result.above_limits_dbp >= 25 else "<25%"
                self.logger.info(f"{threshold_dbp}: {result.above_limits_dbp:.1f}% {above_limit}")
        else:
            if result.above_limits_sbp is not None:
                self.logger.info(f"Above limits(SBP): {result.above_limits_sbp:.1f}%")
            if result.above_limits_dbp is not None:
                self.logger.info(f"Above limits(DBP): {result.above_limits_dbp:.1f}%")


# 数据提取器（BP 原始数据和病人测试计划）
class DataFetcher:
    """数据获取器"""

    def __init__(self, auth_url: str, data_url: str, logger: logging.Logger = None):
        self.auth_url = auth_url
        self.data_url = data_url
        self.token = None
        self.logger = logger or LoggerManager.setup_logger('DataFetcher')

    def authenticate(self, auth_id: str, auth_key: str) -> bool:
        """身份认证"""
        self.logger.info(f"开始身份认证: auth_url={self.auth_url}")

        try:
            payload = {"id": auth_id, "key": auth_key}
            response = requests.post(self.auth_url, json=payload, timeout=30)

            if response.status_code == 200:
                self.token = response.json()["data"].get('token')
                self.logger.info("身份认证成功")
                return True
            else:
                self.logger.error(f"认证失败: status_code={response.status_code}, response={response.text}")
                return False

        except requests.exceptions.Timeout:
            self.logger.error("认证请求超时")
            return False
        except requests.exceptions.ConnectionError:
            self.logger.error("认证连接错误")
            return False
        except Exception as e:
            self.logger.error(f"认证请求异常: error={str(e)}")
            return False

    def fetch_data(self, tenants: str, subject_id: str, data_type: DataType,
                   start_time: int, end_time: int) -> List[Dict]:
        """获取数据"""
        self.logger.info(f"开始获取数据: tenants={tenants}, subject_id={subject_id}, "
                         f"data_type={data_type.value}, start_time={start_time}, end_time={end_time}")

        if not self.token:
            self.logger.error("未认证，请先调用authenticate方法")
            raise ValueError("未认证，请先调用authenticate方法")

        try:
            headers = {
                'Authorization': self.token,
                'Content-Type': 'application/json'
            }

            params = {
                'subjectId': subject_id,
                'type': data_type.value,
                'startTime': start_time,
                'endTime': end_time
            }

            url = f"{self.data_url}/tenants/{tenants}/data"
            self.logger.debug(f"请求URL: {url}, 参数: {params}")

            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=(30, 120),  # 设置超时时间为30秒,读取超时设置为120秒
                stream=True  # 设置流式读取
            )

            if response.status_code == 200:
                data = response.json()

                self.logger.info(f"数据获取成功, 记录数: {len(data.get("data").get('list'))}")
                return data
            else:
                self.logger.error(f"数据获取失败: status_code={response.status_code}, response={response.text}")
                return []

        except requests.exceptions.Timeout:
            self.logger.error("数据请求超时")
            return []
        except requests.exceptions.ConnectionError:
            self.logger.error("数据连接错误")
            return []
        except Exception as e:
            self.logger.error(f"数据请求异常: error={str(e)}")
            return []

    # 获取Webportal授权
    def authenticate_webportal(self, email: str, password: str) -> bool:
        """身份认证"""
        self.logger.info(f"开始WebPortal身份认证: auth_url={self.auth_url}")

        try:

            headers = {
                'Authorization': self.token,
                'Content-Type': 'application/json'
            }
            payload = {"email": email, "password": password}
            response = requests.post(self.auth_url, headers=headers, json=payload, timeout=30)

            if response.status_code == 200:
                self.token = response.json().get('accessToken')
                self.logger.info("身份认证成功")
                return True
            else:
                self.logger.error(f"认证失败: status_code={response.status_code}, response={response.text}")
                return False

        except requests.exceptions.Timeout:
            self.logger.error("认证请求超时")
            return False
        except requests.exceptions.ConnectionError:
            self.logger.error("认证连接错误")
            return False
        except Exception as e:
            self.logger.error(f"认证请求异常: error={str(e)}")
            return False

    # 从Webportal获取用户最新测量计划
    def fetch_measurement_plan(self, patientid):
        """获取计划数据"""
        self.logger.info(f"开始获取计划数据: patientid={patientid}")

        if not self.token:
            self.logger.error("未认证，请先调用authenticate_webportal方法")
            raise ValueError("未认证，请先调用authenticate_webportal方法")

        try:
            headers = {
                'Authorization': self.token,
                'Content-Type': 'application/json'
            }

            params = {
                'patientId': patientid,
            }
            # https://webportal-dev.vivalink.com/api/backend/abpm/plan?patientId=695db80024cd6c753484f95c
            url = f"{self.data_url}/api/backend/abpm/plan"
            self.logger.debug(f"请求URL: {url}, 参数: {params}")

            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=(30, 120),  # 设置超时时间为30秒,读取超时设置为120秒
                stream=True  # 设置流式读取
            )

            if response.status_code == 200:
                data = response.json()
                # 测量计划为 24 小时制
                self.logger.info(f"数据获取成功，测量计划: {data.get("data")}")
                return data
            else:
                self.logger.error(f"数据获取失败: status_code={response.status_code}, response={response.text}")
                return []

        except requests.exceptions.Timeout:
            self.logger.error("数据请求超时")
            return []
        except requests.exceptions.ConnectionError:
            self.logger.error("数据连接错误")
            return []
        except Exception as e:
            self.logger.error(f"数据请求异常: error={str(e)}")
            return []


def parse_raw_data(raw_data: List[Dict], logger: logging.Logger = None) -> List[Measurement]:
    """解析原始数据为Measurement对象列表"""
    logger = logger or LoggerManager.setup_logger('DataParser')
    logger.info(f"开始解析原始数据, 记录数: {len(raw_data)}")

    measurements = []
    measurements_raw = []
    error_count = 0
    # 原始数据不去除0 值
    for i, item in enumerate(raw_data):
        try:
            # 根据实际API响应结构调整字段名
            sbp = item["vitals"].get('sys', item.get('SBP', 0))
            dbp = item['vitals'].get('dia', item.get('DBP', 0))
            pr = item['vitals'].get('hr', item.get('PR', item.get('pulse', 0)))
            timestamp = item.get('recordTime', item.get('timestamp', 0))

            measurement_raw = Measurement(
                time=int(timestamp),
                sbp=int(sbp),
                dbp=int(dbp),
                pr=int(pr)
            )
            measurements_raw.append(measurement_raw)

        except (KeyError, ValueError) as e:
            logger.warning(f"第{i + 1}条数据解析错误: {e}, 数据: {item}")
            error_count += 1
            continue

    # 剔除 sbp 和 dpb 小于等于 0 的数据
    for i, item in enumerate(raw_data):
        try:
            # 根据实际API响应结构调整字段名
            sbp = item["vitals"].get('sys', item.get('SBP', 0))
            dbp = item['vitals'].get('dia', item.get('DBP', 0))
            pr = item['vitals'].get('hr', item.get('PR', item.get('pulse', 0)))
            timestamp = item.get('recordTime', item.get('timestamp', 0))

            # 无效测量值定义：无效测量，应该整条数据都是 0， 剔除 sbp、dpb、pr都小于等于 0 的数据，
            if sbp <= 0 and dbp <= 0:
                logger.warning(f"第{i + 1}条数据为无效读数: {item}")
                error_count += 1
                continue

            # 数据验证，
            if not all([sbp, dbp, pr, timestamp]):
                logger.warning(f"第{i + 1}条数据字段不全: {item}")
                error_count += 1
                continue

            measurement = Measurement(
                time=int(timestamp),
                sbp=int(sbp),
                dbp=int(dbp),
                pr=int(pr)
            )
            measurements.append(measurement)


        except (KeyError, ValueError) as e:
            logger.warning(f"第{i + 1}条数据解析错误: {e}, 数据: {item}")
            error_count += 1
            continue

    logger.info(f"数据解析完成, 有效数据: {len(measurements)}, 有问题数据: {error_count}")
    return measurements, measurements_raw


# 解析计划数据
def parse_plan_data(plan_data: List[Dict], logger: logging.Logger = None) -> List[MeasurementPlan]:
    """解析测量计划数据"""
    logger = logger or LoggerManager.setup_logger('PlanParser')
    """
    测量计划原始数据
    {
        "code": 0,
        "errCode": 0,
        "message": "",
        "data": [
            {
                "startTime": "12:00",
                "endTime": "12:00",
                "interval": 30,
                "enabled": true
            }
        ]
    }
    """

    plan_data = plan_data.get('data')
    plans = []
    for item in plan_data:
        plan = MeasurementPlan(
            start_time=item.get('startTime', ''),
            end_time=item.get('endTime', ''),
            interval=item.get('interval', 0),
            enabled=item.get('enabled', False)
        )
        plans.append(plan)
        logger.debug(f"解析计划数据: {plan}")
    return plans


def iso_to_timestamp_ms(iso_string: str) -> int:
    """
    将 ISO 8601 时间字符串转换为毫秒时间戳

    Args:
        iso_string: ISO 8601 格式的时间字符串，如 "2026-01-08T15:55:00.338Z"

    Returns:
        毫秒时间戳
    """
    # 解析 ISO 8601 时间字符串
    dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
    # 转换为 UTC 时间戳（秒），然后乘以 1000 得到毫秒
    timestamp_ms = int(dt.timestamp() * 1000)
    return timestamp_ms


# 主分析
def main_analysis(start_time: int, end_time: int, measurement_plan: MeasurementPlan,
                  data_list: List[Measurement], timezone_name: str,
                  timezone_offset: int = 0, day_start_hour: Any = 8,
                  night_start_hour: Any = 20, data_list_raw: List[Measurement] = None,
                  logger: logging.Logger = None):
    """主分析函数"""
    logger = logger or LoggerManager.setup_logger('MainAnalysis')
    logger.info(f"开始主分析流程, 数据量: {len(data_list)}, 时区: {timezone_name}")

    if not data_list:
        logger.error("无数据可分析")
        return

    try:
        # 初始化分析器
        analyzer = BPDataAnalyzer(
            start_time=start_time,
            end_time=end_time,
            measurement_plan=measurement_plan,
            timezone_name=timezone_name,
            timezone_offset=timezone_offset,
            day_start_hour=day_start_hour,  # 可以是整数或字符串
            night_start_hour=night_start_hour,  # 可以是整数或字符串
            logger=logger
        )

        # 分析获取捕获率
        capture_rate_result = analyzer.calculate_capture_rate(data_list)

        # 打印原始数据 BP table
        """
        从结果预期角度，我们这边的理解是：

        a. ABPM 测量值 = 0 的 BP 数据，不参与任何与血压诊断/趋势相关的数据呈现，仅作为“测量情况”的技术指标，用于判断该次测量是否有效。

        b. 目前已明确的使用场景包括：capture rate / adequacy note / BP table note，用于标识该条测量为无效。
        """
        logger.info("BP Table:")
        logger.info(f"{'No.':<6}{'时间':<23} {'SBP':<6} {'DBP':<6} {'MBP':<6} {'PR':<6}{'Note':<6}")
        logger.info("-" * 60)

        for item in data_list_raw:
            time_str = analyzer.convert_timestamp_to_local(item.time, include_date=True)
            day_status = analyzer.classify_day_night(item.time, timezone_name)
            if day_status == TimePeriod.DAY:
                item.day_status = day_status.value
                status = "🌞"
            else:
                item.day_status = day_status.value
                status = "🌗"
            if item.sbp == 0 or item.dbp == 0:
                is_valid = "Invalid"
            else:
                is_valid = ""

            # logger.info(f"{time_str} {day_status.value:<6} {item.sbp:<6} {item.dbp:<6} {item.mbp:<6} {item.pr:<6}")
            """PR显示兼容历史数据（未存 PR），没有或小于等于 0 显示 NA"""
            logger.info(
                f"{data_list_raw.index(item) + 1:<6}{time_str} {status:<6} {item.sbp:<6} {item.dbp:<6} {item.mbp:<6} {"NA" if item.pr <= 0 or item.pr is None else item.pr:<6}{is_valid:<6}")

        # 分割白天和夜间数据
        logger.info("分割白天和夜间数据...")
        day_data = []
        night_data = []

        for measurement in data_list:
            if analyzer.classify_day_night(measurement.time, timezone_name) == TimePeriod.DAY:
                day_data.append(measurement)
            else:
                night_data.append(measurement)

        logger.info(f"白天数据量: {len(day_data)}, 夜间数据量: {len(night_data)}")

        # 计算 QC result 通过标准：日间≥20，夜间≥7，总体≥70%。
        qc_result = analyzer.calculate_qc_result(analyzer, data_list, timezone_name, capture_rate_result)

        # 分析各时间段
        day_result = analyzer.analyze_period(day_data, TimePeriod.DAY)
        night_result = analyzer.analyze_period(night_data, TimePeriod.NIGHT)
        total_result = analyzer.analyze_period(data_list, TimePeriod.TOTAL)

        # 补充捕获率和QC结果到各个结果中
        qc_results_data = qc_result.qc_results  # 获取QC结果数据
        for result in [day_result, night_result, total_result]:
            result.total_capture_rate = capture_rate_result.total_capture_rate
            result.day_capture_rate = capture_rate_result.day_capture_rate
            result.night_capture_rate = capture_rate_result.night_capture_rate
            result.qc_results = qc_results_data

            result.day_measurements = capture_rate_result.day_measurements
            result.night_measurements = capture_rate_result.night_measurements
            result.total_records = capture_rate_result.total_records
            result.expected_day_measurements = capture_rate_result.expected_day_measurements
            result.expected_night_measurements = capture_rate_result.expected_night_measurements


        # 批量输出所有结果 - 集中处理日志输出
        logger.info("=" * 80)
        logger.info("ABPM 分析报告")
        logger.info("=" * 80)

        # 输出白天分析结果
        analyzer.log_summary(day_result)

        # 输出夜间分析结果
        analyzer.log_summary(night_result)

        # 计算夜间血压下降比值
        nocturnal_fall_sbp, nocturnal_fall_dbp = analyzer.calculate_nocturnal_fall(day_result, night_result)

        # 输出总体结果（带夜间下降比值）
        logger.info(f"时间范围: {analyzer.convert_timestamp_to_local(data_list[0].time, True)} - "
                    f"{analyzer.convert_timestamp_to_local(data_list[-1].time, True)}")

        # 在总结果中添加夜间血压下降信息
        total_result.nocturnal_fall_sbp = nocturnal_fall_sbp
        total_result.nocturnal_fall_dbp = nocturnal_fall_dbp

        analyzer.log_summary(total_result)

        # 输出夜间血压下降比值总结
        """
        # 夜间血压下降比值
        - **类判断逻辑 (Dipper 类型判断)**：
            - ✅ Extreme Dipper：下降百分比 ≥ -20%，解释为 "Excessive nocturnal BP fall"
            - ✅ Dipper：下降百分比 ≥-10% 且 <-20%，解释为 "Normal circadian BP rhythm"
            - ✅ Non-Dipper：下降百分比 ≥0% 且 <-10%，解释为 "Blunted nocturnal decline"
            - ✅ Riser (Reverse Dipper)：下降百分比 <0%，解释为 "Nighttime BP higher than daytime"
            具体显示：
                Dipper categoryfor SBP(Extreme/Dipper/Non-Dipper/Reverse)
                Dipper category for DBP(Extreme/Dipper/Non-Dipper/Reverse).
        """
        # 获取Dipper类型-SBP
        if nocturnal_fall_sbp >= -20:
            dipper_status_sbp = "Extreme"
        elif nocturnal_fall_sbp >= -10:
            dipper_status_sbp = "Dipper"
        elif nocturnal_fall_sbp >= 0:
            dipper_status_sbp = "Non-Dipper"
        else:
            dipper_status_sbp = "Reverse)"

        # 获取Dipper类型-DBP
        if nocturnal_fall_dbp >= -20:
            dipper_status_dbp = "Extreme"
        elif nocturnal_fall_dbp >= -10:
            dipper_status_dbp = "Dipper"
        elif nocturnal_fall_dbp >= 0:
            dipper_status_dbp = "Non-Dipper"
        else:
            dipper_status_dbp = "Reverse"

        logger.info(f"Nocturnal BP fall (SBP): {nocturnal_fall_sbp:+.1f}% - {dipper_status_sbp}")
        logger.info(f"Nocturnal BP fall (DBP): {nocturnal_fall_dbp:+.1f}% - {dipper_status_dbp}")

        # 记录完整结果到调试日志
        logger.debug(f"完整分析结果 - 白天: {day_result.to_dict()}")
        logger.debug(f"完整分析结果 - 夜间: {night_result.to_dict()}")
        logger.debug(f"完整分析结果 - 总计: {total_result.to_dict()}")

        logger.info("✅ 主分析流程完成")

    except Exception as e:
        logger.error(f"主分析过程中出错: {str(e)}", exc_info=True)
        raise
    finally:
        logger.info("主分析流程所有数据处理完毕")


def get_timezone_offset_by_name(timezone_name: str) -> int:
    """
    根据时区名获取当前偏移量（秒）

    Args:
        timezone_name: 时区名称，如 'Asia/Shanghai', 'UTC', 'America/New_York'

    Returns:
        当前时区偏移量（秒）
    """
    try:
        # 获取当前时间
        now = datetime.now(ZoneInfo(timezone_name))
        # 获取时区偏移量（秒）
        offset_seconds = now.utcoffset().total_seconds()
        return int(offset_seconds)
    except Exception as e:
        logger.error(f"获取时区偏移失败: {str(e)}")
        return 0


def run_analysis_from_api(tenants: str, subject_id: str, data_type: DataType,
                          start_time: str, end_time: str,
                          auth_id: str, auth_key: str,
                          timezone: str, email: str, password: str, patient_id: str,
                          day_start_hour: Any = 8, night_start_hour: Any = 20,  # 修改参数类型为Any
                          vcloud_url: str = "vcloud-test.vivalink.com",
                          web_url: str = "webportal-dev.vivalink.com",
                          log_file: str = None):
    """从API获取数据并进行分析"""
    # 设置日志
    logger = LoggerManager.setup_logger(
        'APIAnalysis',
        log_file=log_file,
        level=logging.INFO
    )

    # 转换字符串时间2026-01-08T15:55:00.338Z为毫秒时间戳
    start_time_ms = iso_to_timestamp_ms(start_time)
    end_time_ms = iso_to_timestamp_ms(end_time)
    logger.info(f"开始结束时间转换时间戳：{start_time_ms}-->{end_time_ms}")

    # 记录传入的时间参数
    logger.info(f"day_start_hour: {day_start_hour} (类型: {type(day_start_hour)})")
    logger.info(f"night_start_hour: {night_start_hour} (类型: {type(night_start_hour)})")

    # 初始化时区偏移量
    timeZone_offset = get_timezone_offset_by_name(timezone)

    try:
        # 初始化数据获取器
        fetcher = DataFetcher(
            auth_url=f"https://{vcloud_url}/auth",
            data_url=f"https://{vcloud_url}",
            logger=logger
        )

        # 认证
        logger.info("正在认证...")
        if not fetcher.authenticate(auth_id, auth_key):
            logger.error("认证失败，退出程序")
            return

        # 获取数据
        logger.info("正在获取数据...")
        raw_data = fetcher.fetch_data(tenants, subject_id, data_type, start_time_ms, end_time_ms)

        if not raw_data:
            logger.warning("未获取到数据，退出程序")
            return

        # 解析数据
        measurements, measurements_raw = parse_raw_data(raw_data.get("data").get("list"), logger)

        if not measurements_raw:
            logger.warning("无有效数据，退出程序")
            return

        logger.info(f"成功获取并解析 {len(measurements_raw)} 条数据")

        # 获取测量计划
        fetcher2 = DataFetcher(
            auth_url=f"https://{web_url}/api/backend/authentication",
            data_url=f"https://{web_url}",
            logger=logger
        )
        # 认证
        logger.info("正在进行 Webportal 认证...")
        if not fetcher2.authenticate_webportal(email, password):
            logger.error("认证失败，退出程序")
            return
        plan_raw_data = fetcher2.fetch_measurement_plan(patient_id)
        logger.info(f"成功获取测量计划")

        plan_list = parse_plan_data(plan_raw_data, logger)

        # 获取plan 中的interval数据
        measurement_plan = plan_list[0]
        logger.info(f"获取到测量计划：{measurement_plan}")

        # 数据分析
        main_analysis(data_list_raw=measurements_raw, data_list=measurements, start_time=start_time_ms,
                      end_time=end_time_ms, measurement_plan=measurement_plan, timezone_name=timezone,
                      day_start_hour=day_start_hour, night_start_hour=night_start_hour, logger=logger)

        logger.info("API数据分析流程完成")

    except Exception as e:
        logger.error(f"API数据分析失败: {str(e)}", exc_info=True)
    finally:
        logger.info("API数据分析流程所有数据处理完毕")


def run_analysis_with_config(config_file: str = "config.json"):
    """通过配置文件运行分析"""
    logger = LoggerManager.setup_logger('ConfigAnalysis')

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)

        logger.info(f"从配置文件加载配置: {config_file}")

        # 解析配置
        tenants = config.get('tenants')
        subject_id = config.get('subject_id')
        data_type_str = config.get('data_type', 'BPRaw')
        start_time = config.get('start_time')
        end_time = config.get('end_time')
        auth_id = config.get('auth_id')
        auth_key = config.get('auth_key')
        timezone = config.get('timezone', 'UTC')  # 默认时区
        log_file = config.get('log_file')
        email = config.get('email')  # 新增字段
        password = config.get('password')  # 新增字段
        patient_id = config.get('patient_id')  # 新增字段
        day_start_hour = config.get('day_start_hour', 8)  # 新增字段，默认值8
        night_start_hour = config.get('night_start_hour', 20)  # 新增字段，默认值20

        # 验证必要参数
        required_fields = ['tenants', 'subject_id', 'start_time', 'end_time', 'auth_id', 'auth_key']
        for field in required_fields:
            if not config.get(field):
                logger.error(f"配置文件中缺少必要字段: {field}")
                return

        # 额外验证 webportal 相关参数
        webportal_required_fields = ['email', 'password', 'patient_id']
        for field in webportal_required_fields:
            if not config.get(field):
                logger.warning(f"配置文件中缺少webportal相关字段: {field}，这可能影响测量计划获取")

        # 转换数据类型
        try:
            data_type = DataType(data_type_str)
        except ValueError:
            logger.error(f"不支持的数据类型: {data_type_str}")
            return

        # 运行分析
        run_analysis_from_api(
            tenants=tenants,
            subject_id=subject_id,
            data_type=data_type,
            start_time=start_time,
            end_time=end_time,
            auth_id=auth_id,
            auth_key=auth_key,
            timezone=timezone,
            log_file=log_file,
            email=email,
            password=password,
            patient_id=patient_id,
            day_start_hour=day_start_hour,
            night_start_hour=night_start_hour
        )

    except FileNotFoundError:
        logger.error(f"配置文件不存在: {config_file}")
    except json.JSONDecodeError as e:
        logger.error(f"配置文件格式错误: {str(e)}")
    except Exception as e:
        logger.error(f"配置分析失败: {str(e)}", exc_info=True)


if __name__ == "__main__":
    logger = LoggerManager.setup_logger(
        'BPAnalysisMain',
        log_file='bp_analysis.log',
        level=logging.DEBUG  # 设置为DEBUG以查看更多详细信息
    )

    logger.info("血压数据分析程序启动")

    # run_analysis_from_api(
    #     tenants="UATV2_360_ABPM",
    #     subject_id="J20260121001",
    #     data_type=DataType.BP_RAW,
    #     start_time="2025-11-01T04:00:00Z",
    #     end_time="2025-11-02T04:00:00Z",
    #     # start_time="2025-11-01T04:00:00Z",  # 2026-01-10T05:00:00Z
    #     # end_time="2025-11-04T04:00:00.00Z",  # 2026-01-11T05:00:59.999Z
    #     auth_id="617070e40daf63ba334ece90d1",
    #     auth_key="@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF",
    #     timezone="America/New_York", #Europe/Brussels America/New_York
    #     log_file="api_analysis.log",
    #     email="jun@vivalink.com.cn",
    #     password="Jun@1234",
    #     patient_id="69702b12c05de95fdb7132f7",
    #     day_start_hour="08:00",
    #     night_start_hour="20:00",
    #     vcloud_url="vcloud-test.vivalink.com",  # 不传默认测试环境：vcloud-test.vivalink.com
    #     web_url="webportal-dev2.vivalink.com"  # 不传默认测试环境：webportal-dev.vivalink.com
    # )

    run_analysis_from_api(
        tenants="UATV2_360_ABPM",
        subject_id="J20260121002",
        data_type=DataType.BP_RAW,
        start_time="2025-03-09T05:00:00Z",
        end_time="2025-03-10T05:00:00Z",
        auth_id="617070e40daf63ba334ece90d1",
        auth_key="@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF",
        timezone="America/New_York",  # Europe/Brussels America/New_York
        log_file="api_analysis.log",
        email="jun@vivalink.com.cn",
        password="Jun@1234",
        patient_id="6970722aba8c3db350fffa05",
        day_start_hour="08:00",
        night_start_hour="20:00",
        vcloud_url="vcloud-test.vivalink.com",  # 不传默认测试环境：vcloud-test.vivalink.com
        web_url="webportal-dev2.vivalink.com"  # 不传默认测试环境：webportal-dev.vivalink.com
    )

    # 通过配置文件运行
    """
    run_analysis_with_config("config.json")
    """