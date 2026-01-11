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

"""
血压数据分析脚本 - Logger优化版
支持传入tenants, subjectId, type, 时间范围参数读取数据
计算Total/Day/Night各指标值

规则：
1. MBP先取整后计算
2. 其他指标先计算最后取整
"""

import timedelta
from datetime import datetime, timedelta,time
import decimal
import json
# import time
import sys
import logging
from decimal import Decimal
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import numpy as np
import requests
from zoneinfo import ZoneInfo
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
        mbp = Decimal(self.dbp + 1/3 * (self.sbp- self.dbp) ).quantize(
            Decimal('0'),
            rounding=decimal.ROUND_HALF_UP
        )
        if mbp < 0:
            mbp = 0
        return int(mbp)


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
class SummaryResult:
    """分析结果汇总"""
    total_capture_rate: int
    day_capture_rate: int
    night_capture_rate: float
    day_measurements: float
    night_measurements: int
    expected_per_day: int
    expected_per_night:int
    period: TimePeriod
    sbp_stats: Statistics
    dbp_stats: Statistics
    mbp_stats: Statistics
    pr_stats: Statistics
    total_records: int
    above_limits_sbp: Optional[float] = None
    above_limits_dbp: Optional[float] = None
    nocturnal_fall_sbp: Optional[float] = None
    nocturnal_fall_dbp: Optional[float] = None

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'total_capture_rate': self.total_capture_rate,
            'day_capture_rate': self.day_capture_rate,
            'night_capture_rate': self.night_capture_rate,
            'day_measurements': self.day_measurements,
            'night_measurements': self.night_measurements,
            'expected_per_day': self.expected_per_day,
            'expected_per_night': self.expected_per_night,
            'period': self.period.value,
            'sbp_stats': self.sbp_stats.to_dict(),
            'dbp_stats': self.dbp_stats.to_dict(),
            'mbp_stats': self.mbp_stats.to_dict(),
            'pr_stats': self.pr_stats.to_dict(),
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
            # logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d]->%(levelname)s: %(message)s')
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

    def __init__(self,start_time: int, end_time: int,plan_interval: int,timezone_name: str, timezone_offset: int = 0, logger: logging.Logger = None):
        """
        初始化分析器

        Args:
            timezone_offset: 时区偏移（秒）
            logger: 日志记录器
        """
        self.START_TIME = start_time
        self.END_TIME = end_time
        self.PLAN_INTERVAL = plan_interval
        self.timezone_offset = timezone_offset
        self.logger = logger or LoggerManager.setup_logger('BPDataAnalyzer')

        self.logger.info(f"初始化BPDataAnalyzer, 时区偏移: {timezone_offset}秒")

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
            local_time = utc_time + timedelta(seconds=self.timezone_offset)

            # 格式化
            if include_date:
                return local_time.strftime("%m/%d/%y %I:%M %p").lower()
            else:
                return local_time.strftime("%I:%M %p").lower()

        except Exception as e:
            self.logger.error(f"时间转换失败: timestamp={timestamp_ms}, error={str(e)}")
            return "时间转换错误"

    def classify_day_night(self, timestamp_ms: int, timezone_name:str) -> TimePeriod:
        """
        根据时间戳判断是白天还是夜间

        Args:
            timestamp_ms: 毫秒时间戳

        Returns:
            TimePeriod.DAY 或 TimePeriod.NIGHT
        """
        try:
            timestamp_sec = timestamp_ms // 1000 if timestamp_ms > 1e12 else timestamp_ms
            utc_time=datetime.fromtimestamp(timestamp_sec)
            # 根据时区名转换为当地时间
            local_time=utc_time.astimezone(ZoneInfo(timezone_name))
            hour = local_time.hour

            # 白天: 8:00-20:00, 夜间: 20:00-次日8:00
            return TimePeriod.DAY if 8 <= hour < 20 else TimePeriod.NIGHT

        except Exception as e:
            self.logger.error(f"时间分类失败: timestamp={timestamp_ms}, error={str(e)}")
            return TimePeriod.DAY  # 默认返回白天

    def calculate_statistics(self, data_list: List[Measurement],
                             value_field: str) -> Statistics:
        """
        计算基本统计指标

        Args:
            data_list: 测量数据列表
            value_field: 要统计的字段名 ('sbp', 'dbp', 'mbp', 'pr')

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

            # 计算统计值
            avg = np.mean(values)
            std = np.std(values, ddof=1)  # 样本标准差

            # 找到最小值和最大值及其时间
            if value_field in ['sbp', 'dbp', 'pr']:
                # 对于这些指标，需要记录对应的时间
                min_val = min(data_list, key=lambda x: getattr(x, value_field))
                max_val = max(data_list, key=lambda x: getattr(x, value_field))
                min_time = min_val.time
                max_time = max_val.time
                min_val = getattr(min_val, value_field)
                max_val = getattr(max_val, value_field)
            else:
                # 对于MBP，值已计算，直接找最大最小值
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

            result = SummaryResult(
                total_capture_rate=0 ,
                day_capture_rate=0 ,
                night_capture_rate=0 ,
                day_measurements=0 ,
                night_measurements=0 ,
                expected_per_day =0 ,
                expected_per_night=0 ,
                period=period,
                sbp_stats=sbp_stats,
                dbp_stats=dbp_stats,
                mbp_stats=mbp_stats,
                pr_stats=pr_stats,
                total_records=len(data_list)
            )

            # 根据时间段计算特定指标
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

            self.logger.info(f"{period.value}时间段分析完成, 总记录数={result.total_records}")
            return result

        except Exception as e:
            self.logger.error(f"{period.value}时间段分析失败: error={str(e)}")
            raise

    # TODO 计算捕获率
    # def calculate_capture_rate(self,data_list: List[Measurement],start_timestamp: int,
    #                                      end_timestamp: int,
    #                                      timezone_name: str,
    #                                      day_start_hour: int = 8,
    #                                      night_start_hour: int = 20):
    #     """
    #     计算白天和夜间时间段的测量次数
    #
    #
    #     Args:
    #         start_timestamp: 开始时间戳（毫秒）
    #         end_timestamp: 结束时间戳（毫秒）
    #         timezone_name: 时区名称
    #         day_start_hour: 白天开始小时 (默认8点)
    #         night_start_hour: 夜间开始小时 (默认20点)
    #         plan_interval: 测量间隔分钟数
    #
    #     Returns:
    #         dict: 包含白天、夜间和总计测量次数的字典
    #     """
    #
    #
    #     self.logger.info("开始计算测量数据中捕获率")
    #     if not data_list:
    #         self.logger.warning("数据列表为空")
    #         return 0
    #     # 根据开始结束时间范围，测量计划间隔计算测量次数,间隔是从 0 分 0 秒 开始
    #     start_time=self.START_TIME
    #     end_time=self.END_TIME
    #     plan_interval=self.PLAN_INTERVAL
    #
    #     # # 应该这样计算预期测量次数
    #     # expected_measurements = int((end_time/1000 - start_time/1000) / plan_interval*60)
    #     # self.logger.debug(f"预期测量次数: {expected_measurements}")
    #     # total_capture_rate = len(data_list) / expected_measurements
    #     # self.logger.info(f"测量数据中总捕获率计算完成, 捕获率={total_capture_rate:.1%}")
    #
    #     # 转换时间戳到时区时间
    #     start_dt = datetime.fromtimestamp(start_timestamp / 1000, tz=ZoneInfo(timezone_name))
    #     end_dt = datetime.fromtimestamp(end_timestamp / 1000, tz=ZoneInfo(timezone_name))
    #
    #     print(f"开始时间: {start_dt}")
    #     print(f"结束时间: {end_dt}")
    #
    #     # 计算总天数
    #     total_days = (end_dt.date() - start_dt.date()).days + 1
    #     print(f"总天数: {total_days}")
    #
    #     # 定义白天和夜间时间段
    #     day_start_time = timedelta(hours=day_start_hour)  # 8:00
    #     night_start_time = timedelta(hours=night_start_hour)  # 20:00
    #     day_duration = night_start_time - day_start_time  # 白天持续时间 (12小时)
    #
    #     # 计算每天的测量次数
    #     day_minutes = int(day_duration.total_seconds() / 60)  # 白天分钟数
    #     measurements_per_day = math.ceil(day_minutes / plan_interval)  # 白天测量次数
    #
    #     night_minutes = 24 * 60 - day_minutes  # 夜间分钟数
    #     measurements_per_night = math.ceil(night_minutes / plan_interval)  # 夜间测量次数
    #
    #     logger.debug(f"每天测量次数: 白天={measurements_per_day}, 夜间={measurements_per_night}")
    #
    #     # 计算跨天情况下的准确测量次数
    #     day_measurements = 0
    #     night_measurements = 0
    #
    #     current_date = start_dt.date()
    #     while current_date <= end_dt.date():
    #         # 计算当天的开始和结束时间
    #         day_start = datetime.combine(current_date, datetime.min.time()) + day_start_time
    #         day_end = datetime.combine(current_date, datetime.min.time()) + night_start_time
    #         next_day_start = datetime.combine(current_date + timedelta(days=1), datetime.min.time())
    #
    #         # 确定当天在整体时间范围内的实际时间段
    #         actual_day_start = max(day_start, start_dt)
    #         actual_day_end = min(day_end, end_dt)
    #         actual_night_start = max(day_end, start_dt)
    #         actual_night_end = min(next_day_start, end_dt)
    #
    #         # 计算白天测量次数
    #         if actual_day_start < actual_day_end:
    #             day_duration_minutes = (actual_day_end - actual_day_start).total_seconds() / 60
    #             day_measurements += math.ceil(day_duration_minutes / plan_interval)
    #
    #         # 计算夜间测量次数
    #         if actual_night_start < actual_night_end:
    #             night_duration_minutes = (actual_night_end - actual_night_start).total_seconds() / 60
    #             night_measurements += math.ceil(night_duration_minutes / plan_interval)
    #
    #         current_date += timedelta(days=1)
    #
    #     total_measurements = day_measurements + night_measurements
    #
    #     total_capture_rate = len(data_list) / total_measurements
    #
    #     # 计算白天捕获率
    #     day_capture_rate = day_measurements / measurements_per_day
    #
    #     # 计算夜晚捕获率
    #     night_capture_rate = night_measurements / measurements_per_night
    #
    #     return {
    #         "total_capture_rate":total_capture_rate, # 总捕获率
    #         "day_capture_rate":day_capture_rate,      # 白天捕获率
    #         "night_capture_rate":night_capture_rate, # 夜晚捕获率
    #         'day_measurements': day_measurements, # 白天测量次数
    #         'night_measurements': night_measurements, # 夜间测量次数
    #         'total_measurements': total_measurements, # 总测量次数
    #         'expected_per_day': measurements_per_day, # 白天测量次数
    #         'expected_per_night': measurements_per_night # 夜间测量次数
    #     }

    def calculate_capture_rate(self, data_list: List[Measurement], start_timestamp: int,
                               end_timestamp: int, timezone_name: str,
                               day_start_hour: int = 8, night_start_hour: int = 20):
        """
        计算白天和夜间时间段的测量捕获率

        Args:
            data_list: 测量数据列表
            start_timestamp: 开始时间戳（毫秒）
            end_timestamp: 结束时间戳（毫秒）
            timezone_name: 时区名称
            day_start_hour: 白天开始时间，默认8点
            night_start_hour: 夜间开始时间，默认20点

        Returns:
            dict: 包含捕获率统计结果的字典
        """
        self.logger.info("开始计算测量数据捕获率")

        if not data_list:
            self.logger.warning("数据列表为空")
            return {
                "total_capture_rate": 0,
                "day_capture_rate": 0,
                "night_capture_rate": 0,
                'day_measurements': 0,
                'night_measurements': 0,
                'total_measurements': 0,
                'expected_day_measurements': 0,
                'expected_night_measurements': 0,
                'expected_total_measurements': 0
            }

        plan_interval = self.PLAN_INTERVAL  # 计划测量间隔（分钟）

        # 转换时间戳到时区时间
        start_dt = datetime.fromtimestamp(start_timestamp / 1000, tz=ZoneInfo(timezone_name))
        end_dt = datetime.fromtimestamp(end_timestamp / 1000, tz=ZoneInfo(timezone_name))

        self.logger.info(f'时间范围：{start_dt} --> {end_dt}')

        # 统计实际测量次数（按白天/夜间分类）
        day_actual_count = 0
        night_actual_count = 0

        for measurement in data_list:
            # 假设Measurement对象有timestamp属性（毫秒）
            measure_dt = datetime.fromtimestamp(measurement.time / 1000,
                                                tz=ZoneInfo(timezone_name))
            hour = measure_dt.hour

            # 判断是白天还是夜间
            if day_start_hour <= hour < night_start_hour:
                day_actual_count += 1
            else:
                night_actual_count += 1

        # 计算计划测量次数
        day_planned_count = 0
        night_planned_count = 0

        # 当前日期（用于循环）
        current_date = start_dt.date()
        end_date = end_dt.date()

        while current_date <= end_date:
            # 计算当天的白天开始和结束时间（使用 datetime.time 类）
            day_start = datetime.combine(current_date, time(day_start_hour, 0, 0)) \
                .replace(tzinfo=ZoneInfo(timezone_name))
            day_end = datetime.combine(current_date, time(night_start_hour, 0, 0)) \
                .replace(tzinfo=ZoneInfo(timezone_name))

            # 计算当天的夜间开始和结束时间（夜间跨天）
            night_start = day_end
            night_end = datetime.combine(current_date + timedelta(days=1),
                                         time(day_start_hour, 0, 0)) \
                .replace(tzinfo=ZoneInfo(timezone_name))

            # 计算实际时间范围内的白天时间段
            actual_day_start = max(day_start, start_dt)
            actual_day_end = min(day_end, end_dt)

            if actual_day_start < actual_day_end:
                # 计算白天时间段内的计划测量次数
                day_duration_minutes = (actual_day_end - actual_day_start).total_seconds() / 60
                # 向上取整计算计划测量次数
                day_planned_count += math.ceil(day_duration_minutes / plan_interval)

            # 计算实际时间范围内的夜间时间段
            actual_night_start = max(night_start, start_dt)
            actual_night_end = min(night_end, end_dt)

            if actual_night_start < actual_night_end:
                # 计算夜间时间段内的计划测量次数
                night_duration_minutes = (actual_night_end - actual_night_start).total_seconds() / 60
                # 向上取整计算计划测量次数
                night_planned_count += math.ceil(night_duration_minutes / plan_interval)

            # 下一天
            current_date += timedelta(days=1)

        # 计算总计划测量次数
        total_planned_count = day_planned_count + night_planned_count

        # 计算捕获率
        total_actual_count = day_actual_count + night_actual_count
        total_capture_rate = total_actual_count / total_planned_count if total_planned_count > 0 else 0

        day_capture_rate = day_actual_count / day_planned_count if day_planned_count > 0 else 0
        night_capture_rate = night_actual_count / night_planned_count if night_planned_count > 0 else 0

        result = {
            "total_capture_rate": round(total_capture_rate, 4),
            "day_capture_rate": round(day_capture_rate, 4),
            "night_capture_rate": round(night_capture_rate, 4),
            'day_measurements': day_actual_count,
            'night_measurements': night_actual_count,
            'total_measurements': total_actual_count,
            'expected_day_measurements': day_planned_count,
            'expected_night_measurements': night_planned_count,
            'expected_total_measurements': total_planned_count,
            'plan_interval_minutes': plan_interval,
            'time_range_days': (end_date - start_dt.date()).days + 1
        }

        self.logger.info(f"捕获率计算完成：总计划{total_planned_count}次，"
                         f"实际{total_actual_count}次，捕获率{total_capture_rate:.2%}")
        self.logger.info(f"白天：计划{day_planned_count}次，实际{day_actual_count}次，"
                         f"捕获率{day_capture_rate:.2%}")
        self.logger.info(f"夜间：计划{night_planned_count}次，实际{night_actual_count}次，"
                         f"捕获率{night_capture_rate:.2%}")

        return result

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

    def log_summary(self, result: SummaryResult):
        """记录分析结果到日志"""
        period_name = result.period.value.capitalize()

        self.logger.info(f"{'=' * 80}")
        self.logger.info(f"{period_name} Summary")
        self.logger.info(f"{'=' * 80}")

        # 格式化时间显示
        def format_time(timestamp):
            return self.convert_timestamp_to_local(timestamp) if timestamp else "N/A"

        """
            计算逻辑说明
        """
        # SBP -计算逻辑需要说明
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

        # Pulse Rate
        self.logger.info(f"Pulse Rate  Min: {result.pr_stats.min_val} ({format_time(result.pr_stats.min_time)})")
        self.logger.info(f"            Max: {result.pr_stats.max_val} ({format_time(result.pr_stats.max_time)})")
        self.logger.info(
            f"            Average: {Decimal(result.pr_stats.avg).quantize(Decimal('0'), rounding=decimal.ROUND_HALF_UP)}")
        self.logger.info(f"            Std.Dev.: {result.pr_stats.std:.1f}")
        self.logger.info(f"            CV: {result.pr_stats.cv:.2f}")

        # TODO 增加捕获率指标
        self.logger.info(f" Total Capture Rate{result.total_capture_rate:.1f}")
        self.logger.info(f"Day Capture Rate{result.day_capture_rate:.1f}")
        self.logger.info(f"Night Capture Rate{result.night_capture_rate:.1f}")

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
                timeout=(30,120),# 设置超时时间为30秒,读取超时设置为120秒
                stream=True # 设置流式读取
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

    # TODO 获取Webportal授权
    def authenticate_webportal(self, email: str, password: str) -> bool:
        """身份认证"""
        self.logger.info(f"开始WebPortal身份认证: auth_url={self.auth_url}")

        try:

            headers = {
                'Authorization': self.token,
                'Content-Type': 'application/json'
            }
            payload = {"email": email,"password":password}
            response = requests.post(self.auth_url, headers=headers,json=payload, timeout=30)

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

    # TODO 获取用户最新测量计划
    def fetch_measurement_plan(self,patientid):
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
            #https://webportal-dev.vivalink.com/api/backend/abpm/plan?patientId=695db80024cd6c753484f95c
            url = f"{self.data_url}/api/backend/abpm/plan"
            self.logger.debug(f"请求URL: {url}, 参数: {params}")

            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=(30,120),# 设置超时时间为30秒,读取超时设置为120秒
                stream=True # 设置流式读取
            )

            if response.status_code == 200:
                data = response.json()

                self.logger.info(f"数据获取成功，测试计划列表: {len(data.get("data"))}")
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
    error_count = 0

    for i, item in enumerate(raw_data):
        try:
            # 根据实际API响应结构调整字段名
            sbp = item["vitals"].get('sys', item.get('SBP', 0))
            dbp = item['vitals'].get('dia', item.get('DBP', 0))
            pr = item['vitals'].get('hr', item.get('PR', item.get('pulse', 0)))
            timestamp = item.get('recordTime', item.get('timestamp', 0))

            # 数据验证
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

    logger.info(f"数据解析完成, 成功: {len(measurements)}, 失败: {error_count}")
    return measurements

# TODO 解析测量计划数据
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


# TODO 解析计划数据
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
    return  plans



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

def main_analysis(start_time:int, end_time:int,plan_interval:int,data_list: List[Measurement],timezone_name: str, timezone_offset: int = 0,
                  logger: logging.Logger = None):
    """主分析函数"""
    logger = logger or LoggerManager.setup_logger('MainAnalysis')
    logger.info(f"开始主分析流程, 数据量: {len(data_list)}, 时区: {timezone_name}")

    if not data_list:
        logger.error("无数据可分析")
        return

    try:
        # 初始化分析器
        # analyzer = BPDataAnalyzer(start_time=start_time, end_time=end_time,plan_interval=plan_interval,timezone_name=timezone_name,timezone_offset=timezone_offset, logger=logger)
        analyzer = BPDataAnalyzer(start_time=start_time, end_time=end_time,plan_interval=plan_interval,timezone_name=timezone_name,timezone_offset=timezone_offset,logger=logger)

        # 打印原始数据
        logger.info("BP Table:")
        logger.info(f"{'时间':<23} {'SBP':<6} {'DBP':<6} {'MBP':<6} {'PR':<6}")
        logger.info("-" * 60)

        for item in data_list:
            time_str = analyzer.convert_timestamp_to_local(item.time, include_date=True)
            day_status=analyzer.classify_day_night(item.time,timezone_name)
            if day_status==TimePeriod.DAY:
                item.day_status=day_status.value
                status="🌞"
            else:
                item.day_status=day_status.value
                status="🌗"

            # logger.info(f"{time_str} {day_status.value:<6} {item.sbp:<6} {item.dbp:<6} {item.mbp:<6} {item.pr:<6}")
            logger.info(f"{time_str} {status:<6} {item.sbp:<6} {item.dbp:<6} {item.mbp:<6} {item.pr:<6}")

        # 分割白天和夜间数据
        logger.info("分割白天和夜间数据...")
        day_data = []
        night_data = []

        for measurement in data_list:
            if analyzer.classify_day_night(measurement.time,timezone_name) == TimePeriod.DAY:
                day_data.append(measurement)
            else:
                night_data.append(measurement)

        logger.info(f"白天数据量: {len(day_data)}, 夜间数据量: {len(night_data)}")

        # 分析各时间段
        day_result = analyzer.analyze_period(day_data, TimePeriod.DAY)
        night_result = analyzer.analyze_period(night_data, TimePeriod.NIGHT)
        total_result = analyzer.analyze_period(data_list, TimePeriod.TOTAL)


        # analyzer = BPDataAnalyzer(start_time,end_time,plan_interval,timezone_name,timezone_offset,logger=logger)
        # capture_rate_list=analyzer.calculate_capture_rate(data_list,start_time,end_time,timezone_name,day_start_hour,night_start_hour)

        # 记录结果
        analyzer.log_summary(day_result)
        analyzer.log_summary(night_result)

        # 计算夜间血压下降比值
        nocturnal_fall_sbp, nocturnal_fall_dbp = analyzer.calculate_nocturnal_fall(day_result, night_result)

        # 记录总体结果（带夜间下降比值）
        logger.info(f"{'=' * 80}")
        logger.info(f"Total Summary ({analyzer.convert_timestamp_to_local(data_list[0].time, True)} - "
                    f"{analyzer.convert_timestamp_to_local(data_list[-1].time, True)})")
        logger.info(f"{'=' * 80}")

        analyzer.log_summary(total_result)

        # 夜间血压下降比值
        dipper_status_sbp = "(Normal)" if nocturnal_fall_sbp >= 10 else "(Non-Dipper)"
        dipper_status_dbp = "(Normal)" if nocturnal_fall_dbp >= 10 else "(Non-Dipper)"

        logger.info(f"Nocturnal BP fall (SBP): {nocturnal_fall_sbp:.1f}% {dipper_status_sbp}")
        logger.info(f"Nocturnal BP fall (DBP): {nocturnal_fall_dbp:.1f}% {dipper_status_dbp}")

        # 记录完整结果到调试日志
        logger.debug(f"完整分析结果 - 白天: {day_result.to_dict()}")
        logger.debug(f"完整分析结果 - 夜间: {night_result.to_dict()}")
        logger.debug(f"完整分析结果 - 总计: {total_result.to_dict()}")

        logger.info("分析流程完成")

    except Exception as e:
        logger.error(f"分析过程中出错: {str(e)}", exc_info=True)
        raise

# TODO 测量计划分析
def main_analysis_with_plan(start_time: int, end_time: int, plan_interval: int, timezone_name: str, timezone_offset: int,data_list: List[Measurement], day_start_hour: int, night_start_hour: int):
    """计算预期测量次数"""
    logger = LoggerManager.setup_logger('MainAnalysisWithPlan')
    logger.info(f"开始计划分析流程")
    try:
        analyzer = BPDataAnalyzer(start_time,end_time,plan_interval,timezone_name,timezone_offset,logger=logger)
        capture_rate_list=analyzer.calculate_capture_rate(data_list,start_time,end_time,timezone_name,day_start_hour,night_start_hour)
        # capture_rate_list=[item.to_dict() for item in capture_rate_list]

       # 记录结果
        result = SummaryResult(
            total_capture_rate=capture_rate_list.get('total_capture_rate'),
            day_capture_rate=capture_rate_list.get('day_capture_rate'),
            night_capture_rate=capture_rate_list.get('night_capture_rate'),
            day_measurements=capture_rate_list.get('day_measurements'),
            night_measurements=capture_rate_list.get('night_measurements'),
            expected_per_day=capture_rate_list.get('expected_per_day'),
            expected_per_night=capture_rate_list.get('expected_per_night'),
            period=0,
            sbp_stats = 0,
            dbp_stats = 0,
            mbp_stats = 0,
            pr_stats = 0,
            total_records = 0

        )
        analyzer.log_summary(result )
        return capture_rate_list
    except Exception as e:
        logger.error(f"计划分析过程中出错: {str(e)}", exc_info=True)
        raise

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
            now = datetime.datetime.now(ZoneInfo(timezone_name))
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
                          day_start_hour: int = 8, night_start_hour: int = 20,
                          log_file: str = None):
    """从API获取数据并进行分析"""
    # 设置日志
    logger = LoggerManager.setup_logger(
        'APIAnalysis',
        log_file=log_file,
        level=logging.INFO
    )

    # 转换字符串时间2026-01-08T15:55:00.338Z为毫秒时间戳
    start_time = iso_to_timestamp_ms(start_time)
    end_time = iso_to_timestamp_ms(end_time)
    logger.info(f"开始结束时间转换时间戳：{start_time}-->{end_time}")

    logger.info(f"开始API数据分析: tenants={tenants}, subject_id={subject_id}, "
                f"data_type={data_type.value}, start={start_time}, end={end_time}")

    #初始化时区偏移量
    timeZone_offset = get_timezone_offset_by_name(timezone)

    try:
        # 初始化数据获取器
        fetcher = DataFetcher(
            auth_url="https://vcloud-test.vivalink.com/auth",
            data_url="https://vcloud-test.vivalink.com",
            logger=logger
        )

        # 认证
        logger.info("正在认证...")
        if not fetcher.authenticate(auth_id, auth_key):
            logger.error("认证失败，退出程序")
            return

        # 获取数据
        logger.info("正在获取数据...")
        raw_data = fetcher.fetch_data(tenants, subject_id, data_type, start_time, end_time)

        if not raw_data:
            logger.warning("未获取到数据，退出程序")
            return

        # 解析数据
        measurements = parse_raw_data(raw_data.get("data").get("list"), logger)

        if not measurements:
            logger.warning("无有效数据，退出程序")
            return

        logger.info(f"成功获取并解析 {len(measurements)} 条数据")

        # 进行分析
        # main_analysis(measurements, timezone, timeZone_offset, logger)
        # 默认值，需要单独处理
        plan_interval=30
        main_analysis(start_time, end_time, plan_interval, measurements,timezone, timeZone_offset,  logger)

        # TODO 获取测量计划
        "https://webportal-dev.vivalink.com/api/backend/abpm/plan?patientId=695db80024cd6c753484f95c"
        # 获取测量计划
        # 初始化数据获取器
        fetcher2 = DataFetcher(
            auth_url="https://webportal-dev.vivalink.com/api/backend/authentication",
            data_url="https://webportal-dev.vivalink.com",
            logger=logger
        )
        # 认证
        logger.info("正在进行 Webportal 认证...")
        if not fetcher2.authenticate_webportal(email, password):
            logger.error("认证失败，退出程序")
            return
        plan_raw_data = fetcher2.fetch_measurement_plan(patient_id)
        logger.info(f"成功获取测量计划")

        plan_list=parse_plan_data(plan_raw_data, logger)

        # # 获取plan 中的interval数据
        plan=plan_list[0]
        plan_interval=plan.interval
        logger.info(f"测量计划间隔为：{plan_interval}")

        # 进行计划分析
        #Todo 测量计划分析
        main_analysis_with_plan(start_time, end_time, plan_interval, timezone, timeZone_offset, measurements,  day_start_hour, night_start_hour)

        logger.info("API数据分析流程完成")

    except Exception as e:
        logger.error(f"API数据分析失败: {str(e)}", exc_info=True)


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
        timezone = config.get('timezone', 0)
        log_file = config.get('log_file')

        # 验证必要参数
        required_fields = ['tenants', 'subject_id', 'start_time', 'end_time', 'auth_id', 'auth_key']
        for field in required_fields:
            if not config.get(field):
                logger.error(f"配置文件中缺少必要字段: {field}")
                return

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
            log_file=log_file
        )

    except FileNotFoundError:
        logger.error(f"配置文件不存在: {config_file}")
    except json.JSONDecodeError as e:
        logger.error(f"配置文件格式错误: {str(e)}")
    except Exception as e:
        logger.error(f"配置分析失败: {str(e)}", exc_info=True)


if __name__ == "__main__":
    # 示例1：使用Logger记录器
    logger = LoggerManager.setup_logger(
        'BPAnalysisMain',
        log_file='bp_analysis.log',
        level=logging.DEBUG  # 设置为DEBUG以查看更多详细信息
    )

    logger.info("血压数据分析程序启动")

    run_analysis_from_api(
        tenants="Test360_V2_ABPM",
        subject_id="J20260107001",
        data_type=DataType.BP_RAW,
        start_time="2026-01-08T05:00:00.338Z",
        end_time="2026-01-08T15:55:00.338Z",
        auth_id="617070e40daf63ba334ece90d1",
        auth_key="@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF",
        timezone="America/New_York",
        log_file="api_analysis.log",
        email="jun@vivalink.com.cn",
        password="Jun@1234",
        patient_id="695db80024cd6c753484f95c",
        day_start_hour=8,
        night_start_hour=20
    )

    # 通过配置文件运行（创建config.json文件）
    """
    run_analysis_with_config("config.json")
    """