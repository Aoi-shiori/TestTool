#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Project: TestTool
@Author: guojun
@Email: 391350540@qq.com
@Date: 2026/5/15 14:08
@File: extract_patient.py
@IDE: PyCharm
@Description: 从 API 返回的病人列表 JSON 中提取关键字段，生成便于批量处理的配置 JSON。
"""
import json
import re
from typing import Dict, Any, List

# ========== 常量配置（根据实际情况修改）==========
CONSTANTS = {
    "projectId": "first",
    "siteName": "first",
    "tenantId": "019cdbd3-4741-752e-bf34-9e436d752aa5",
    "siteId": "019cdbd3-4741-753e-a233-bd8b052c7790",
    "deviceSecret": "IK6U0dly3Uax33IXZz5wwf3Q5aI13bGI"
}


def normalize_subject_id(subject_id: str) -> str:
    """将 subjectId 标准化为不带后缀的原始 ID（例如移除 '-Rename2'）"""
    # 根据实际数据，移除常见的重命名后缀
    if subject_id:
        # 匹配 "-Rename数字" 后缀
        cleaned = re.sub(r'-Rename\d+$', '', subject_id)
        return cleaned
    return subject_id


def extract_patient_config(patient: Dict[str, Any]) -> Dict[str, Any]:
    """从单个病人对象中提取所需配置"""
    # 基础字段
    subject_id_raw = patient.get("subjectId", "")

    # subject_id = normalize_subject_id(subject_id_raw)
    subject_id=subject_id_raw

    # 提取设备信息（取第一个传感器和第一个设备）
    sensors = patient.get("sensors", [])
    if not sensors:
        raise ValueError(f"病人 {subject_id} 没有 sensors 数据")
    sensor = sensors[0]
    device_name = [sensor.get("identifier", "")]
    sensor_id = sensor.get("id", "")

    devices = patient.get("devices", [])
    if not devices:
        raise ValueError(f"病人 {subject_id} 没有 devices 数据")
    device_id = devices[0].get("id", "")

    # 提取 activeSession 中的 sessionId
    active_session = patient.get("activeSession", {})
    session_id = active_session.get("id", "")

    # 病人自身 ID
    patient_id = patient.get("id", "")

    # 组装配置
    config = {
        "projectId": CONSTANTS["projectId"],
        "subjectId": subject_id,
        "siteName": CONSTANTS["siteName"],
        "deviceName": device_name,
        "tenantId": CONSTANTS["tenantId"],
        "siteId": CONSTANTS["siteId"],
        "deviceId": device_id,
        "sensorId": sensor_id,
        "sessionId": session_id,
        "patientId": patient_id,
        "deviceSecret": CONSTANTS["deviceSecret"]
    }
    return config


def extract_all_patients(api_data: Dict[str, Any]) -> Dict[str, Any]:
    """从完整 API 响应中提取所有病人配置，按标准化 subjectId 为 key"""
    patients_list = api_data.get("data", [])
    result = {}
    for patient in patients_list:
        try:
            config = extract_patient_config(patient)
            key = config["subjectId"]  # 标准化后的 ID
            result[key] = config
        except Exception as e:
            print(f"警告：处理病人 {patient.get('subjectId', 'unknown')} 时出错: {e}")
            continue
    return result


def main():
    import sys
    # 读取输入 JSON（文件或标准输入）
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        # 从标准输入读取
        data = json.load(sys.stdin)

    # 提取配置
    patients_config = extract_all_patients(data)

    # 输出
    json.dump(patients_config, sys.stdout, indent=2, ensure_ascii=False)
    # 可选：打印统计信息到 stderr
    print(f"\n成功提取 {len(patients_config)} 个病人配置", file=sys.stderr)


if __name__ == "__main__":
    main()