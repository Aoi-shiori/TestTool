import requests
import json
import re
import logging
import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum

# 导入客户端
from webportal_client import WebPortalClient
from vcloud_client import VCloudClient


class OperationMode(Enum):
    """操作模式枚举"""
    CREATE_DEVICES = "create_devices"
    CREATE_PATIENTS = "create_patients"
    CREATE_BOTH = "create_both"
    BIND = "bind"
    UNBIND = "unbind"
    DELETE = "delete"
    QUERY_INFO = "query_info"


class TestDataManager:
    def __init__(self):
        # 设置日志
        self.setup_logging()
        self.logger = logging.getLogger(__name__)

        self.base_dir = "./test_data/"
        self.patient_file = self.base_dir + "patients.json"
        self.device_file = self.base_dir + "devices.json"
        self.patient_device_file = self.base_dir + "patient_devices.json"
        self.excel_file = self.base_dir + "base_data.xlsx"

        # 初始化客户端
        self.webportal_client = WebPortalClient(
            base_url="https://webportal-dev.vivalink.com/api/backend",
            auth_url="https://webportal-dev.vivalink.com/api/backend/authentication",
            email="jun@vivalink.com.cn",
            password="Jun@1234"
        )

        self.vcloud_client = VCloudClient(
            base_url="https://vcloud-test.vivalink.com",
            auth_url="https://vcloud-test.vivalink.com/auth",
            client_id="617070e40daf63ba334ece90d1",
            client_key="@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF",
            tenant="Test_340"
        )

        # 初始化目录
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

        self.logger.info("TestDataManager初始化完成")

    def setup_logging(self):
        """配置日志记录"""
        # 创建日志目录
        log_dir = "./logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # 设置日志文件名（按日期）
        log_file = f"{log_dir}/test_data_manager_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(thread)d %(filename)s[line:%(lineno)d]->%(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

    def execute_operation(self, mode: OperationMode, config: Dict[str, Any]) -> bool:
        """
        执行指定操作

        参数:
        mode: 操作模式
        config: 配置参数
        """
        self.logger.info(f"开始执行操作: {mode.value}")

        try:
            if mode == OperationMode.CREATE_DEVICES:
                return self.create_devices(config)
            elif mode == OperationMode.CREATE_PATIENTS:
                return self.create_patients(config)
            elif mode == OperationMode.CREATE_BOTH:
                return self.create_both(config)
            elif mode == OperationMode.BIND:
                return self.bind_devices(config)
            elif mode == OperationMode.UNBIND:
                return self.unbind_devices(config)
            elif mode == OperationMode.DELETE:
                return self.delete_resources(config)
            elif mode == OperationMode.QUERY_INFO:
                return self.query_binding_info(config)
            else:
                self.logger.error(f"不支持的操作模式: {mode}")
                return False
        except Exception as e:
            self.logger.exception(f"执行操作时发生异常: {str(e)}")
            return False

    def create_devices(self, config: Dict[str, Any]) -> bool:
        """
        创建设备

        参数:
        config: 配置参数，包含设备规则和数量
        """
        self.logger.info("开始创建设备")

        # 从配置中获取设备规则和数量
        device_rules = config.get("device_rules", [])
        count = config.get("count", 1)

        if not device_rules:
            self.logger.error("未提供设备规则")
            return False

        # 生成设备列表
        devices = []
        for i in range(count):
            for rule in device_rules:
                device_type = rule.get("type", "")
                identifier_pattern = rule.get("pattern", "")

                # 根据模式生成设备标识符
                identifier = identifier_pattern.replace("{index}", str(i + 1).zfill(2))
                devices.append({
                    "type": device_type,
                    "identifier": identifier
                })

        # 保存到Excel
        try:
            df = pd.DataFrame(devices)

            # 如果文件已存在，读取并更新设备sheet
            if os.path.exists(self.excel_file):
                with pd.ExcelWriter(self.excel_file, mode='a', if_sheet_exists='replace') as writer:
                    df.to_excel(writer, sheet_name='devices', index=False)
            else:
                df.to_excel(self.excel_file, sheet_name='devices', index=False)

            self.logger.info(f"成功创建 {len(devices)} 个设备并保存到Excel")
            return True
        except Exception as e:
            self.logger.exception(f"保存设备到Excel时发生异常: {str(e)}")
            return False

    def create_patients(self, config: Dict[str, Any]) -> bool:
        """
        创建病人

        参数:
        config: 配置参数，包含病人规则和数量
        """
        self.logger.info("开始创建病人")

        # 从配置中获取病人规则和数量
        patient_rules = config.get("patient_rules", {})
        count = config.get("count", 1)

        if not patient_rules:
            self.logger.error("未提供病人规则")
            return False

        # 生成病人列表
        patients = []
        for i in range(count):
            internal_id_pattern = patient_rules.get("internalId_pattern", "")
            first_name_pattern = patient_rules.get("firstName_pattern", "")
            last_name_pattern = patient_rules.get("lastName_pattern", "")

            # 根据模式生成病人信息
            internal_id = internal_id_pattern.replace("{index}", str(i + 1).zfill(2))
            first_name = first_name_pattern.replace("{index}", str(i + 1).zfill(2))
            last_name = last_name_pattern.replace("{index}", str(i + 1).zfill(2))

            patient = {
                "internalId": internal_id,
                "firstName": first_name,
                "lastName": last_name,
                "birthDate": patient_rules.get("birthDate", "1990/01/01"),
                "monitoringProtocol": patient_rules.get("monitoringProtocol",
                                                        "Quick Patient Assignment Monitoring Protocol"),
                "physicianFirstName": patient_rules.get("physicianFirstName", "Jun"),
                "physicianLastName": patient_rules.get("physicianLastName", "Guo"),
                "gender": patient_rules.get("gender", "Male"),
                "height": patient_rules.get("height", 170),
                "weight": patient_rules.get("weight", 70),
                "rt_rpm_patient": patient_rules.get("rt_rpm_patient", "No"),
                "rpm_group": patient_rules.get("rpm_group", "")
            }
            patients.append(patient)

        # 保存到Excel
        try:
            df = pd.DataFrame(patients)

            # 如果文件已存在，读取并更新病人sheet
            if os.path.exists(self.excel_file):
                with pd.ExcelWriter(self.excel_file, mode='a', if_sheet_exists='replace') as writer:
                    df.to_excel(writer, sheet_name='patients', index=False)
            else:
                df.to_excel(self.excel_file, sheet_name='patients', index=False)

            self.logger.info(f"成功创建 {len(patients)} 个病人并保存到Excel")
            return True
        except Exception as e:
            self.logger.exception(f"保存病人到Excel时发生异常: {str(e)}")
            return False

    def create_both(self, config: Dict[str, Any]) -> bool:
        """
        同时创建病人和设备

        参数:
        config: 配置参数，包含病人和设备规则和数量
        """
        self.logger.info("开始同时创建病人和设备")

        # 分别创建病人和设备
        patient_success = self.create_patients(config)
        device_success = self.create_devices(config)

        return patient_success and device_success

    def bind_devices(self, config: Dict[str, Any]) -> bool:
        """
        绑定设备

        参数:
        config: 配置参数
        """
        self.logger.info("开始绑定设备")

        # 获取认证token
        if not self.webportal_client.get_token():
            self.logger.error("无法获取WebPortal token，退出")
            return False

        if not self.vcloud_client.get_token():
            self.logger.error("无法获取Vcloud token，退出")
            return False

        # 从Excel读取数据
        patients, devices = self.read_data_from_excel(config)

        if not patients and not devices:
            self.logger.error("未找到病人或设备数据")
            return False

        # 创建或查找病人和设备
        patient_map = {}  # internalId -> webportal_id
        device_map = {}  # identifier -> webportal_id

        # 处理病人
        for patient in patients:
            patient_id = patient['internalId']
            self.logger.info(f"处理病人: {patient_id}")

            # 查找是否已存在
            existing_patient = self.webportal_client.find_patient_by_internal_id(patient_id)
            if existing_patient:
                patient_map[patient_id] = existing_patient.get("_id")
                self.logger.info(f"病人已存在: {patient_id}, ID: {patient_map[patient_id]}")
            else:
                # 创建新病人
                result = self.webportal_client.create_patient(patient)
                if result:
                    patient_map[patient_id] = result.get("_id")
                    self.logger.info(f"创建新病人: {patient_id}, ID: {patient_map[patient_id]}")

                    # 在Vcloud中注册病人
                    if self.vcloud_client.register_patient(patient):
                        self.logger.info(f"成功在Vcloud中注册病人: {patient_id}")
                    else:
                        self.logger.error(f"在Vcloud中注册病人失败: {patient_id}")
                else:
                    self.logger.error(f"创建病人失败: {patient_id}")

        # 处理设备
        for device in devices:
            device_id = device['identifier']
            self.logger.info(f"处理设备: {device_id}")

            # 查找是否已存在
            existing_device = self.webportal_client.find_device_by_identifier(device_id)
            if existing_device:
                device_map[device_id] = existing_device.get("_id")
                self.logger.info(f"设备已存在: {device_id}, ID: {device_map[device_id]}")
            else:
                # 创建新设备
                result = self.webportal_client.create_device(device)
                if result:
                    device_map[device_id] = result.get("_id")
                    self.logger.info(f"创建新设备: {device_id}, ID: {device_map[device_id]}")
                else:
                    self.logger.error(f"创建设备失败: {device_id}")

        # 绑定匹配的病人和设备
        bound_pairs = []

        for patient_id, patient_webportal_id in patient_map.items():
            for device_id, device_webportal_id in device_map.items():
                if self.match_patient_and_device(patient_id, device_id):
                    self.logger.info(f"匹配成功: 病人 {patient_id} - 设备 {device_id}")

                    # 检查是否已绑定
                    if self.webportal_client.is_device_bound_to_patient(patient_webportal_id, device_webportal_id):
                        self.logger.info(f"设备已绑定: {device_id} -> {patient_id}")
                        bound_pairs.append((patient_id, device_id))
                        continue

                    # 在WebPortal中绑定
                    if self.webportal_client.bind_device_to_patient(patient_webportal_id, device_webportal_id):
                        self.logger.info(f"WebPortal绑定成功: {patient_id} - {device_id}")
                        bound_pairs.append((patient_id, device_id))
                    else:
                        self.logger.error(f"WebPortal绑定失败: {patient_id} - {device_id}")

                    # 在Vcloud中绑定
                    if self.vcloud_client.bind_device(device_id, patient_id):
                        self.logger.info(f"Vcloud绑定成功: {patient_id} - {device_id}")
                    else:
                        self.logger.error(f"Vcloud绑定失败: {patient_id} - {device_id}")

        # 获取并保存所有病人设备绑定信息
        self.save_patient_device_info()

        # 输出绑定结果
        self.logger.info(f"绑定完成，共 {len(bound_pairs)} 对绑定关系:")
        for patient_id, device_id in bound_pairs:
            self.logger.info(f"  {patient_id} <-> {device_id}")

        return True

    def unbind_devices(self, config: Dict[str, Any]) -> bool:
        """
        解绑设备

        参数:
        config: 配置参数
        """
        self.logger.info("开始解绑设备")

        # 获取认证token
        if not self.webportal_client.get_token():
            self.logger.error("无法获取WebPortal token，退出")
            return False

        # 从Excel读取数据
        patients, devices = self.read_data_from_excel(config)

        if not patients and not devices:
            self.logger.error("未找到病人或设备数据")
            return False

        # 查找病人和设备
        patient_map = {}  # internalId -> webportal_id
        device_map = {}  # identifier -> webportal_id

        # 查找病人
        for patient in patients:
            patient_id = patient['internalId']
            self.logger.info(f"查找病人: {patient_id}")

            existing_patient = self.webportal_client.find_patient_by_internal_id(patient_id)
            if existing_patient:
                patient_map[patient_id] = existing_patient.get("_id")
                self.logger.info(f"找到病人: {patient_id}, ID: {patient_map[patient_id]}")
            else:
                self.logger.warning(f"未找到病人: {patient_id}")

        # 查找设备
        for device in devices:
            device_id = device['identifier']
            self.logger.info(f"查找设备: {device_id}")

            existing_device = self.webportal_client.find_device_by_identifier(device_id)
            if existing_device:
                device_map[device_id] = existing_device.get("_id")
                self.logger.info(f"找到设备: {device_id}, ID: {device_map[device_id]}")
            else:
                self.logger.warning(f"未找到设备: {device_id}")

        # 解绑设备
        unbound_pairs = []

        for patient_id, patient_webportal_id in patient_map.items():
            for device_id, device_webportal_id in device_map.items():
                if self.match_patient_and_device(patient_id, device_id):
                    self.logger.info(f"匹配成功: 病人 {patient_id} - 设备 {device_id}")

                    # 检查是否已绑定
                    if not self.webportal_client.is_device_bound_to_patient(patient_webportal_id, device_webportal_id):
                        self.logger.info(f"设备未绑定: {device_id} -> {patient_id}")
                        unbound_pairs.append((patient_id, device_id))
                        continue

                    # 解绑设备
                    if self.webportal_client.unbind_device_from_patient(patient_webportal_id, device_webportal_id):
                        self.logger.info(f"解绑成功: {patient_id} - {device_id}")
                        unbound_pairs.append((patient_id, device_id))
                    else:
                        self.logger.error(f"解绑失败: {patient_id} - {device_id}")

        # 输出解绑结果
        self.logger.info(f"解绑完成，共 {len(unbound_pairs)} 对解绑关系:")
        for patient_id, device_id in unbound_pairs:
            self.logger.info(f"  {patient_id} <-> {device_id}")

        return True

    def delete_resources(self, config: Dict[str, Any]) -> bool:
        """
        删除资源（病人和设备）

        参数:
        config: 配置参数
        """
        self.logger.info("开始删除资源")

        # 获取认证token
        if not self.webportal_client.get_token():
            self.logger.error("无法获取WebPortal token，退出")
            return False

        # 从Excel读取数据
        patients, devices = self.read_data_from_excel(config)

        if not patients and not devices:
            self.logger.error("未找到病人或设备数据")
            return False

        # 删除病人
        deleted_patients = []
        for patient in patients:
            patient_id = patient['internalId']
            self.logger.info(f"查找病人: {patient_id}")

            existing_patient = self.webportal_client.find_patient_by_internal_id(patient_id)
            if existing_patient:
                patient_webportal_id = existing_patient.get("_id")
                if self.webportal_client.delete_patient(patient_webportal_id):
                    self.logger.info(f"删除病人成功: {patient_id}")
                    deleted_patients.append(patient_id)
                else:
                    self.logger.error(f"删除病人失败: {patient_id}")
            else:
                self.logger.warning(f"未找到病人: {patient_id}")

        # 删除设备
        deleted_devices = []
        for device in devices:
            device_id = device['identifier']
            self.logger.info(f"查找设备: {device_id}")

            existing_device = self.webportal_client.find_device_by_identifier(device_id)
            if existing_device:
                device_webportal_id = existing_device.get("_id")
                if self.webportal_client.delete_device(device_webportal_id):
                    self.logger.info(f"删除设备成功: {device_id}")
                    deleted_devices.append(device_id)
                else:
                    self.logger.error(f"删除设备失败: {device_id}")
            else:
                self.logger.warning(f"未找到设备: {device_id}")

        # 输出删除结果
        self.logger.info(f"删除完成，共 {len(deleted_patients)} 个病人和 {len(deleted_devices)} 个设备被删除")

        return True

    def query_binding_info(self, config: Dict[str, Any]) -> bool:
        """
        查询绑定信息

        参数:
        config: 配置参数
        """
        self.logger.info("开始查询绑定信息")

        # 获取认证token
        if not self.webportal_client.get_token():
            self.logger.error("无法获取WebPortal token，退出")
            return False

        # 从Excel读取数据
        patients, devices = self.read_data_from_excel(config)

        # 获取所有病人设备绑定信息
        patient_device_info = self.webportal_client.get_all_patient_device_info()

        # 如果提供了特定的病人和设备，筛选相关信息
        if patients or devices:
            filtered_info = []
            for info in patient_device_info:
                if patients and info["internalId"] in [p["internalId"] for p in patients]:
                    filtered_info.append(info)
                elif devices and any(device in info["device_list"] for device in [d["identifier"] for d in devices]):
                    filtered_info.append(info)

            patient_device_info = filtered_info

        # 保存到JSON文件
        self.save_patient_device_info(patient_device_info)

        # 输出查询结果
        self.logger.info(f"查询完成，共获取 {len(patient_device_info)} 个病人的绑定信息")

        return True

    def read_data_from_excel(self, config: Dict[str, Any]) -> Tuple[List[Dict], List[Dict]]:
        """
        从Excel读取数据

        参数:
        config: 配置参数

        返回:
        (patients, devices) 元组
        """
        self.logger.info("从Excel读取数据")

        patients = []
        devices = []

        try:
            # 读取Excel文件
            if not os.path.exists(self.excel_file):
                self.logger.error("Excel文件不存在")
                return [], []

            # 读取病人数据
            if "read_patients" in config.get("options", []):
                try:
                    patients_df = pd.read_excel(self.excel_file, sheet_name='patients')
                    patients = patients_df.to_dict('records')
                    self.logger.info(f"从Excel读取 {len(patients)} 个病人")
                except Exception as e:
                    self.logger.warning(f"读取病人数据失败: {str(e)}")

            # 读取设备数据
            if "read_devices" in config.get("options", []):
                try:
                    devices_df = pd.read_excel(self.excel_file, sheet_name='devices')
                    devices = devices_df.to_dict('records')
                    self.logger.info(f"从Excel读取 {len(devices)} 个设备")
                except Exception as e:
                    self.logger.warning(f"读取设备数据失败: {str(e)}")

            # 读取绑定数据（病人和设备在同一sheet）
            if "read_binding" in config.get("options", []):
                try:
                    binding_df = pd.read_excel(self.excel_file, sheet_name='binding')
                    if 'patient' in binding_df.columns and 'device' in binding_df.columns:
                        # 转换为病人和设备列表
                        patients = [{"internalId": row['patient']} for _, row in binding_df.iterrows()]
                        devices = [{"identifier": row['device']} for _, row in binding_df.iterrows()]
                        self.logger.info(f"从Excel读取 {len(patients)} 对绑定关系")
                except Exception as e:
                    self.logger.warning(f"读取绑定数据失败: {str(e)}")

            return patients, devices
        except Exception as e:
            self.logger.exception(f"从Excel读取数据时发生异常: {str(e)}")
            return [], []

    def match_patient_and_device(self, patient_id: str, device_id: str) -> bool:
        """
        根据规则匹配病人和设备

        匹配规则:
        1. 单设备测试: 病人ID中包含设备类型和编号 (如 ECGRec_202509/JD0001 -> J20250905_ECG_01)
        2. 多设备混合测试: 一个病人绑定多个设备，设备标识符末尾数字与病人ID末尾数字相同
        """
        self.logger.debug(f"尝试匹配病人 {patient_id} 和设备 {device_id}")

        # 规则1: 单设备测试匹配
        # 提取设备类型和编号 (如 ECGRec_202509/JD0001 -> ECG, 01)
        device_match = re.search(r'([A-Za-z]+)[_/]J?(\d+)(\d{2})', device_id)
        if device_match:
            device_type = device_match.group(1)  # 设备类型 (ECGRec, O2, BP5C, Temp, CGL)
            device_number = device_match.group(3)  # 设备编号

            # 设备类型映射
            type_map = {
                "ECGRec": "ECG",
                "O2": "SPO2",
                "BP5C": "BP",
                "Temp": "TEMP",
                "CGL": "CGL"
            }

            device_type_short = type_map.get(device_type, device_type)

            # 检查病人ID是否包含设备类型和相同编号
            pattern = f".*{device_type_short}.*{device_number}$"
            if re.match(pattern, patient_id):
                self.logger.info(f"单设备匹配成功: 病人 {patient_id} - 设备 {device_id}")
                return True

        # 规则2: 多设备混合测试匹配
        # 提取设备标识符末尾的数字 (如 ECGRec_202509/J050001 -> 001)
        device_number_match = re.search(r'(\d{2,3})$', device_id)
        patient_number_match = re.search(r'(\d{2,3})$', patient_id)

        if device_number_match and patient_number_match:
            device_number = device_number_match.group(1)
            patient_number = patient_number_match.group(1)

            if device_number == patient_number:
                self.logger.info(f"多设备匹配成功: 病人 {patient_id} - 设备 {device_id}")
                return True

        # 规则3: 简单编号匹配 (最后2-3位数字相同)
        device_digits = re.findall(r'\d+', device_id)
        patient_digits = re.findall(r'\d+', patient_id)

        if device_digits and patient_digits:
            device_last_digits = device_digits[-1][-2:]  # 取最后2位
            patient_last_digits = patient_digits[-1][-2:]  # 取最后2位

            if device_last_digits == patient_last_digits:
                self.logger.info(f"简单编号匹配成功: 病人 {patient_id} - 设备 {device_id}")
                return True

        self.logger.debug(f"病人 {patient_id} 和设备 {device_id} 不匹配")
        return False

    def save_patient_device_info(self, patient_device_info: List[Dict] = None):
        """保存病人和设备绑定信息到JSON文件"""
        try:
            if patient_device_info is None:
                # 如果没有提供数据，则从WebPortal获取
                patient_device_info = self.webportal_client.get_all_patient_device_info()

            with open(self.patient_device_file, 'w', encoding='utf-8') as f:
                json.dump(patient_device_info, f, indent=2, ensure_ascii=False)
            self.logger.info(f"成功保存病人设备绑定信息到: {self.patient_device_file}")
        except Exception as e:
            self.logger.exception(f"保存病人设备绑定信息时发生异常: {str(e)}")

    def quick_patient_assignment(self, config: Dict[str, Any]) -> bool:
        """
        快速病人分配

        参数:
        config: 配置参数
        """
        self.logger.info("开始快速病人分配")

        # 获取认证token
        if not self.webportal_client.get_token():
            self.logger.error("无法获取WebPortal token，退出")
            return False

        if not self.vcloud_client.get_token():
            self.logger.error("无法获取Vcloud token，退出")
            return False

        # 从Excel读取数据
        patients, devices = self.read_data_from_excel(config)

        if not patients or not devices:
            self.logger.error("未找到病人或设备数据")
            return False

        # 快速病人分配：先在WebPortal添加设备，然后通过Vcloud接口同步创建病人和设备
        success_count = 0
        for patient in patients:
            for device in devices:
                if self.match_patient_and_device(patient["internalId"], device["identifier"]):
                    self.logger.info(f"匹配成功: 病人 {patient['internalId']} - 设备 {device['identifier']}")

                    # 1. 在WebPortal中添加设备
                    device_result = self.webportal_client.create_device(device)
                    if not device_result:
                        self.logger.error(f"在WebPortal中添加设备失败: {device['identifier']}")
                        continue

                    # 2. 通过Vcloud接口同步创建病人和设备
                    if self.vcloud_client.quick_patient_assignment(device, patient):
                        self.logger.info(f"快速病人分配成功: {patient['internalId']} - {device['identifier']}")
                        success_count += 1
                    else:
                        self.logger.error(f"快速病人分配失败: {patient['internalId']} - {device['identifier']}")

        # 保存绑定信息
        self.save_patient_device_info()

        self.logger.info(f"快速病人分配完成，成功 {success_count} 对绑定关系")
        return success_count > 0


# 使用示例
if __name__ == "__main__":
    manager = TestDataManager()

    # 示例3: 绑定设备
    bind_config = {
        "options": ["read_patients", "base_data.xlsx"]
    }

    manager.execute_operation(OperationMode.BIND, bind_config)


    # # 示例1: 创建设备
    # device_config = {
    #     "device_rules": [
    #         {"type": "ECG", "pattern": "ECGRec_202509/JD00{index}"},
    #         {"type": "BP", "pattern": "BP5C_J20250906{index}"}
    #     ],
    #     "count": 5
    # }
    # manager.execute_operation(OperationMode.CREATE_DEVICES, device_config)
    #
    # # 示例2: 创建病人
    # patient_config = {
    #     "patient_rules": {
    #         "internalId_pattern": "J20250905_ECG_{index}",
    #         "firstName_pattern": "J20250905_ECG_{index}",
    #         "lastName_pattern": "J20250905_ECG_{index}",
    #         "birthDate": "1990/01/01",
    #         "gender": "Male",
    #         "height": 170,
    #         "weight": 70
    #     },
    #     "count": 5
    # }
    # manager.execute_operation(OperationMode.CREATE_PATIENTS, patient_config)
    #
    # # 示例3: 绑定设备
    # bind_config = {
    #     "options": ["read_patients", "read_devices"]
    # }
    # manager.execute_operation(OperationMode.BIND, bind_config)
    #
    # # 示例4: 查询绑定信息
    # query_config = {
    #     "options": ["read_patients", "read_devices"]
    # }
    # manager.execute_operation(OperationMode.QUERY_INFO, query_config)
    #
    # # 示例5: 解绑设备
    # unbind_config = {
    #     "options": ["read_patients", "read_devices"]
    # }
    # manager.execute_operation(OperationMode.UNBIND, unbind_config)
    #
    # # 示例6: 删除资源
    # delete_config = {
    #     "options": ["read_patients", "read_devices"]
    # }
    # manager.execute_operation(OperationMode.DELETE, delete_config)
    #
    # # 示例7: 快速病人分配
    # qpa_config = {
    #     "options": ["read_patients", "read_devices"]
    # }
    # manager.quick_patient_assignment(qpa_config)