import requests
import json
import re
import logging
import pandas as pd
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum


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
        self.webportal_token = None
        self.vcloud_token = None
        self.token_expiry = {}  # 存储token过期时间

        # WebPortal配置
        self.webportal_base_url = "https://webportal-dev.vivalink.com/api/backend"
        self.webportal_auth_url = f"{self.webportal_base_url}/authentication"
        self.webportal_email = "jun@vivalink.com.cn"
        self.webportal_password = "Jun@1234"

        # Vcloud配置
        self.vcloud_base_url = "https://vcloud-test.vivalink.com"
        self.vcloud_auth_url = f"{self.vcloud_base_url}/auth"
        self.vcloud_id = "617070e40daf63ba334ece90d1"
        self.vcloud_key = "@baIevnyO<iqo<r5L5VYK0BH[CFvJXUf0W4Y;WZF"

        # 租户信息
        self.tenant = "Test_340"

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
        if not self.get_webportal_token():
            self.logger.error("无法获取WebPortal token，退出")
            return False

        if not self.get_vcloud_token():
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
            existing_patient = self.find_patient_by_internal_id(patient_id)
            if existing_patient:
                patient_map[patient_id] = existing_patient.get("_id")
                self.logger.info(f"病人已存在: {patient_id}, ID: {patient_map[patient_id]}")
            else:
                # 创建新病人
                result = self.create_patient_in_webportal(patient)
                if result:
                    patient_map[patient_id] = result.get("_id")
                    self.logger.info(f"创建新病人: {patient_id}, ID: {patient_map[patient_id]}")
                else:
                    self.logger.error(f"创建病人失败: {patient_id}")

        # 处理设备
        for device in devices:
            device_id = device['identifier']
            self.logger.info(f"处理设备: {device_id}")

            # 查找是否已存在
            existing_device = self.find_device_by_identifier(device_id)
            if existing_device:
                device_map[device_id] = existing_device.get("_id")
                self.logger.info(f"设备已存在: {device_id}, ID: {device_map[device_id]}")
            else:
                # 创建新设备
                result = self.create_device_in_webportal(device)
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
                    if self.is_device_bound_to_patient(patient_webportal_id, device_webportal_id):
                        self.logger.info(f"设备已绑定: {device_id} -> {patient_id}")
                        bound_pairs.append((patient_id, device_id))
                        continue

                    # 在WebPortal中绑定
                    if self.bind_device_to_patient(patient_webportal_id, device_webportal_id):
                        self.logger.info(f"WebPortal绑定成功: {patient_id} - {device_id}")
                        bound_pairs.append((patient_id, device_id))
                    else:
                        self.logger.error(f"WebPortal绑定失败: {patient_id} - {device_id}")

                    # 在Vcloud中绑定
                    if self.bind_device_in_vcloud(device_id, patient_id):
                        self.logger.info(f"Vcloud绑定成功: {patient_id} - {device_id}")
                    else:
                        self.logger.error(f"Vcloud绑定失败: {patient_id} - {device_id}")

        # 获取并保存所有病人设备绑定信息
        self.get_all_patient_device_info()

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
        if not self.get_webportal_token():
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

            existing_patient = self.find_patient_by_internal_id(patient_id)
            if existing_patient:
                patient_map[patient_id] = existing_patient.get("_id")
                self.logger.info(f"找到病人: {patient_id}, ID: {patient_map[patient_id]}")
            else:
                self.logger.warning(f"未找到病人: {patient_id}")

        # 查找设备
        for device in devices:
            device_id = device['identifier']
            self.logger.info(f"查找设备: {device_id}")

            existing_device = self.find_device_by_identifier(device_id)
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
                    if not self.is_device_bound_to_patient(patient_webportal_id, device_webportal_id):
                        self.logger.info(f"设备未绑定: {device_id} -> {patient_id}")
                        unbound_pairs.append((patient_id, device_id))
                        continue

                    # 解绑设备
                    if self.unbind_device_from_patient(patient_webportal_id, device_webportal_id):
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
        if not self.get_webportal_token():
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

            existing_patient = self.find_patient_by_internal_id(patient_id)
            if existing_patient:
                patient_webportal_id = existing_patient.get("_id")
                if self.delete_patient(patient_webportal_id):
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

            existing_device = self.find_device_by_identifier(device_id)
            if existing_device:
                device_webportal_id = existing_device.get("_id")
                if self.delete_device(device_webportal_id):
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
        if not self.get_webportal_token():
            self.logger.error("无法获取WebPortal token，退出")
            return False

        # 从Excel读取数据
        patients, devices = self.read_data_from_excel(config)

        # 获取所有病人设备绑定信息
        patient_device_info = self.get_all_patient_device_info()

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

    # 以下是原有的工具方法，保持不变
    def get_webportal_token(self, force_refresh=False) -> Optional[str]:
        """获取WebPortal认证token，如果已有且未过期则直接使用"""
        # 实现略，与之前相同
        pass

    def get_vcloud_token(self, force_refresh=False) -> Optional[str]:
        """获取Vcloud认证token，如果已有且未过期则直接使用"""
        # 实现略，与之前相同
        pass

    def webportal_request(self, method, url, **kwargs):
        """执行WebPortal请求，如果token过期会自动刷新"""
        # 实现略，与之前相同
        pass

    def vcloud_request(self, method, url, **kwargs):
        """执行Vcloud请求，如果token过期会自动刷新"""
        # 实现略，与之前相同
        pass

    def find_patient_by_internal_id(self, internal_id: str) -> Optional[Dict]:
        """根据internalId查找病人"""
        # 实现略，与之前相同
        pass

    def find_device_by_identifier(self, identifier: str) -> Optional[Dict]:
        """根据identifier查找设备"""
        # 实现略，与之前相同
        pass

    def query_all_devices(self) -> Optional[Dict]:
        """查询所有设备信息"""
        # 实现略，与之前相同
        pass

    def get_patient_devices(self, patient_id: str) -> List[Dict]:
        """获取病人已绑定的设备"""
        # 实现略，与之前相同
        pass

    def is_device_bound_to_patient(self, patient_id: str, device_id: str) -> bool:
        """检查设备是否已绑定到病人"""
        # 实现略，与之前相同
        pass

    def create_patient_in_webportal(self, patient_data: Dict) -> Optional[Dict]:
        """在WebPortal中创建病人"""
        # 实现略，与之前相同
        pass

    def create_device_in_webportal(self, device_data: Dict) -> Optional[Dict]:
        """在WebPortal中添加设备"""
        # 实现略，与之前相同
        pass

    def bind_device_to_patient(self, patient_id: str, device_id: str) -> bool:
        """将设备绑定到病人"""
        # 实现略，与之前相同
        pass

    def unbind_device_from_patient(self, patient_id: str, device_id: str) -> bool:
        """将设备从病人解绑"""
        # 实现略，与之前相同
        pass

    def delete_patient(self, patient_id: str) -> bool:
        """删除病人"""
        # 实现略，与之前相同
        pass

    def delete_device(self, device_id: str) -> bool:
        """删除设备"""
        # 实现略，与之前相同
        pass

    def query_patients(self, query_value: str = "", skip: int = 0, limit: int = 10) -> Optional[Dict]:
        """查询病人信息"""
        # 实现略，与之前相同
        pass

    def match_patient_and_device(self, patient_id: str, device_id: str) -> bool:
        """根据规则匹配病人和设备"""
        # 实现略，与之前相同
        pass

    def save_patient_device_info(self, patient_device_info: List[Dict]):
        """保存病人和设备绑定信息到JSON文件"""
        # 实现略，与之前相同
        pass

    def get_all_patient_device_info(self) -> List[Dict]:
        """获取所有病人和设备的绑定信息"""
        # 实现略，与之前相同
        pass

    def bind_device_in_vcloud(self, sensor_id: str, subject_id: str,
                              record_time: int = None, timezone: int = 28800,
                              timezone_name: str = "Asia/Shanghai") -> bool:
        """在Vcloud系统中绑定设备到病人"""
        # 实现略，与之前相同
        pass


# 使用示例
if __name__ == "__main__":
    manager = TestDataManager()

    # 示例1: 创建设备
    device_config = {
        "device_rules": [
            {"type": "ECG", "pattern": "ECGRec_202509/JD00{index}"},
            {"type": "BP", "pattern": "BP5C_J20250906{index}"}
        ],
        "count": 5
    }
    manager.execute_operation(OperationMode.CREATE_DEVICES, device_config)

    # 示例2: 创建病人
    patient_config = {
        "patient_rules": {
            "internalId_pattern": "J20250905_ECG_{index}",
            "firstName_pattern": "J20250905_ECG_{index}",
            "lastName_pattern": "J20250905_ECG_{index}",
            "birthDate": "1990/01/01",
            "gender": "Male",
            "height": 170,
            "weight": 70
        },
        "count": 5
    }
    manager.execute_operation(OperationMode.CREATE_PATIENTS, patient_config)

    # 示例3: 绑定设备
    bind_config = {
        "options": ["read_patients", "read_devices"]
    }
    manager.execute_operation(OperationMode.BIND, bind_config)

    # 示例4: 查询绑定信息
    query_config = {
        "options": ["read_patients", "read_devices"]
    }
    manager.execute_operation(OperationMode.QUERY_INFO, query_config)

    # 示例5: 解绑设备
    unbind_config = {
        "options": ["read_patients", "read_devices"]
    }
    manager.execute_operation(OperationMode.UNBIND, unbind_config)

    # 示例6: 删除资源
    delete_config = {
        "options": ["read_patients", "read_devices"]
    }
    manager.execute_operation(OperationMode.DELETE, delete_config)