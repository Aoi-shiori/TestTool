import requests
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime


class WebPortalClient:
    def __init__(self, base_url: str, auth_url: str, email: str, password: str):
        self.logger = logging.getLogger(__name__)
        self.base_url = base_url
        self.auth_url = auth_url
        self.email = email
        self.password = password
        self.token = None
        self.token_expiry = None

    def get_token(self, force_refresh=False) -> Optional[str]:
        """获取WebPortal认证token，如果已有且未过期则直接使用"""
        if self.token and not force_refresh:
            # 检查token是否过期
            if self.token_expiry and self.token_expiry > datetime.now():
                self.logger.info("使用现有的WebPortal token")
                return self.token

        self.logger.info("尝试获取WebPortal认证token")

        try:
            payload = {
                "strategy": "local",
                "email": self.email,
                "password": self.password
            }
            headers = {"Content-Type": "application/json"}

            self.logger.debug(f"发送认证请求到: {self.auth_url}")
            response = requests.post(self.auth_url, json=payload, headers=headers)

            if response.status_code == 200:
                data = response.json()
                self.token = data.get("accessToken")

                # 假设token有效期为1小时（根据实际情况调整）
                self.token_expiry = datetime.now() + timedelta(hours=1)

                self.logger.info("成功获取WebPortal token")
                return self.token
            else:
                self.logger.error(f"获取WebPortal token失败: HTTP {response.status_code} - {response.text}")
                return None
        except Exception as e:
            self.logger.exception(f"获取WebPortal token时发生异常: {str(e)}")
            return None

    def request(self, method, url, **kwargs):
        """执行WebPortal请求，如果token过期会自动刷新"""
        # 获取token
        token = self.get_token()
        if not token:
            return None

        # 添加认证头
        headers = kwargs.get('headers', {})
        headers['Authorization'] = f'Bearer {token}'
        kwargs['headers'] = headers

        try:
            response = requests.request(method, url, **kwargs)

            # 检查是否token过期
            if response.status_code == 401:
                self.logger.warning("Token可能已过期，尝试刷新")
                token = self.get_token(force_refresh=True)
                if token:
                    headers['Authorization'] = f'Bearer {token}'
                    response = requests.request(method, url, **kwargs)

            return response
        except Exception as e:
            self.logger.exception(f"WebPortal请求失败: {str(e)}")
            return None

    def find_patient_by_internal_id(self, internal_id: str) -> Optional[Dict]:
        """根据internalId查找病人"""
        self.logger.info(f"查找病人: {internal_id}")

        # 查询病人
        patients_data = self.query_patients(query_value=internal_id)
        if not patients_data or "data" not in patients_data:
            return None

        # 查找匹配的病人
        for patient in patients_data["data"]:
            if patient.get("internalId") == internal_id:
                self.logger.info(f"找到病人: {internal_id}, ID: {patient.get('_id')}")
                return patient

        self.logger.info(f"未找到病人: {internal_id}")
        return None

    def find_device_by_identifier(self, identifier: str) -> Optional[Dict]:
        """根据identifier查找设备"""
        self.logger.info(f"查找设备: {identifier}")

        # 查询所有设备
        devices_data = self.query_all_devices()
        if not devices_data or "data" not in devices_data:
            return None

        # 查找匹配的设备
        for device in devices_data["data"]:
            if device.get("identifier") == identifier:
                self.logger.info(f"找到设备: {identifier}, ID: {device.get('_id')}")
                return device

        self.logger.info(f"未找到设备: {identifier}")
        return None

    def query_all_devices(self) -> Optional[Dict]:
        """查询所有设备信息"""
        self.logger.info("查询所有设备信息")

        url = f"{self.base_url}/devices?$limit=1000"

        response = self.request("GET", url)
        if not response:
            return None

        if response.status_code == 200:
            result = response.json()
            self.logger.info(f"成功查询到 {result.get('total', 0)} 个设备")
            return result
        else:
            self.logger.error(f"查询设备失败: HTTP {response.status_code} - {response.text}")
            return None

    def get_patient_devices(self, patient_id: str) -> List[Dict]:
        """获取病人已绑定的设备"""
        self.logger.info(f"获取病人已绑定的设备: {patient_id}")

        url = f"{self.base_url}/patientDevices?patientId={patient_id}"

        response = self.request("GET", url)
        if not response:
            self.logger.error(f"获取病人设备请求失败: {patient_id}")
            return []

        if response.status_code == 200:
            result = response.json()
            # 确保devices是列表，即使接口返回null也会转为空列表
            devices = result.get("data", []) or []
            self.logger.info(f"获取到病人 {patient_id} 的 {len(devices)} 个设备")
            return devices
        else:
            self.logger.error(f"获取病人设备失败: HTTP {response.status_code} - {response.text}")
            return []

    def is_device_bound_to_patient(self, patient_id: str, device_id: str) -> bool:
        """检查设备是否已绑定到病人"""
        self.logger.info(f"检查设备 {device_id} 是否已绑定到病人 {patient_id}")

        devices = self.get_patient_devices(patient_id)
        for device in devices:
            if device.get("deviceId") == device_id:
                self.logger.info(f"设备 {device_id} 已绑定到病人 {patient_id}")
                return True

        self.logger.info(f"设备 {device_id} 未绑定到病人 {patient_id}")
        return False

    def create_patient(self, patient_data: Dict) -> Optional[Dict]:
        """在WebPortal中创建病人"""
        self.logger.info(f"尝试在WebPortal中创建病人: {patient_data.get('internalId', 'Unknown')}")

        url = f"{self.base_url}/patients"

        # 转换日期格式
        birth_date = patient_data.get("birthDate", "").replace("/", "-")
        if birth_date:
            try:
                # 尝试解析日期并格式化为ISO格式
                date_obj = datetime.strptime(birth_date, "%Y-%m-%d")
                birth_date = date_obj.strftime("%Y-%m-%d 00:00:00")
            except:
                birth_date = "1970-01-01 00:00:00"
                self.logger.warning(f"病人 {patient_data.get('internalId')} 的生日格式无效，使用默认值")

        payload = {
            "firstName": patient_data.get("firstName", ""),
            "lastName": patient_data.get("lastName", ""),
            "internalId": patient_data.get("internalId", ""),
            "birthDate": birth_date,
            "timeZoneName": "Asia/Shanghai",
            "monitoringProtocol": "68bb994c47a53c345185334a",  # 需要根据实际情况获取
            "physician": "65fa8e73bf4a3d68757b4361",  # 需要根据实际情况获取
            "isOpenPrescribingPhysician": False,
            "isRTRPMPatient": patient_data.get("rt_rpm_patient", "No") == "Yes",
            "codes": [],
            "isRpm": False,
            "gender": patient_data.get("gender", "Male"),
            "weight": patient_data.get("weight", 70),
            "height": patient_data.get("height", 170)
        }

        headers = {"Content-Type": "application/json"}

        response = self.request("POST", url, json=payload, headers=headers)
        if not response:
            return None

        if response.status_code == 201:
            result = response.json()
            self.logger.info(f"成功创建病人: {patient_data.get('internalId')}, ID: {result.get('_id')}")
            return result
        else:
            self.logger.error(f"创建病人失败: HTTP {response.status_code} - {response.text}")
            return None

    def create_device(self, device_data: Dict) -> Optional[Dict]:
        """在WebPortal中添加设备"""
        device_id = device_data.get("identifier", "Unknown")
        self.logger.info(f"尝试在WebPortal中添加设备: {device_id}")

        url = f"{self.base_url}/devices"

        # 设备类型映射
        device_type_map = {
            "ECG": 100,
            "SpO2": 101,
            "BP": 102,
            "Temperature": 103,
            "CGM": 104
        }

        device_type = device_data.get("type", "")
        device_type_code = device_type_map.get(device_type, 100)

        payload = {
            "type": device_type_code,
            "identifier": device_data.get("identifier", "")
        }

        headers = {"Content-Type": "application/json"}

        response = self.request("POST", url, json=payload, headers=headers)
        if not response:
            return None

        if response.status_code == 200:
            result = response.json()
            self.logger.info(f"成功添加设备: {device_id}, ID: {result.get('_id')}")
            return result
        else:
            self.logger.error(f"添加设备失败: HTTP {response.status_code} - {response.text}")
            return None

    def bind_device_to_patient(self, patient_id: str, device_id: str) -> bool:
        """将设备绑定到病人"""
        self.logger.info(f"尝试将设备 {device_id} 绑定到病人 {patient_id}")

        # 先检查是否已绑定
        if self.is_device_bound_to_patient(patient_id, device_id):
            self.logger.info(f"设备 {device_id} 已绑定到病人 {patient_id}，无需重复绑定")
            return True

        url = f"{self.base_url}/patientDevices"

        payload = {
            "patient": patient_id,
            "device": device_id
        }

        headers = {"Content-Type": "application/json"}

        response = self.request("POST", url, json=payload, headers=headers)
        if not response:
            return False

        if response.status_code == 200:
            result = response.json()
            success = result.get("code", -1) == 0

            if success:
                self.logger.info(f"成功绑定设备 {device_id} 到病人 {patient_id}")
            else:
                self.logger.error(f"绑定设备失败: {result.get('message', 'Unknown error')}")

            return success
        else:
            self.logger.error(f"绑定设备失败: HTTP {response.status_code} - {response.text}")
            return False

    def unbind_device_from_patient(self, patient_id: str, device_id: str) -> bool:
        """将设备从病人解绑"""
        self.logger.info(f"尝试将设备 {device_id} 从病人 {patient_id} 解绑")

        url = f"{self.base_url}/patientDevices?patient={patient_id}&device={device_id}"

        response = self.request("DELETE", url)
        if not response:
            return False

        success = response.status_code == 200
        if success:
            self.logger.info(f"成功解绑设备 {device_id} 从病人 {patient_id}")
        else:
            self.logger.error(f"解绑设备失败: HTTP {response.status_code} - {response.text}")

        return success

    def delete_patient(self, patient_id: str) -> bool:
        """删除病人"""
        self.logger.info(f"尝试删除病人: {patient_id}")

        url = f"{self.base_url}/patients/{patient_id}"

        response = self.request("DELETE", url)
        if not response:
            return False

        success = response.status_code == 200
        if success:
            self.logger.info(f"成功删除病人: {patient_id}")
        else:
            self.logger.error(f"删除病人失败: HTTP {response.status_code} - {response.text}")

        return success

    def delete_device(self, device_id: str) -> bool:
        """删除设备"""
        self.logger.info(f"尝试删除设备: {device_id}")

        url = f"{self.base_url}/devices/{device_id}"

        response = self.request("DELETE", url)
        if not response:
            return False

        success = response.status_code == 200
        if success:
            self.logger.info(f"成功删除设备: {device_id}")
        else:
            self.logger.error(f"删除设备失败: HTTP {response.status_code} - {response.text}")

        return success

    def query_patients(self, query_value: str = "", skip: int = 0, limit: int = 10) -> Optional[Dict]:
        """查询病人信息"""
        self.logger.info(f"查询病人信息: query_value={query_value}, skip={skip}, limit={limit}")

        url = f"{self.base_url}/patients?$skip={skip}&$limit={limit}&isManage=true"
        if query_value:
            url += f"&queryValue={query_value}"

        response = self.request("GET", url)
        if not response:
            return None

        if response.status_code == 200:
            result = response.json()
            self.logger.info(f"成功查询到 {result.get('total', 0)} 个病人")
            return result
        else:
            self.logger.error(f"查询病人失败: HTTP {response.status_code} - {response.text}")
            return None

    def get_all_patient_device_info(self) -> List[Dict]:
        """获取所有病人和设备的绑定信息"""
        self.logger.info("获取所有病人和设备的绑定信息")

        # 查询所有病人
        patients_data = self.query_patients(limit=1000)
        if not patients_data or "data" not in patients_data:
            self.logger.error("获取病人数据失败")
            return []

        patient_device_info = []

        for patient in patients_data["data"]:
            patient_id = patient.get("_id")
            internal_id = patient.get("internalId")
            first_name = patient.get("firstName")
            last_name = patient.get("lastName")

            if not patient_id:
                continue

            # 获取病人绑定的设备
            devices = self.get_patient_devices(patient_id)
            device_identifiers = []

            for device in devices:
                device_name = device.get("deviceName")
                if device_name:
                    device_identifiers.append(device_name)

            patient_info = {
                "patientName": f"{first_name} {last_name}",
                "patient_id": patient_id,
                "internalId": internal_id,
                "device_list": device_identifiers
            }

            patient_device_info.append(patient_info)

        return patient_device_info