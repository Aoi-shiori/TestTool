import requests
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta


class VCloudClient:
    def __init__(self, base_url: str, auth_url: str, client_id: str, client_key: str, tenant: str):
        self.logger = logging.getLogger(__name__)
        self.base_url = base_url
        self.auth_url = auth_url
        self.client_id = client_id
        self.client_key = client_key
        self.tenant = tenant
        self.token = None
        self.token_expiry = None

    def get_token(self, force_refresh=False) -> Optional[str]:
        """获取Vcloud认证token，如果已有且未过期则直接使用"""
        if self.token and not force_refresh:
            # 检查token是否过期
            if self.token_expiry and self.token_expiry > datetime.now():
                self.logger.info("使用现有的Vcloud token")
                return self.token

        self.logger.info("尝试获取Vcloud认证token")

        try:
            payload = {
                "id": self.client_id,
                "key": self.client_key
            }
            headers = {"Content-Type": "application/json"}

            self.logger.debug(f"发送认证请求到: {self.auth_url}")
            response = requests.post(self.auth_url, json=payload, headers=headers)

            if response.status_code == 200:
                data = response.json()
                self.token = data.get("token")

                # 假设token有效期为1小时（根据实际情况调整）
                self.token_expiry = datetime.now() + timedelta(hours=1)

                self.logger.info("成功获取Vcloud token")
                return self.token
            else:
                self.logger.error(f"获取Vcloud token失败: HTTP {response.status_code} - {response.text}")
                return None
        except Exception as e:
            self.logger.exception(f"获取Vcloud token时发生异常: {str(e)}")
            return None

    def request(self, method, url, **kwargs):
        """执行Vcloud请求，如果token过期会自动刷新"""
        # 获取token
        token = self.get_token()
        if not token:
            return None

        # 添加认证头
        headers = kwargs.get('headers', {})
        headers['Authorization'] = token
        kwargs['headers'] = headers

        try:
            response = requests.request(method, url, **kwargs)

            # 检查是否token过期
            if response.status_code == 401:
                self.logger.warning("Token可能已过期，尝试刷新")
                token = self.get_token(force_refresh=True)
                if token:
                    headers['Authorization'] = token
                    response = requests.request(method, url, **kwargs)

            return response
        except Exception as e:
            self.logger.exception(f"Vcloud请求失败: {str(e)}")
            return None

    def register_patient(self, patient_data: Dict) -> bool:
        """在Vcloud系统中注册病人"""
        patient_id = patient_data.get("internalId", "Unknown")
        self.logger.info(f"尝试在Vcloud中注册病人: {patient_id}")

        subject_id = patient_data.get("internalId", "")
        url = f"{self.base_url}/internal/tenants/{self.tenant}/subjects/{subject_id}"

        payload = {
            "firstName": patient_data.get("firstName", ""),
            "lastName": patient_data.get("lastName", ""),
            "studyId": "test-jun-study1",
            "notificationTime": 4,
            "treatment": 14,
            "cycleStartNumber": 1,
            "cycleStartDays": 10
        }

        headers = {"Content-Type": "application/json"}

        response = self.request("POST", url, json=payload, headers=headers)
        if not response:
            return False

        success = response.status_code == 200
        if success:
            self.logger.info(f"成功在Vcloud中注册病人: {patient_id}")
        else:
            self.logger.error(f"在Vcloud中注册病人失败: HTTP {response.status_code} - {response.text}")

        return success

    def bind_device(self, sensor_id: str, subject_id: str,
                    record_time: int = None, timezone: int = 28800,
                    timezone_name: str = "Asia/Shanghai") -> bool:
        """在Vcloud系统中绑定设备到病人"""
        self.logger.info(f"尝试在Vcloud中将设备 {sensor_id} 绑定到病人 {subject_id}")

        url = f"{self.base_url}/tenants/{self.tenant}/events?type=sensorEvent"

        if record_time is None:
            record_time = int(datetime.now().timestamp() * 1000)

        payload = [{
            "sensorId": sensor_id,
            "manual": 1,
            "subtype": "Connect",
            "subjectId": subject_id,
            "recordTime": record_time,
            "timezone": timezone,
            "timezoneName": timezone_name
        }]

        headers = {"Content-Type": "application/json"}

        response = self.request("POST", url, json=payload, headers=headers)
        if not response:
            return False

        success = response.status_code == 200
        if success:
            self.logger.info(f"成功在Vcloud中绑定设备 {sensor_id} 到病人 {subject_id}")
        else:
            self.logger.error(f"在Vcloud中绑定设备失败: HTTP {response.status_code} - {response.text}")

        return success

    def quick_patient_assignment(self, device_data: Dict, patient_data: Dict) -> bool:
        """快速病人分配：先在WebPortal添加设备，然后通过Vcloud接口同步创建病人和设备"""
        self.logger.info(f"快速病人分配: 设备 {device_data.get('identifier')} -> 病人 {patient_data.get('internalId')}")

        # 1. 在WebPortal中添加设备
        # 注意：这里需要WebPortal客户端，但在VCloudClient中无法直接调用
        # 这个功能应该在主程序中协调两个客户端完成

        # 2. 在Vcloud中注册病人
        patient_success = self.register_patient(patient_data)

        # 3. 在Vcloud中绑定设备
        if patient_success:
            device_success = self.bind_device(device_data.get("identifier"), patient_data.get("internalId"))
            return device_success

        return False