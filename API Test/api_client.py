import requests
import json
from urllib.parse import urljoin


class APIClient:
    def __init__(self, base_url, auth_token=None):
        self.base_url = base_url
        self.session = requests.Session()

        headers = {
            "Content-Type": "application/json"
        }

        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        self.session.headers.update(headers)

    def request(self, method, endpoint, **kwargs):
        url = urljoin(self.base_url, endpoint)

        # 处理JSON数据
        if "json" in kwargs:
            kwargs["data"] = json.dumps(kwargs.pop("json"))

        response = self.session.request(method, url, **kwargs)

        # 尝试解析JSON响应
        try:
            response.json_data = response.json()
        except:
            response.json_data = {}

        return response

    def get(self, endpoint, **kwargs):
        return self.request("GET", endpoint, **kwargs)

    def post(self, endpoint, **kwargs):
        return self.request("POST", endpoint, **kwargs)

    def put(self, endpoint, **kwargs):
        return self.request("PUT", endpoint, **kwargs)

    def patch(self, endpoint, **kwargs):
        return self.request("PATCH", endpoint, **kwargs)

    def delete(self, endpoint, **kwargs):
        return self.request("DELETE", endpoint, **kwargs)