import requests
import json
from ..config.config import config
from ..utils.logger import logger


class APIClient:
    def __init__(self):
        self.base_url = config.get('base_url')
        self.headers = config.get('headers', {})
        self.timeout = config.get('timeout', 30)

    def request(self, method, endpoint, **kwargs):
        url = f"{self.base_url}{endpoint}"

        # 合并headers
        headers = {**self.headers, **kwargs.get('headers', {})}
        kwargs['headers'] = headers

        # 设置超时
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout

        logger.info(f"请求: {method} {url}")
        logger.debug(f"请求参数: {kwargs}")

        try:
            response = requests.request(method, url, **kwargs)
            logger.info(f"响应状态码: {response.status_code}")
            logger.debug(f"响应内容: {response.text}")
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"请求异常: {e}")
            raise

    def get(self, endpoint, **kwargs):
        return self.request('GET', endpoint, **kwargs)

    def post(self, endpoint, **kwargs):
        return self.request('POST', endpoint, **kwargs)

    def put(self, endpoint, **kwargs):
        return self.request('PUT', endpoint, **kwargs)

    def delete(self, endpoint, **kwargs):
        return self.request('DELETE', endpoint, **kwargs)