"""
API测试断言库
提供丰富的断言功能，支持状态码、响应内容、响应时间等的验证
"""

import json
import re
from typing import Any, Dict, List, Union, Optional
from requests.models import Response

from ..utils.response_matcher import ResponseMatcher
from ..utils.logger import logger

class APIAssertions:
    """API测试断言类"""

    @staticmethod
    def assert_status_code(response: Response, expected_code: Union[int, List[int]]):
        """
        断言响应状态码

        Args:
            response: 响应对象
            expected_code: 预期的状态码或状态码列表

        Raises:
            AssertionError: 如果状态码不匹配
        """
        if isinstance(expected_code, list):
            if response.status_code not in expected_code:
                raise AssertionError(
                    f"状态码不匹配! 预期: {expected_code}, 实际: {response.status_code}\n"
                    f"响应内容: {response.text}"
                )
        else:
            if response.status_code != expected_code:
                raise AssertionError(
                    f"状态码不匹配! 预期: {expected_code}, 实际: {response.status_code}\n"
                    f"响应内容: {response.text}"
                )

        logger.info(f"状态码断言通过: {response.status_code}")

    @staticmethod
    def assert_response_time(response: Response, max_time_ms: int):
        """
        断言响应时间不超过指定值

        Args:
            response: 响应对象
            max_time_ms: 最大允许的响应时间(毫秒)

        Raises:
            AssertionError: 如果响应时间超过限制
        """
        # 注意: response.elapsed 返回的是 timedelta 对象
        response_time_ms = response.elapsed.total_seconds() * 1000

        if response_time_ms > max_time_ms:
            raise AssertionError(
                f"响应时间超过限制! 最大允许: {max_time_ms}ms, 实际: {response_time_ms:.2f}ms"
            )

        logger.info(f"响应时间断言通过: {response_time_ms:.2f}ms <= {max_time_ms}ms")

    @staticmethod
    def assert_response_contains(response: Response, expected_content: Any, match_type: str = "partial"):
        """
        断言响应包含预期内容

        Args:
            response: 响应对象
            expected_content: 预期内容
            match_type: 匹配类型 (exact, key_fields, partial)

        Raises:
            AssertionError: 如果响应不包含预期内容
        """
        try:
            # 尝试解析JSON响应
            actual_response = response.json()
        except (json.JSONDecodeError, ValueError):
            # 如果不是JSON，使用文本
            actual_response = response.text

        if not ResponseMatcher.match_response(actual_response, expected_content, match_type):
            raise AssertionError(
                f"响应内容不匹配! 匹配类型: {match_type}\n"
                f"预期: {expected_content}\n"
                f"实际: {actual_response}"
            )

        logger.info(f"响应内容断言通过: {match_type}匹配")

    @staticmethod
    def assert_json_schema(response: Response, schema: Dict):
        """
        断言响应JSON符合指定的JSON Schema

        Args:
            response: 响应对象
            schema: JSON Schema

        Raises:
            AssertionError: 如果响应不符合JSON Schema
        """
        try:
            import jsonschema
        except ImportError:
            raise ImportError("jsonschema库未安装，请运行: pip install jsonschema")

        try:
            response_json = response.json()
            jsonschema.validate(instance=response_json, schema=schema)
        except jsonschema.ValidationError as e:
            raise AssertionError(f"JSON Schema验证失败: {e.message}\n路径: {list(e.path)}")
        except json.JSONDecodeError:
            raise AssertionError("响应不是有效的JSON格式，无法进行Schema验证")

        logger.info("JSON Schema断言通过")

    @staticmethod
    def assert_header_exists(response: Response, header_name: str):
        """
        断言响应头包含指定字段

        Args:
            response: 响应对象
            header_name: 头部字段名称

        Raises:
            AssertionError: 如果响应头不包含指定字段
        """
        if header_name not in response.headers:
            raise AssertionError(f"响应头不包含字段: {header_name}")

        logger.info(f"响应头断言通过: 包含字段 {header_name}")

    @staticmethod
    def assert_header_value(response: Response, header_name: str, expected_value: str):
        """
        断言响应头字段的值

        Args:
            response: 响应对象
            header_name: 头部字段名称
            expected_value: 预期的字段值

        Raises:
            AssertionError: 如果响应头字段值不匹配
        """
        if header_name not in response.headers:
            raise AssertionError(f"响应头不包含字段: {header_name}")

        actual_value = response.headers[header_name]
        if actual_value != expected_value:
            raise AssertionError(
                f"响应头字段值不匹配! 字段: {header_name}\n"
                f"预期: {expected_value}\n"
                f"实际: {actual_value}"
            )

        logger.info(f"响应头值断言通过: {header_name} = {actual_value}")

    @staticmethod
    def assert_cookie_exists(response: Response, cookie_name: str):
        """
        断言响应包含指定Cookie

        Args:
            response: 响应对象
            cookie_name: Cookie名称

        Raises:
            AssertionError: 如果响应不包含指定Cookie
        """
        if cookie_name not in response.cookies:
            raise AssertionError(f"响应不包含Cookie: {cookie_name}")

        logger.info(f"Cookie断言通过: 包含Cookie {cookie_name}")

    @staticmethod
    def assert_cookie_value(response: Response, cookie_name: str, expected_value: str):
        """
        断言Cookie的值

        Args:
            response: 响应对象
            cookie_name: Cookie名称
            expected_value: 预期的Cookie值

        Raises:
            AssertionError: 如果Cookie值不匹配
        """
        if cookie_name not in response.cookies:
            raise AssertionError(f"响应不包含Cookie: {cookie_name}")

        actual_value = response.cookies[cookie_name]
        if actual_value != expected_value:
            raise AssertionError(
                f"Cookie值不匹配! Cookie: {cookie_name}\n"
                f"预期: {expected_value}\n"
                f"实际: {actual_value}"
            )

        logger.info(f"Cookie值断言通过: {cookie_name} = {actual_value}")

    @staticmethod
    def assert_response_size(response: Response, min_size: Optional[int] = None, max_size: Optional[int] = None):
        """
        断言响应大小在指定范围内

        Args:
            response: 响应对象
            min_size: 最小响应大小(字节)
            max_size: 最大响应大小(字节)

        Raises:
            AssertionError: 如果响应大小不在指定范围内
        """
        response_size = len(response.content)

        if min_size is not None and response_size < min_size:
            raise AssertionError(f"响应大小太小! 最小: {min_size}字节, 实际: {response_size}字节")

        if max_size is not None and response_size > max_size:
            raise AssertionError(f"响应大小太大! 最大: {max_size}字节, 实际: {response_size}字节")

        logger.info(f"响应大小断言通过: {response_size}字节")

    @staticmethod
    def assert_json_path(response: Response, json_path: str, expected_value: Any):
        """
        断言JSON路径的值

        Args:
            response: 响应对象
            json_path: JSON路径表达式
            expected_value: 预期的值

        Raises:
            AssertionError: 如果JSON路径的值不匹配
        """
        try:
            import jsonpath_ng
        except ImportError:
            raise ImportError("jsonpath-ng库未安装，请运行: pip install jsonpath-ng")

        try:
            response_json = response.json()

            # 解析JSON路径
            expr = jsonpath_ng.parse(json_path)
            matches = [match.value for match in expr.find(response_json)]

            if not matches:
                raise AssertionError(f"JSON路径未找到匹配项: {json_path}")

            # 检查是否至少有一个匹配项等于预期值
            if expected_value not in matches:
                raise AssertionError(
                    f"JSON路径值不匹配! 路径: {json_path}\n"
                    f"预期: {expected_value}\n"
                    f"实际匹配值: {matches}"
                )

        except json.JSONDecodeError:
            raise AssertionError("响应不是有效的JSON格式，无法进行JSON路径查询")
        except jsonpath_ng.jsonpath.JsonPathError as e:
            raise AssertionError(f"JSON路径解析错误: {e}")

        logger.info(f"JSON路径断言通过: {json_path} = {expected_value}")

    @staticmethod
    def assert_regex_match(response: Response, pattern: str, flags: int = 0):
        """
        断言响应文本匹配正则表达式

        Args:
            response: 响应对象
            pattern: 正则表达式模式
            flags: 正则表达式标志

        Raises:
            AssertionError: 如果响应文本不匹配正则表达式
        """
        if not re.search(pattern, response.text, flags):
            raise AssertionError(f"响应文本不匹配正则表达式: {pattern}")

        logger.info(f"正则表达式断言通过: 匹配模式 {pattern}")

# 创建全局实例
assertions = APIAssertions()

# 导出常用断言函数
assert_status_code = assertions.assert_status_code
assert_response_time = assertions.assert_response_time
assert_response_contains = assertions.assert_response_contains
assert_json_schema = assertions.assert_json_schema
assert_header_exists = assertions.assert_header_exists
assert_header_value = assertions.assert_header_value
assert_cookie_exists = assertions.assert_cookie_exists
assert_cookie_value = assertions.assert_cookie_value
assert_response_size = assertions.assert_response_size
assert_json_path = assertions.assert_json_path
assert_regex_match = assertions.assert_regex_match