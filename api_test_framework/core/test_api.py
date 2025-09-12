import pytest
import json
from ..core.api_client import APIClient
from ..utils.excel_reader import ExcelReader
from ..utils.response_matcher import ResponseMatcher
from ..core.assertions import assert_status_code


class TestAPI:
    @classmethod
    def setup_class(cls):
        cls.api_client = APIClient()
        cls.test_results = []

    @classmethod
    def teardown_class(cls):
        # 所有测试完成后写入结果
        excel_reader = ExcelReader()
        excel_reader.write_test_results(cls.test_results)

    def test_api_case(self, test_case):
        # 获取用例ID
        case_id = test_case.get('用例ID', '')
        print(53453,case_id)

        # 检查确认列，如果已确认通过则跳过
        if test_case.get('确认结果', '').lower() in ['通过', 'yes', 'true', '1']:
            result = {
                'case_id': case_id,
                'result': '跳过',
                'error_msg': '用例已确认通过，跳过执行'
            }
            self.test_results.append(result)
            pytest.skip(f"用例 {case_id} 已确认通过，跳过执行")

        # 检查启用状态，如果禁用则跳过
        if test_case.get('启用状态', '是').lower() in ['否', 'no', 'false', '0']:
            result = {
                'case_id': case_id,
                'result': '跳过',
                'error_msg': '用例被禁用，跳过执行'
            }
            self.test_results.append(result)
            pytest.skip(f"用例 {case_id} 被禁用，跳过执行")

        # 准备请求参数
        method = test_case.get('请求方法', 'GET')
        endpoint = test_case.get('接口路径', '')
        params = self._parse_json(test_case.get('请求参数', ''))
        data = self._parse_json(test_case.get('请求体', ''))
        headers = self._parse_json(test_case.get('请求头', ''))
        expected_status = test_case.get('预期状态码', 200)
        expected_response = test_case.get('预期响应', '')
        match_type = test_case.get('匹配方式', 'exact')  # 默认为精确匹配
        max_response_time = test_case.get('最大响应时间', 5000)

        # 记录测试结果
        result = {
            'case_id': case_id,
            'result': '失败',  # 默认为失败，只有全部断言通过才改为成功
            'error_msg': ''
        }

        try:
            # 发送请求
            response = self.api_client.request(
                method=method,
                endpoint=endpoint,
                params=params,
                json=data if isinstance(data, dict) else None,
                data=data if isinstance(data, str) else None,
                headers=headers
            )

            # 断言状态码
            assert_status_code(response, expected_status)

            # 断言响应时间
            if max_response_time:
                from ..core.assertions import assert_response_time
                assert_response_time(response, max_response_time)

            # 如果有预期响应，进行响应内容断言
            if expected_response:
                # 根据匹配方式进行断言
                if match_type == 'exact':
                    # 精确匹配
                    actual_response = response.text
                    if not ResponseMatcher.exact_match(actual_response, expected_response):
                        raise AssertionError(f"响应内容不匹配\n预期: {expected_response}\n实际: {actual_response}")

                elif match_type == 'key_fields':
                    # 关键字段匹配
                    try:
                        actual_json = response.json()
                        expected_json = self._parse_json(expected_response)
                        if not ResponseMatcher.key_fields_match(actual_json, expected_json):
                            raise AssertionError(f"关键字段不匹配\n预期: {expected_json}\n实际: {actual_json}")
                    except (json.JSONDecodeError, ValueError):
                        raise AssertionError("响应不是有效的JSON格式，无法进行关键字段匹配")

                elif match_type == 'partial':
                    # 部分匹配
                    try:
                        actual_json = response.json()
                        expected_json = self._parse_json(expected_response)
                        if not ResponseMatcher.partial_match(actual_json, expected_json):
                            raise AssertionError(f"响应部分内容不匹配\n预期包含: {expected_json}\n实际: {actual_json}")
                    except (json.JSONDecodeError, ValueError):
                        # 如果不是JSON，尝试文本匹配
                        if expected_response not in response.text:
                            raise AssertionError(
                                f"响应文本中不包含预期内容\n预期包含: {expected_response}\n实际: {response.text}")

                else:
                    raise ValueError(f"不支持的匹配方式: {match_type}")

            # JSON Schema验证
            json_schema = test_case.get('JSON Schema', '')
            if json_schema:
                schema = self._parse_json(json_schema)
                from ..core.assertions import assert_json_schema
                assert_json_schema(response, schema)

            # 所有断言通过，标记为成功
            result['result'] = '通过'

        except Exception as e:
            result['error_msg'] = str(e)
            raise e
        finally:
            self.test_results.append(result)

    def _parse_json(self, json_str):
        """解析JSON字符串"""
        if not json_str or not isinstance(json_str, str):
            return {}
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return json_str