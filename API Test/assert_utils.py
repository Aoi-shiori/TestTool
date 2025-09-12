import jsonschema
from deepdiff import DeepDiff


class AssertUtils:
    @staticmethod
    def assert_status_code(response, expected_code):
        """断言状态码"""
        assert response.status_code == expected_code, \
            f"Expected status code {expected_code}, but got {response.status_code}"

    @staticmethod
    def assert_json_schema(response, schema):
        """断言JSON结构"""
        try:
            jsonschema.validate(instance=response.json(), schema=schema)
        except jsonschema.ValidationError as e:
            raise AssertionError(f"JSON schema validation failed: {e}")

    @staticmethod
    def assert_response_contains(response, key, value=None):
        """断言响应包含特定键值"""
        data = response.json()
        assert key in data, f"Key '{key}' not found in response"

        if value is not None:
            assert data[key] == value, \
                f"Expected value '{value}' for key '{key}', but got '{data[key]}'"

    @staticmethod
    def assert_response_equals(response, expected_data, ignore_order=False):
        """断言响应等于预期数据"""
        actual_data = response.json() if hasattr(response, 'json') else response
        diff = DeepDiff(actual_data, expected_data, ignore_order=ignore_order)
        assert not diff, f"Response does not match expected data: {diff}"

    @staticmethod
    def assert_response_time(response, max_time):
        """断言响应时间"""
        assert response.elapsed.total_seconds() <= max_time, \
            f"Response time {response.elapsed.total_seconds()}s exceeds maximum {max_time}s"