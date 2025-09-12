import json
import re
from typing import Any, Dict, List


class ResponseMatcher:
    @staticmethod
    def exact_match(actual, expected):
        """完全匹配"""
        return actual == expected

    @staticmethod
    def key_fields_match(actual_response, expected_fields):
        """关键字段匹配"""
        if not isinstance(actual_response, dict) or not isinstance(expected_fields, dict):
            return False

        for key, expected_value in expected_fields.items():
            if key not in actual_response:
                return False

            actual_value = actual_response[key]
            if actual_value != expected_value:
                return False

        return True

    @staticmethod
    def partial_match(actual_response, expected_partial):
        """部分匹配（支持嵌套字段）"""
        if not isinstance(actual_response, (dict, list)) or not isinstance(expected_partial, (dict, list)):
            return str(actual_response) == str(expected_partial)

        if isinstance(actual_response, list) and isinstance(expected_partial, list):
            if len(actual_response) != len(expected_partial):
                return False

            for i, item in enumerate(expected_partial):
                if not ResponseMatcher.partial_match(actual_response[i], item):
                    return False
            return True

        if isinstance(actual_response, dict) and isinstance(expected_partial, dict):
            for key, expected_value in expected_partial.items():
                if key not in actual_response:
                    return False

                if not ResponseMatcher.partial_match(actual_response[key], expected_value):
                    return False
            return True

        return False

    @staticmethod
    def match_response(actual_response, expected_response, match_type="exact"):
        """
        根据匹配类型验证响应

        Args:
            actual_response: 实际响应
            expected_response: 预期响应
            match_type: 匹配类型 (exact, key_fields, partial)

        Returns:
            bool: 是否匹配
        """
        try:
            # 尝试解析JSON响应
            if isinstance(actual_response, str):
                actual_response = json.loads(actual_response)
            if isinstance(expected_response, str):
                expected_response = json.loads(expected_response)
        except (json.JSONDecodeError, TypeError):
            pass  # 如果不是JSON，保持原样

        if match_type == "exact":
            return ResponseMatcher.exact_match(actual_response, expected_response)
        elif match_type == "key_fields":
            return ResponseMatcher.key_fields_match(actual_response, expected_response)
        elif match_type == "partial":
            return ResponseMatcher.partial_match(actual_response, expected_response)
        else:
            raise ValueError(f"不支持的匹配类型: {match_type}")