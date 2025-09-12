import yaml
import json
import os
from data_loader import DataLoader


class SwaggerParser:
    def __init__(self, swagger_path):
        self.swagger_path = swagger_path
        self.spec = self.load_swagger()

    def load_swagger(self):
        """加载Swagger文件"""
        if not os.path.exists(self.swagger_path):
            raise FileNotFoundError(f"Swagger file not found: {self.swagger_path}")

        if self.swagger_path.endswith('.yaml') or self.swagger_path.endswith('.yml'):
            with open(self.swagger_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        else:
            with open(self.swagger_path, 'r', encoding='utf-8') as f:
                return json.load(f)

    def generate_test_cases(self, output_format="excel"):
        """生成测试用例"""
        test_cases = []

        for path, methods in self.spec.get("paths", {}).items():
            for method, details in methods.items():
                # 生成基础测试用例
                base_case = {
                    "name": f"{method.upper()} {path}",
                    "method": method.upper(),
                    "endpoint": path,
                    "description": details.get("description", ""),
                    "parameters": self._parse_parameters(details.get("parameters", [])),
                    "request_body": self._parse_request_body(details),
                    "responses": self._parse_responses(details.get("responses", {})),
                    "tags": details.get("tags", [])
                }

                test_cases.append(base_case)

                # 为每个响应状态码生成一个测试用例
                for status_code, response_info in details.get("responses", {}).items():
                    if status_code.isdigit() and int(status_code) < 400:  # 只生成成功用例
                        case = base_case.copy()
                        case["name"] = f"{method.upper()} {path} - {status_code}"
                        case["expected_status"] = int(status_code)
                        case["expected_response"] = response_info
                        test_cases.append(case)

        # 保存测试用例
        os.makedirs("test_cases/generated", exist_ok=True)
        if output_format == "excel":
            DataLoader.save_excel(test_cases, "test_cases/generated/swagger_cases.xlsx")
        else:
            DataLoader.save_json(test_cases, "test_cases/generated/swagger_cases.json")

        return test_cases

    def _parse_parameters(self, parameters):
        """解析参数"""
        result = {}
        for param in parameters:
            param_name = param.get("name")
            param_in = param.get("in")
            result[f"{param_in}_{param_name}"] = {
                "type": param.get("type", "string"),
                "required": param.get("required", False),
                "description": param.get("description", ""),
                "example": param.get("example")
            }
        return result

    def _parse_request_body(self, details):
        """解析请求体"""
        if "parameters" in details:
            body_params = [p for p in details.get("parameters", []) if p.get("in") == "body"]
            if body_params:
                return body_params[0].get("schema", {})
        return {}

    def _parse_responses(self, responses):
        """解析响应"""
        result = {}
        for status_code, response_info in responses.items():
            result[status_code] = {
                "description": response_info.get("description", ""),
                "schema": response_info.get("schema", {})
            }
        return result
if __name__ == "__main__":
    parser = SwaggerParser("swagger_20250909.yaml")
    test_cases = parser.generate_test_cases("excel")
