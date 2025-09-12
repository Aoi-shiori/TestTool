import argparse
import sys
import os
from datetime import datetime

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config_loader import ConfigLoader
from api_client import APIClient
from assert_utils import AssertUtils
from data_loader import DataLoader
from swagger_parser import SwaggerParser
from report_generator import ReportGenerator


def run_tests(env, test_case_file, output_format):
    """运行测试用例"""
    print(f"Running tests for environment: {env}")

    # 加载配置
    config = ConfigLoader(env)
    base_url = config.get_base_url()
    auth_token = config.get_auth_token()

    print(f"Base URL: {base_url}")

    # 初始化API客户端
    api_client = APIClient(base_url, auth_token)

    # 初始化报告生成器
    report = ReportGenerator()

    # 加载测试用例
    if test_case_file.endswith('.xlsx'):
        test_cases = DataLoader.excel_to_dict(test_case_file)
    elif test_case_file.endswith('.json'):
        test_cases = DataLoader.load_json(test_case_file)
    else:
        raise ValueError("Unsupported test case format. Use .xlsx or .json")

    print(f"Loaded {len(test_cases)} test cases")

    # 执行测试用例
    for i, case in enumerate(test_cases, 1):
        print(f"Running test {i}/{len(test_cases)}: {case.get('name', 'Unnamed test')}")

        try:
            # 准备请求参数
            method = case.get("method", "GET")
            endpoint = case.get("endpoint")

            if not endpoint:
                raise ValueError("Endpoint is required in test case")

            # 发送请求
            response = api_client.request(
                method,
                endpoint,
                json=case.get("request_body", {}),
                params=case.get("parameters", {})
            )

            # 执行断言
            expected_status = case.get("expected_status", 200)
            AssertUtils.assert_status_code(response, expected_status)

            # 记录成功结果
            report.add_result(
                case.get("name", f"{method} {endpoint}"),
                "PASS",
                response.elapsed.total_seconds(),
                response
            )

            print(f"  ✓ PASSED (Status: {response.status_code}, Time: {response.elapsed.total_seconds():.3f}s)")

        except Exception as e:
            # 记录失败结果
            result = report.add_result(
                case.get("name", f"{method} {endpoint}"),
                "FAIL",
                response.elapsed.total_seconds() if 'response' in locals() else 0,
                response if 'response' in locals() else None,
                e
            )

            print(f"  ✗ FAILED: {str(e)}")

    # 生成报告
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if output_format == "excel":
        report_path = f"results/excel_reports/report_{timestamp}.xlsx"
        report.generate_excel_report(report_path)
    elif output_format == "json":
        report_path = f"results/json_reports/report_{timestamp}.json"
        report.generate_json_report(report_path)
    else:
        report_path = f"results/html_reports/report_{timestamp}.html"
        report.generate_html_report(report_path)

    print(f"\nTest execution completed. Report generated: {report_path}")


def generate_test_cases(swagger_path, output_format):
    """从Swagger生成测试用例"""
    print(f"Generating test cases from: {swagger_path}")

    parser = SwaggerParser(swagger_path)
    test_cases = parser.generate_test_cases(output_format)

    print(f"Generated {len(test_cases)} test cases")
    print(f"Test cases saved to: test_cases/generated/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="API Test Framework")
    parser.add_argument("--env", default="dev", help="Environment: dev, staging, prod")
    parser.add_argument("--test-cases", help="Path to test cases file")
    parser.add_argument("--output", default="html", help="Output format: html, excel, json")
    parser.add_argument("--generate", help="Generate test cases from Swagger file")
    parser.add_argument("--generate-format", default="excel", help="Format for generated test cases: excel, json")

    args = parser.parse_args()

    # 创建必要的目录
    os.makedirs("config", exist_ok=True)
    os.makedirs("test_cases/generated", exist_ok=True)
    os.makedirs("results/excel_reports", exist_ok=True)
    os.makedirs("results/json_reports", exist_ok=True)
    os.makedirs("results/html_reports", exist_ok=True)

    if args.generate:
        generate_test_cases(args.generate, args.generate_format)
    elif args.test_cases:
        run_tests(args.env, args.test_cases, args.output)
    else:
        print("Please specify either --test-cases to run tests or --generate to generate test cases from Swagger")
        print("\nExamples:")
        print("  python run_tests.py --generate swagger.yaml --generate-format excel")
        print("  python run_tests.py --env dev --test-cases test_cases/generated/swagger_cases.xlsx --output html")