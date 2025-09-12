import pandas as pd
import json
from datetime import datetime
from data_loader import DataLoader
import os


class ReportGenerator:
    def __init__(self):
        self.results = []

    def add_result(self, test_name, status, response_time, response=None, error=None):
        """添加测试结果"""
        result = {
            "test_name": test_name,
            "status": status,
            "response_time": response_time,
            "timestamp": datetime.now().isoformat(),
            "error": str(error) if error else None
        }

        if response:
            result["status_code"] = response.status_code
            if hasattr(response, 'json_data'):
                result["response_data"] = response.json_data
            else:
                try:
                    result["response_data"] = response.json()
                except:
                    result["response_data"] = response.text

        self.results.append(result)
        return result

    def generate_excel_report(self, file_path):
        """生成Excel报告"""
        df = pd.DataFrame(self.results)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        DataLoader.save_excel(df, file_path)
        print(f"Excel report generated: {file_path}")

    def generate_json_report(self, file_path):
        """生成JSON报告"""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        DataLoader.save_json(self.results, file_path)
        print(f"JSON report generated: {file_path}")

    def generate_html_report(self, file_path):
        """生成HTML报告"""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        html_content = """
        <html>
        <head>
            <title>API Test Report</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                table { border-collapse: collapse; width: 100%; margin-top: 20px; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
                .pass { background-color: #d4edda; }
                .fail { background-color: #f8d7da; }
                .summary { margin-bottom: 20px; }
            </style>
        </head>
        <body>
            <h1>API Test Report</h1>
            <div class="summary">
                <p><strong>Generated at:</strong> {timestamp}</p>
                <p><strong>Total tests:</strong> {total_tests}</p>
                <p><strong>Passed:</strong> {passed} | <strong>Failed:</strong> {failed}</p>
                <p><strong>Success rate:</strong> {success_rate}%</p>
            </div>
            <table>
                <tr>
                    <th>Test Name</th>
                    <th>Status</th>
                    <th>Response Time (s)</th>
                    <th>Status Code</th>
                    <th>Error</th>
                </tr>
                {rows}
            </table>
        </body>
        </html>
        """

        passed = sum(1 for r in self.results if r["status"] == "PASS")
        total = len(self.results)
        failed = total - passed
        success_rate = round((passed / total) * 100, 2) if total > 0 else 0

        rows = ""
        for result in self.results:
            row_class = "pass" if result["status"] == "PASS" else "fail"
            rows += f"""
            <tr class="{row_class}">
                <td>{result['test_name']}</td>
                <td>{result['status']}</td>
                <td>{result['response_time']}</td>
                <td>{result.get('status_code', 'N/A')}</td>
                <td>{result.get('error', '')}</td>
            </tr>
            """

        html_content = html_content.format(
            timestamp=datetime.now().isoformat(),
            total_tests=total,
            passed=passed,
            failed=failed,
            success_rate=success_rate,
            rows=rows
        )

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"HTML report generated: {file_path}")