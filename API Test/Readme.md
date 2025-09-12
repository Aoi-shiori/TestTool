

# 从Swagger生成测试用例:
python run_tests.py --generate swagger_20250909.yaml --generate-format excel
# 运行测试:
python run_tests.py --env dev --test-cases test_cases/generated/swagger_cases.xlsx --output html


```yaml
api-test-framework/
├── config/                 # 环境配置文件
│   ├── dev.json
│   ├── staging.json
│   └── prod.json
├── test_cases/            # 测试用例目录
│   ├── generated/         # 自动生成的测试用例
│   ├── manual/            # 手动编写的测试用例
│   └── templates/         # 用例模板
├── test_data/             # 测试数据
│   ├── input/             # 输入数据
│   └── expected/          # 预期结果
├── utils/                 # 工具类
│   ├── __init__.py
│   ├── api_client.py
│   ├── assert_utils.py
│   ├── config_loader.py
│   ├── data_loader.py
│   ├── excel_utils.py
│   ├── json_utils.py
│   ├── report_generator.py
│   └── swagger_parser.py
├── tests/                 # 测试脚本
│   ├── __init__.py
│   ├── test_authentication.py
│   ├── test_bp_data.py
│   └── ...
├── results/               # 测试结果
│   ├── html_reports/
│   ├── excel_reports/
│   └── json_reports/
├── requirements.txt
├── run_tests.py          # 主运行脚本
└── README.md
```