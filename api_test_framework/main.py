#!/usr/bin/env python3
"""
API接口自动化测试框架主入口
支持命令行参数配置测试运行
"""

import argparse
import sys
import os
from pathlib import Path


# 添加项目根目录到Python路径
def add_project_root_to_path():
    """将项目根目录添加到Python路径中"""
    # 尝试通过环境变量获取项目根目录
    project_root = os.environ.get('PROJECT_ROOT')

    if project_root:
        project_root = Path(project_root)
    else:
        # 自动检测项目根目录
        current_dir = Path(__file__).resolve().parent
        for parent in current_dir.parents:
            # 检查是否有requirements.txt或已知的项目结构
            if (parent / 'requirements.txt').exists() or \
                    (parent / 'TestTool').exists() and (parent / 'api_test_framework').exists():
                project_root = parent
                break
        else:
            # 如果没有找到，使用当前目录的父目录
            project_root = current_dir.parent

    # 将项目根目录添加到Python路径
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    return project_root


# 获取项目根目录
PROJECT_ROOT = add_project_root_to_path()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="API接口自动化测试框架")


    # 添加命令行参数
    parser.add_argument(
        "--env",
        default="test",
        choices=["test", "dev", "prod"],
        help="测试环境配置 (默认: test)"
    )
    parser.add_argument(
        "--excel",
        default="test_cases.xlsx",
        help="Excel测试用例文件名 (默认: test_cases.xlsx)"
    )
    parser.add_argument(
        "--sheet",
        default=0,
        help="Excel工作表名称或索引 (默认: 0)"
    )
    parser.add_argument(
        "--skip-confirmed",
        action="store_true",
        help="跳过已确认通过的测试用例"
    )
    parser.add_argument(
        "--html-report",
        help="生成HTML测试报告到指定路径"
    )
    parser.add_argument(
        "--junit-xml",
        help="生成JUnit XML测试报告到指定路径"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="详细输出模式"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="简洁输出模式"
    )
    parser.add_argument(
        "--list-cases",
        action="store_true",
        help="列出所有测试用例但不执行"
    )

    args = parser.parse_args()

    # 构建pytest命令行参数
    pytest_args = [
        "-x",  # 遇到第一个失败就停止
        f"--env={args.env}",
        f"--excel={args.excel}",
        f"--sheet={args.sheet}",
    ]

    if args.skip_confirmed:
        pytest_args.append("--skip-confirmed")

    if args.html_report:
        pytest_args.append(f"--html={args.html_report}")

    if args.junit_xml:
        pytest_args.append(f"--junitxml={args.junit_xml}")

    if args.verbose:
        pytest_args.append("-v")

    if args.quiet:
        pytest_args.append("-q")

    if args.list_cases:
        pytest_args.append("--collect-only")

    # 添加测试目录
    test_dir = PROJECT_ROOT / "api_test_framework" / "tests"
    if test_dir.exists():
        pytest_args.append(str(test_dir))
    else:
        # 如果在项目根目录下找不到，尝试在当前目录下查找
        current_test_dir = Path(__file__).parent / "tests"
        if current_test_dir.exists():
            pytest_args.append(str(current_test_dir))
        else:
            print("错误: 找不到测试目录")
            return 1

    # 导入pytest并运行
    try:
        import pytest
        exit_code = pytest.main(pytest_args)
        return exit_code
    except ImportError:
        print("错误: 未安装pytest，请先安装依赖")
        print("运行: pip install -r requirements.txt")
        return 1


if __name__ == "__main__":
    main()
    # sys.exit(main())