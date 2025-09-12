import pytest
import os
import sys
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
                    (parent / 'TesTool').exists() and (parent / 'api_test_framework').exists():
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


def pytest_addoption(parser):
    """添加自定义命令行选项"""
    parser.addoption(
        "--env",
        action="store",
        default="test",
        help="环境配置: test, dev, prod"
    )
    parser.addoption(
        "--excel",
        action="store",
        default="test_cases.xlsx",
        help="Excel测试用例文件名"
    )
    parser.addoption(
        "--sheet",
        action="store",
        default=0,
        help="Excel工作表名称或索引"
    )
    parser.addoption(
        "--skip-confirmed",
        action="store_true",
        default=False,
        help="跳过已确认通过的测试用例"
    )


@pytest.fixture(scope="session")
def env_config(request):
    """获取环境配置"""
    env = request.config.getoption("--env")
    return env


@pytest.fixture(scope="session")
def excel_file(request):
    """获取Excel文件路径"""
    excel_name = request.config.getoption("--excel")

    # 首先在子项目data目录中查找
    subproject_data = PROJECT_ROOT / "api_test_framework" / "data" / excel_name
    if subproject_data.exists():
        return subproject_data

    # 然后在项目根目录的data目录中查找
    root_data = PROJECT_ROOT / "data" / excel_name
    if root_data.exists():
        return root_data

    # 如果都找不到，抛出异常
    raise FileNotFoundError(f"Excel文件不存在: {excel_name}")


@pytest.fixture(scope="session")
def sheet_name(request):
    """获取工作表名称或索引"""
    sheet = request.config.getoption("--sheet")
    try:
        # 尝试转换为整数（索引）
        return int(sheet)
    except ValueError:
        # 如果不是数字，返回字符串（工作表名称）
        return sheet


@pytest.fixture(scope="session")
def skip_confirmed(request):
    """是否跳过已确认的测试用例"""
    return request.config.getoption("--skip-confirmed")


@pytest.fixture(scope="session", autouse=True)
def setup_config(env_config):
    """设置环境配置"""
    # 导入配置管理器
    from api_test_framework.config.config import config
    config.set_env(env_config)
    yield


@pytest.fixture(scope="class")
def api_client():
    """创建API客户端实例"""
    from api_test_framework.core.api_client import APIClient
    return APIClient()


@pytest.fixture(scope="class")
def excel_reader(excel_file, sheet_name):
    """创建Excel读取器实例"""
    from api_test_framework.utils.excel_reader import ExcelReader
    reader = ExcelReader(str(excel_file))
    return reader, sheet_name


@pytest.fixture(scope="class")
def test_cases(excel_reader, skip_confirmed):
    """获取测试用例"""
    reader, sheet_name = excel_reader
    cases = reader.read_test_cases(sheet_name)

    # 如果需要跳过已确认的用例，过滤掉已确认的
    if skip_confirmed:
        cases = [case for case in cases if case.get('确认结果', '').lower() not in ['通过', 'yes', 'true', '1']]

    return cases


# # 添加这个fixture来参数化测试用例
# def pytest_generate_tests(metafunc):
#     """为每个测试用例生成测试"""
#     if "test_case" in metafunc.fixturenames:
#         # 获取fixture
#         excel_reader_fixture = metafunc._arg2fixturedefs.get("excel_reader")
#         skip_confirmed_fixture = metafunc._arg2fixturedefs.get("skip_confirmed")
#
#         if excel_reader_fixture and skip_confirmed_fixture:
#             # 获取fixture的值
#             excel_reader, sheet_name = excel_reader_fixture.func(metafunc)
#             skip_confirmed = skip_confirmed_fixture.func(metafunc)
#
#             # 读取测试用例
#             cases = excel_reader.read_test_cases(sheet_name)
#
#             # 如果需要跳过已确认的用例，过滤掉已确认的
#             if skip_confirmed:
#                 cases = [case for case in cases if case.get('确认结果', '').lower() not in ['通过', 'yes', 'true', '1']]
#
#             # 参数化测试
#             metafunc.parametrize("test_case", cases)


# 在 conftest.py 的 pytest_generate_tests 函数中添加日志输出
def pytest_generate_tests(metafunc):
    """为每个测试用例生成测试"""
    if "test_case" in metafunc.fixturenames:
        # 获取fixture
        excel_reader_fixture = metafunc._arg2fixturedefs.get("excel_reader")
        skip_confirmed_fixture = metafunc._arg2fixturedefs.get("skip_confirmed")

        if excel_reader_fixture and skip_confirmed_fixture:
            # 获取fixture的值
            excel_reader, sheet_name = excel_reader_fixture.func(metafunc)
            skip_confirmed = skip_confirmed_fixture.func(metafunc)

            # 读取测试用例
            cases = excel_reader.read_test_cases(sheet_name)

            print(f"找到 {len(cases)} 个测试用例")  # 添加调试输出

            # 如果需要跳过已确认的用例，过滤掉已确认的
            if skip_confirmed:
                cases = [case for case in cases if case.get('确认结果', '').lower() not in ['通过', 'yes', 'true', '1']]
                print(f"过滤后剩下 {len(cases)} 个测试用例")  # 添加调试输出

            # 参数化测试
            metafunc.parametrize("test_case", cases)
if __name__ == "__main__":
    pytest_generate_tests()