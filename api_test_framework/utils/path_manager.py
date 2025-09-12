import os
import sys
from pathlib import Path


def get_project_root():
    """获取项目根目录"""
    # 尝试通过环境变量获取项目根目录
    root_env = os.environ.get('PROJECT_ROOT')
    if root_env:
        return Path(root_env)

    # 尝试通过当前文件路径推断
    current_file = Path(__file__).resolve()

    # 向上查找直到找到项目根目录标记（如requirements.txt）
    for parent in current_file.parents:
        if (parent / 'requirements.txt').exists():
            return parent

        # 或者检查是否有已知的目录结构
        if (parent / 'api_test_framework').exists() and (parent / 'TestToos').exists():
            return parent

    # 如果找不到，使用当前工作目录
    return Path.cwd()


def get_module_path(module_name):
    """获取指定模块的路径"""
    project_root = get_project_root()

    # 检查是否是子项目
    subproject_path = project_root / 'api_test_framework'
    if subproject_path.exists():
        return subproject_path / module_name

    # 如果不是子项目结构，直接返回模块路径
    return project_root / module_name


# 常用路径
PROJECT_ROOT = get_project_root()
CONFIG_PATH = get_module_path('config')
DATA_PATH = get_module_path('data')
TESTS_PATH = get_module_path('tests')