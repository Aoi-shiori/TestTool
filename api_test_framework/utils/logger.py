import logging
import sys
from pathlib import Path

# 创建logger
logger = logging.getLogger('api_test_framework')
logger.setLevel(logging.DEBUG)

# 创建控制台处理器
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# 创建文件处理器
log_file = Path(__file__).parent.parent / 'logs' / 'api_test.log'
log_file.parent.mkdir(exist_ok=True)
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)

# 创建格式化器
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# 添加处理器到logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)