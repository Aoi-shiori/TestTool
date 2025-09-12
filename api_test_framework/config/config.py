import os
import yaml
from pathlib import Path
from ..utils.path_manager import CONFIG_PATH


class Config:
    def __init__(self, env="test"):
        self.env = env
        self.config_dir = CONFIG_PATH
        self.load_config()

    def load_config(self):
        config_file = self.config_dir / f"{self.env}.yaml"
        if not config_file.exists():
            # 尝试在项目根目录查找
            from ..utils.path_manager import PROJECT_ROOT
            root_config = PROJECT_ROOT / "config" / f"{self.env}.yaml"
            if root_config.exists():
                config_file = root_config
            else:
                raise FileNotFoundError(f"配置文件不存在: {config_file}")

        with open(config_file, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

    def get(self, key, default=None):
        return self.config.get(key, default)

    def set_env(self, env):
        self.env = env
        self.load_config()


# 全局配置实例
config = Config()