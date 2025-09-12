import json
import os


class ConfigLoader:
    def __init__(self, env="dev"):
        self.env = env
        self.config = self.load_config(env)

    def load_config(self, env):
        config_path = f"config/{env}.json"
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, 'r') as f:
            return json.load(f)

    def get_base_url(self):
        return self.config.get("base_url")

    def get_auth_token(self):
        return self.config.get("auth_token")

    def get_db_config(self):
        return self.config.get("database", {})

    def get_other_config(self, key):
        return self.config.get(key)