import pandas as pd
import json
import os


class DataLoader:
    @staticmethod
    def load_excel(file_path, sheet_name=0):
        """从Excel加载数据"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Excel file not found: {file_path}")

        return pd.read_excel(file_path, sheet_name=sheet_name)

    @staticmethod
    def load_json(file_path):
        """从JSON加载数据"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"JSON file not found: {file_path}")

        with open(file_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def save_excel(data, file_path, sheet_name="Results"):
        """保存数据到Excel"""
        if isinstance(data, pd.DataFrame):
            data.to_excel(file_path, sheet_name=sheet_name, index=False)
        else:
            df = pd.DataFrame(data)
            df.to_excel(file_path, sheet_name=sheet_name, index=False)

    @staticmethod
    def save_json(data, file_path):
        """保存数据到JSON"""
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)

    @staticmethod
    def excel_to_dict(file_path, sheet_name=0):
        """Excel转换为字典列表"""
        df = DataLoader.load_excel(file_path, sheet_name)
        return df.to_dict('records')