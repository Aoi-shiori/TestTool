# 读取接口生成患者信息
python extract_patient.py api_response.json > patient_config.json

# 库
pip install -r requirements.txt
requests
pytz
aiohttp
pyjwt