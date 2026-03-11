import requests, os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('COINGLASS_API_KEY')
print("Key length:", len(api_key) if api_key else 0)

url = "https://open-api-v4.coinglass.com/api/index/bitcoin-net-unrealized-profit-loss"
headers = {"accept": "application/json", "CG-API-KEY": api_key}
try:
    response = requests.get(url, headers=headers)
    print("Status code:", response.status_code)
    import json
    # we only print a bit of it or keys
    data = response.json()
    print("Keys in data:", data.keys())
    if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
        print("First item keys:", data["data"][0].keys() if isinstance(data["data"][0], dict) else type(data["data"][0]))
    else:
        print("Data field:", str(data)[:1000])
except Exception as e:
    print(e)
