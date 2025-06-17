import os
import requests
import pandas as pd
import time
from dotenv import load_dotenv
from feast import FeatureStore
from datetime import datetime
from pytz import timezone

# Load API key
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("API key is missing. Please set OPENWEATHER_API_KEY in your .env file.")

BASE_URL = "http://api.openweathermap.org/data/2.5/air_pollution"

# City map
CITY_COORD_MAP = {
    "hà nội": (21.0, 105.75),
    "hải phòng": (20.75, 106.75),
    "thái bình": (20.5, 106.25),
}

# Khởi tạo Feast store
store = FeatureStore(repo_path="/Users/buihung/VT/project1/feast")

# Tính AQI từ PM2.5
def calculate_aqi_pm25(concentration):
    breakpoints = [
        (0.0, 12.0, 0, 50),
        (12.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200),
        (150.5, 250.4, 201, 300),
        (250.5, 350.4, 301, 400),
        (350.5, 500.4, 401, 500)
    ]
    for bp_lo, bp_hi, i_lo, i_hi in breakpoints:
        if bp_lo <= concentration <= bp_hi:
            return ((i_hi - i_lo) / (bp_hi - bp_lo)) * (concentration - bp_lo) + i_lo
    return 500  # Nếu vượt ngưỡng, gán max

def extract_and_load_to_redis():
    all_records = []

    for city_name, (lat, lon) in CITY_COORD_MAP.items():
        print(f"Fetching data for {city_name.title()} (lat={lat}, lon={lon})...")
        params = {
            "lat": lat,
            "lon": lon,
            "appid": API_KEY
        }
        response = requests.get(BASE_URL, params=params)

        if response.status_code == 200:
            data = response.json()
            if "list" in data and data["list"]:
                entry = data["list"][0]
                pm25 = entry["components"]["pm2_5"]
                aqi = calculate_aqi_pm25(pm25)

                # Lấy thời gian hiện tại
                feature_time = datetime.now(timezone("Asia/Ho_Chi_Minh"))
                hour = feature_time.hour
                day = feature_time.day
                day_of_week = feature_time.isoweekday()

                entity_id = f"{lat}_{lon}"

                record = {
                    "entity_id": entity_id,
                    "hour": hour,
                    "day": day,
                    "dayOfWeek": day_of_week,
                    "aqi": aqi,
                    "feature_timestamp": feature_time
                }
                all_records.append(record)
            else:
                print(f"No valid data for {city_name.title()}")
        else:
            print(f"Failed to fetch data for {city_name.title()}: {response.status_code}")
            print(f"Response: {response.text}")

        time.sleep(1)

    if all_records:
        df = pd.DataFrame(all_records)
        print(df)

        # Đẩy vào Feast Redis
        store.write_to_online_store(feature_view_name="aqi_info_v1", df=df)
        print("✅ Đã cập nhật Redis thành công.")

