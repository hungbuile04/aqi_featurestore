import os
import datetime
import asyncio
import threading
import smtplib
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo
from feast import FeatureStore
from pytz import timezone
import pandas as pd
import joblib

import google.auth
from google.adk.agents import Agent
import uvicorn


# --- CẤU HÌNH ---
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", "iron-envelope-455716-g8")
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "us-east1")
os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "True")

# Gmail gửi email cảnh báo
EMAIL_SENDER = "builehunghuy2004@gmail.com"
EMAIL_PASSWORD = "dfrwngpmhldqmwyd"  # Dùng App Password từ Gmail
EMAIL_RECIPIENTS = ["overleaf.editor1@gmail.com"]

# Map entity_id cho từng thành phố
CITY_COORD_MAP = {
    "hà nội": "21.0_105.75",
    "hải phòng": "20.75_106.75",
    "thái bình": "20.5_106.25",
}

# Map city name -> model file
CITY_MODEL_MAP = {
    "hà nội": "/Users/buihung/VT/project1/model/aqi_model_hn.pkl",
    "hải phòng": "/Users/buihung/VT/project1/model/aqi_model_hp.pkl",
    "thái bình": "/Users/buihung/VT/project1/model/aqi_model_tb.pkl",
}

# --- TOOL: Phân tích ngày từ truy vấn ---
def parse_date_from_query(query: str) -> datetime.datetime | None:
    try:
        for part in query.split():
            if "/" in part:
                day, month, *year = map(int, part.split("/"))
                year = year[0] if year else datetime.datetime.now().year
                return datetime.datetime(year, month, day, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))
    except Exception:
        return None
    return None

# Hàm chính dự đoán
def get_aqi_from_city_name(query: str) -> str:
    city_name = next((city for city in CITY_COORD_MAP if city in query.lower()), None)
    if not city_name:
        return f"Xin lỗi, tôi không có dữ liệu cho thành phố được yêu cầu trong: {query}."

    entity_id = CITY_COORD_MAP[city_name]
    model_path = CITY_MODEL_MAP[city_name]
    target_date = parse_date_from_query(query)
    is_historical = target_date is not None

    try:
        store = FeatureStore(repo_path="/Users/buihung/VT/project1/feast")
        features = ["aqi_info_v1:aqi", "aqi_info_v1:hour", "aqi_info_v1:day", "aqi_info_v1:dayOfWeek"]

        if is_historical:
            return "⚠️ Truy vấn dữ liệu lịch sử chưa được hỗ trợ qua Redis. Hãy dùng BigQuery hoặc offline store."

        # Lấy dữ liệu hiện tại từ Redis
        feature_data = store.get_online_features(
            features=features,
            entity_rows=[{"entity_id": entity_id}]
        ).to_df()

        if feature_data.empty or pd.isna(feature_data["aqi"].iloc[0]):
            return f"⚠️ Không có dữ liệu AQI cho {city_name.title()} hôm nay. Có thể Redis chưa được cập nhật."

        # Lấy thông tin hiện tại
        current_aqi = feature_data["aqi"].iloc[0]
        hour = int(feature_data["hour"].iloc[0])
        day = int(feature_data["day"].iloc[0])
        dow = int(feature_data["dayOfWeek"].iloc[0])

        # Tính giờ tiếp theo
        next_hour = (hour + 1) % 24
        next_day = day + 1 if next_hour == 0 else day
        next_dow = (dow % 7) + 1 if next_hour == 0 else dow

        # Load đúng model theo từng thành phố
        model_loaded, metadata_loaded = joblib.load(model_path)
        input_df = pd.DataFrame([{
            "hour": next_hour,
            "day": next_day,
            "dayOfWeek": next_dow,
            "last_hour_aqi": current_aqi
        }])

        predicted_aqi = model_loaded.predict(input_df)[0]

        def level(aqi):
            if aqi <= 50: return "Tốt"
            elif aqi <= 100: return "Trung bình"
            elif aqi <= 150: return "Không tốt cho nhóm nhạy cảm"
            else: return "Ô nhiễm, nên hạn chế ra ngoài"

        return (
            f"AQI tại {city_name.title()} hiện tại là {current_aqi:.2f} – {level(current_aqi)}.\n"
            f"✅ Dự đoán AQI giờ tiếp theo (h{next_hour:02d}): {predicted_aqi:.2f} – {level(predicted_aqi)}"
        )

    except Exception as e:
        return f"❌ Không thể lấy hoặc dự đoán dữ liệu AQI cho {city_name.title()}. Lỗi: {str(e)}"
    
# --- TOOL: Lấy ngày hiện tại ---
def get_current_date(query: str) -> str:
    now = datetime.datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    return f"Hôm nay là {now.strftime('%Y-%m-%d %H:%M:%S %Z')}"

# --- GỬI EMAIL ---
def send_email(to_email: str, subject: str, body: str):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = to_email
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"📧 Email sent to {to_email}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

# --- KIỂM TRA AQI ĐỊNH KỲ ---
async def hourly_aqi_check():
    while True:
        print("🔄 Kiểm tra AQI định kỳ...")
        result = get_aqi_from_city_name(f"aqi cho hà nội, không cần đưa ra dự báo")
        if any(w in result.lower() for w in ["ô nhiễm", "không tốt"]):
            subject = f"[CẢNH BÁO AQI] Không khí xấu tại hà nội"
            for email in EMAIL_RECIPIENTS:
                send_email(email, subject, result)
        await asyncio.sleep(3600)  # Chờ 1 tiếng

# --- TẠO AGENT ---
root_agent = Agent(
    name="root_agent",
    model="gemini-2.0-flash",
    instruction=(
        "Bạn là trợ lý AI hữu ích, chuyên cung cấp thông tin AQI cho các thành phố tại Việt Nam. "
        "Bạn có thể cung cấp dữ liệu AQI hiện tại hoặc quá khứ nếu được yêu cầu "
        "(ví dụ: 'AQI cho Hà Nội ngày 24/5'). "
        "Bạn cũng có thể trả lời ngày hiện tại nếu được hỏi. "
        "Nếu không có dữ liệu hoặc yêu cầu không rõ, hãy giải thích rõ ràng và lịch sự."
    ),
    tools=[get_aqi_from_city_name, get_current_date],
)

# --- CHẠY AGENT + VÒNG LẶP ĐỊNH KỲ ---
if __name__ == "__main__":
    def run_scheduler():
        asyncio.run(hourly_aqi_check())

    threading.Thread(target=run_scheduler, daemon=True).start()
    uvicorn.run("agent:root_agent", host="127.0.0.1", port=8001)