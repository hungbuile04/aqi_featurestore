
from google.cloud import bigquery
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor
import pandas as pd
import joblib
import numpy as np

# ====== STEP 1: Truy vấn dữ liệu từ BigQuery ======
# 1. Khởi tạo client BigQuery
client = bigquery.Client()

# 2. Truy vấn dữ liệu có thêm trường "timestamp" để sắp xếp
query = """
SELECT
  dayOfWeek AS day_of_week,
  hour,
  day,
  aqi,
  feature_timestamp
FROM `iron-envelope-455716-g8.aq_data.aqi_info`
WHERE entity_id = '20.5_106.25'
  AND (EXTRACT(MONTH FROM feature_timestamp) = 5 OR EXTRACT(MONTH FROM feature_timestamp) = 6)
ORDER BY feature_timestamp, hour
"""

df = client.query(query).to_dataframe()

# ====== STEP 2: Tiền xử lý & Huấn luyện mô hình ======
# 3. Tạo cột last_hour_aqi
df['datetime'] = pd.to_datetime(df['feature_timestamp']) + pd.to_timedelta(df['hour'], unit='h')
df = df.sort_values('datetime')
df['last_hour_aqi'] = df['aqi'].shift(1)

# 4. Loại bỏ NaN
df = df.dropna()

# 5. Train/test split
X = df[['hour', 'day', 'day_of_week', 'last_hour_aqi']]
y = df['aqi']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 6. Huấn luyện mô hình
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# ====== STEP 3: Đánh giá mô hình ======
y_pred = model.predict(X_test)   
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print("\n📊 Đánh giá mô hình:")
print(f"MAE  (Mean Absolute Error):      {mae:.2f}")
print(f"RMSE (Root Mean Squared Error): {rmse:.2f}")
print(f"R²   (R-squared Score):          {r2:.3f}")


# 7. Dự đoán và lưu mô hình
preds = model.predict(X_test)
joblib.dump(model, '/Users/buihung/VT/project1/model/aqi_model_tb.pkl')
print("✅ Mô hình đã lưu thành 'aqi_model_tb.pkl' với feature last_hour_aqi")


# ====== STEP 3: Giả lập dữ liệu online và dự đoán (Serving) ======
print("\n🎯 Kiểm thử dự đoán online:")
model = joblib.load("/Users/buihung/VT/project1/model/aqi_model_tb.pkl")

# Dữ liệu giả
input_data = pd.DataFrame([{       # Thứ Ba
    "hour": 14, 
    "day": 15,   
    "day_of_week": 2,           # 2 PM
    "last_hour_aqi": 75.0    # AQI của giờ trước
}])

pred = model.predict(input_data)
print(f"Dự đoán AQI hiện tại: {pred[0]:.2f}")