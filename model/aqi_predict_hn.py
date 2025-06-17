# ===== Import thư viện =====
import pandas as pd
import numpy as np
import joblib
from feast import FeatureStore
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# ===== STEP 1: Kết nối FEAST và lấy dữ liệu từ Offline Store =====
print("🔗 Kết nối Feature Store...")

# Kết nối repo Feast (bạn thay path nếu cần)
store = FeatureStore(repo_path=".")

# Chuẩn bị entity dataframe chứa entity_id + event_timestamp
# (bạn có thể đọc từ BigQuery nếu cần)
entity_df = pd.DataFrame({
    "entity_id": ["21.0_105.75"] * 100,
    "event_timestamp": pd.date_range(start="2025-05-01", periods=100, freq="H")
})

# Truy vấn dữ liệu historical features từ Feast (lấy từ BigQuery)
print("📥 Truy vấn historical features từ offline store...")
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "aqi_info_v1:hour",
        "aqi_info_v1:day",
        "aqi_info_v1:dayOfWeek",
        "aqi_info_v1:aqi"
    ]
).to_df()

print(f"✅ Dữ liệu huấn luyện: {training_df.shape}")

# ===== STEP 2: Tiền xử lý features bổ sung (tính last_hour_aqi) =====

# Tạo cột datetime chuẩn hóa (event_timestamp + hour)
training_df['datetime'] = pd.to_datetime(training_df['event_timestamp']) + pd.to_timedelta(training_df['hour'], unit='h')
training_df = training_df.sort_values('datetime')

# Tạo feature last_hour_aqi (shift 1 giờ trước)
training_df['last_hour_aqi'] = training_df['aqi'].shift(1)

# Loại bỏ các dòng có NaN (do shift)
training_df = training_df.dropna()

# Chuẩn bị dữ liệu input/output cho model
X = training_df[['hour', 'day', 'dayOfWeek', 'last_hour_aqi']]
y = training_df['aqi']

# ===== STEP 3: Huấn luyện mô hình =====
print("🤖 Huấn luyện mô hình...")

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# ===== STEP 4: Đánh giá mô hình =====
y_pred = model.predict(X_test)
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print("\n📊 Đánh giá mô hình:")
print(f"MAE  (Mean Absolute Error):      {mae:.2f}")
print(f"RMSE (Root Mean Squared Error): {rmse:.2f}")
print(f"R²   (R-squared Score):          {r2:.3f}")

# ===== STEP 5: Lưu model =====
model_path = '/Users/buihung/VT/project1/model/aqi_model_hn.pkl'
joblib.dump(model, model_path)
print(f"✅ Mô hình đã lưu tại {model_path}")

# ===== STEP 6: Mô phỏng Serving Online =====
print("\n🎯 Kiểm thử dự đoán online (Serving Demo)...")

# Giả lập lấy feature online từ Feast
entity_id = "21.0_105.75"
online_features = store.get_online_features(
    features=[
        "aqi_info_v1:hour",
        "aqi_info_v1:day",
        "aqi_info_v1:dayOfWeek",
        "aqi_info_v1:aqi"
    ],
    entity_rows=[{"entity_id": entity_id}]
).to_dict()

# Chuẩn bị dữ liệu phục vụ serving inference
hour = online_features["hour"][0]
day = online_features["day"][0]
day_of_week = online_features["dayOfWeek"][0]
last_hour_aqi = online_features["aqi"][0]  # Giả lập lấy từ online store

# Chuẩn hóa input model
input_data = pd.DataFrame([{
    "hour": hour,
    "day": day,
    "dayOfWeek": day_of_week,
    "last_hour_aqi": last_hour_aqi
}])

training_metadata = {
    "model_version": "v20240613",
    "feature_view_version": "aqi_info_v1",
    "offline_store": "BigQuery",
    "trained_at": "2024-06-13"
}

# Gói model + metadata vào 1 tuple
model_package = (model, training_metadata)
# Load lại mô hình và dự đoán
joblib.dump(model_package, model_path)
model_loaded, metadata_loaded = joblib.load(model_path)
pred = model_loaded.predict(input_data)
print(f"🎯 Dự đoán AQI hiện tại: {pred[0]:.2f}")


# Kiểm tra metadata
# Load lại object từ file pkl
model_package = joblib.load("/Users/buihung/VT/project1/model/aqi_model_hn.pkl")

# Tách ra model và metadata
loaded_model, loaded_metadata = model_package

# In metadata
print("📊 Metadata kèm theo model:")
for k, v in loaded_metadata.items():
    print(f"{k}: {v}")