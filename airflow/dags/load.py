import os
import ijson
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage, bigquery
from datetime import datetime
import time
from urllib.parse import urlparse

# Load environment variables
load_dotenv()

# Initialize Redis client
import redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

GCS_BUCKET = "project-bigdata-bucket"
GCS_FOLDER = "air_quality_data"
GOOGLE_SERVICE_ACCOUNT_KEY = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
#LOCAL_FILE_PATH = "/Users/buihung/VT/project1/airflow/dags/air_quality_data.json"
LOCAL_FILE_PATH = "/opt/airflow/dags/air_quality_data.json"

if not GOOGLE_SERVICE_ACCOUNT_KEY:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY không được tìm thấy! Kiểm tra lại file .env")

# Set Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_SERVICE_ACCOUNT_KEY

# Khởi tạo GCS
gcs = pa.fs.GcsFileSystem()

# Initialize GCS Client
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET)


# Redis URL
redis_url = "redis://default:7kJpfxzQJlyxxCJ4aO5vVc4hzkYOgUtu@redis-11719.c334.asia-southeast2-1.gce.redns.redis-cloud.com:11719"
parsed = urlparse(redis_url)

# Parse URL
parsed_url = urlparse(redis_url)

# Kết nối Redis Cloud
r = redis.StrictRedis(
        host=parsed.hostname,
        port=parsed.port,
        username=parsed.username,
        password=parsed.password,
        ssl=False,  # Tắt SSL
        socket_timeout=5,
        socket_connect_timeout=5
    )

def save_lineage():
    # Cấu hình
    project_id = "iron-envelope-455716-g8"
    dataset_id = "aq_data"
    table_id = "lineage"
    timestamp = datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Khởi tạo BigQuery client
    client = bigquery.Client()

    #Tạo câu truy vấn kiểm tra xem bản ghi đã tồn tại chưa
    query_check = f"""
    SELECT COUNT(*) as record_count
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE feature_name = 'pm2_5'
    AND version = 'v1'
    """

    query_job = client.query(query_check)
    result = query_job.result()
    record_count = list(result)[0].record_count

    if record_count == 0:
        # Nếu chưa tồn tại thì chèn bản ghi mới
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)

        rows_to_insert = [{
            "feature_name": "pm2_5",
            "version": "v1",
            "source": "API Openweather",
            "transformation_file": "extract.py",
            "timestamp": timestamp,
        }]

        errors = client.insert_rows_json(table, rows_to_insert)

        if errors == []:
            print("✅ Dòng mới đã được thêm vào bảng lineage.")
        else:
            print(f"❌ Lỗi khi thêm dòng vào bảng lineage: {errors}")
    else:
        print("⚠️ Dữ liệu đã tồn tại trong bảng lineage. Không thêm lại.")

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
            return round(((i_hi - i_lo) / (bp_hi - bp_lo)) * (concentration - bp_lo) + i_lo)
    return None

def upload_large_json_to_gcs(batch_size=25000):
    def write_batch_to_gcs(batch, batch_num):
        df = pd.DataFrame(batch)

        df["dt"] = pd.to_datetime(df["dt"], unit="s", errors='coerce')
        df["year"] = df["dt"].dt.year
        df["month"] = df["dt"].dt.month
        df["day"] = df["dt"].dt.day
        df["dt"] = df["dt"].astype('int64') // 1_000_000_000

        float_columns = ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3", "lat", "lon"]
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(float).round(2)

        # Tính AQI từ pm2_5 nếu có
        if "pm2_5" in df.columns:
            df["aqi"] = df["pm2_5"].apply(lambda x: calculate_aqi_pm25(x) if pd.notna(x) else None)

        # Ghi vào Redis
        for _, row in df.iterrows():
            if pd.notna(row.get("lat")) and pd.notna(row.get("lon")):
                key = f"aqi:{row['lat']}:{row['lon']}"
                redis_data = {
                    "lat": row["lat"],
                    "lon": row["lon"],
                    "dt": int(row["dt"]),
                    "aqi": int(row["aqi"]) if pd.notna(row["aqi"]) else None
                }
                # redis_client.hset(key, mapping=redis_data)

        # Ghi từng nhóm partition theo ngày riêng
        grouped = df.groupby(["year", "month", "day"])

        # Define consistent schema
        schema = pa.schema([
            ("dt", pa.int64()),
            ("lat", pa.float64()),
            ("lon", pa.float64()),
            ("co", pa.float64()),
            ("no", pa.float64()),
            ("no2", pa.float64()),
            ("o3", pa.float64()),
            ("so2", pa.float64()),
            ("pm2_5", pa.float64()),
            ("pm10", pa.float64()),
            ("nh3", pa.float64()),
            ("aqi_level", pa.int64()),
            # add other fields if needed
        ])

        for (year, month, day), group_df in grouped:
            # Bỏ 3 trường phân vùng khỏi DataFrame
            group_df = group_df.drop(columns=["year", "month", "day"])
            group_df = group_df.drop(columns=["aqi"])

            table = pa.Table.from_pandas(group_df, schema=schema, preserve_index=False)
            current_unix_time = int(time.time())
            partition_path = f"{GCS_FOLDER}/year={year}/month={month}/day={day}/batch_{batch_num}_{current_unix_time}.parquet"

            with gcs.open_output_stream(f"{GCS_BUCKET}/{partition_path}") as out_stream:
                pq.write_table(table, out_stream)

            print(f"✅ Batch {batch_num}: Đã ghi {len(group_df)} bản ghi vào {partition_path}")

    with open(LOCAL_FILE_PATH, "r", encoding="utf-8") as f:
        parser = ijson.items(f, "item")
        batch = []
        count = 0

        for record in parser:
            batch.append(record)
            if len(batch) >= batch_size:
                write_batch_to_gcs(batch, count)
                count += 1
                batch = []

        if batch:
            write_batch_to_gcs(batch, count)

    print(f"🎉 Hoàn tất tải dữ liệu lên GCS theo từng batch Parquet!")