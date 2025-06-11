from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, from_unixtime, hour, to_date, year, month, dayofmonth, dayofweek, monotonically_increasing_id, udf, hash, round as spark_round, concat_ws
from pyspark.sql.types import DoubleType, DecimalType, StructType, StructField, LongType, StringType
import re
import os
from dotenv import load_dotenv
from google.cloud import bigquery, storage
from datetime import datetime, timedelta, timezone
import json
from decimal import Decimal
import io
import pyarrow
from pyarrow.fs import GcsFileSystem

# Load environment variables
load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not GOOGLE_APPLICATION_CREDENTIALS:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS không được tìm thấy! Kiểm tra lại .env file.")

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("Read from GCS") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.jars", "/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar, /opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
    .config("spark.hadoop.google.cloud.project.id", "iron-envelope-455716-g8") \
    .getOrCreate()

client = storage.Client()
bucket = client.get_bucket("project-bigdata-bucket")
prefix = "air_quality_data/"
folders = set()

with open("/opt/spark/start_date.txt", "r") as f:
    start_date_str = f.read().strip()
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

latest_date = None

# Lấy các phân vùng có dạng year=YYYY/month=MM/day=DD
for blob in bucket.list_blobs(prefix=prefix):
    match = re.match(r"air_quality_data/year=(\d+)/month=(\d+)/day=(\d+)/", blob.name)
    if match:
        y, m, d = map(int, match.groups())
        folder_date = datetime(y, m, d)
        
        if folder_date >= start_date:
            folder_path = f"air_quality_data/year={y}/month={m}/day={d}"
            folders.add(folder_path)
            if latest_date is None or folder_date > latest_date:
                latest_date = folder_date

folder_paths = sorted(folders)
print(f"✅ Tìm được {len(folder_paths)} ngày dữ liệu phân vùng.")


full_path = f"gs://project-bigdata-bucket/{folder_paths[0]}"
print(f"🚀 Đang đọc dữ liệu: {full_path}")

# Hàm trích xuất datetime từ đường dẫn thư mục
def extract_date_from_path(path):
    match = re.search(r"year=(\d+)/month=(\d+)/day=(\d+)", path)
    if match:
        y, m, d = map(int, match.groups())
        return datetime(y, m, d)
    return None

# Sắp xếp theo datetime
folder_paths_sorted = sorted(folder_paths, key=extract_date_from_path, reverse=True)

for folder in folder_paths_sorted:
    full_path = f"gs://project-bigdata-bucket/{folder}"
    print(f"🚀 Đang xử lý phân vùng: {full_path}")

    # Đọc toàn bộ thư mục (không cần lặp từng file nữa)
    df = spark.read.parquet(full_path)

    # Tiền xử lý dữ liệu
    df = df.withColumn("date", to_date(from_unixtime(col("dt")))) \
           .withColumn("hour", hour(from_unixtime(col("dt")))) \
           .withColumn("year", year("date")) \
           .withColumn("month", month("date")) \
           .withColumn("day", dayofmonth("date")) \
           .withColumn("dayOfWeek", dayofweek("date")) \
           .withColumn("entity_id", concat_ws("_", col("lat").cast("string"), col("lon").cast("string"))) \
           .withColumn("feature_timestamp", from_unixtime(col("dt")).cast("timestamp"))

    # AQI UDF
    def calculate_aqi_pm25(concentration):
        # Định nghĩa các khoảng nồng độ và AQI tương ứng
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
        return 8.5

    aqi_pm25_udf = udf(calculate_aqi_pm25, DoubleType())
    df = df.withColumn("aqi", aqi_pm25_udf(df["pm2_5"]))

    df = df.select("entity_id", "feature_timestamp", "dt", "lat", "lon", "aqi", "hour", "day", "dayOfWeek")

    # ✅ Ghi dữ liệu vào BigQuery
    df.write.format("bigquery") \
        .option("table", "iron-envelope-455716-g8.aq_data.aqi_info") \
        .option("parentProject", "iron-envelope-455716-g8") \
        .option("temporaryGcsBucket", "project-bigdata-bucket") \
        .mode("append") \
        .save()

print("✅ Đã ghi dữ liệu vào BigQuery thành công!")

# Sau vòng lặp, lưu ngày kế tiếp của latest_date vào file nếu có
if latest_date:
    next_date = latest_date + timedelta(days=1)
    with open("/opt/spark/start_date.txt", "w") as f:
        f.write(next_date.strftime("%Y-%m-%d"))
    print(f"✅ Đã lưu ngày tiếp theo: {next_date.strftime('%Y-%m-%d')}")
else:
    print("⚠️ Không có ngày nào phù hợp để cập nhật.")

def save_lineage_test():
    project_id = "iron-envelope-455716-g8"
    dataset_id = "aq_data"
    table_id = "lineage"
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    rows_to_insert = [
        {
            "name": "month",
            "version": "v1",
            "source": "dt",
            "transformation_file": "write_to_bigquery.py",
            "timestamp": timestamp,
        },
    ]

    # Chuyển danh sách dict thành NDJSON (newline-delimited JSON)
    json_lines = "\n".join([json.dumps(row) for row in rows_to_insert])
    json_buffer = io.StringIO(json_lines)

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Ghi nối tiếp
    )

    load_job = client.load_table_from_file(
        json_buffer, full_table_id, job_config=job_config
    )

    load_job.result()  # Chờ job hoàn tất

    print(f"✅ Đã ghi {len(rows_to_insert)} dòng vào bảng {full_table_id}")

def insert_feature_metadata():
    project_id = "iron-envelope-455716-g8"
    dataset_id = "aq_data"
    table_id = "feature_metadata"

    client = bigquery.Client()

    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    rows_to_insert = [
        {
            "feature_name": "aqi",
            "version": "v1",
            "formula": "US_EPA tính từ PM2.5",
            "description": "AQI theo tiêu chuẩn US EPA",
            "created_at": "2025-06-01 10:00:00"
        },
        {
            "feature_name": "day",
            "version": "v1",
            "formula": "Ngày trích xuất từ unixtime",
            "description": "Ngày trong tháng",
            "created_at": "2025-06-01 10:00:00"
        },
        {
            "feature_name": "day_of_week",
            "version": "v1",
            "formula": "Ngày trong tuần trích xuất từ unixtime",
            "description": "Ngày trong tuần",
            "created_at": "2025-06-01 10:00:00"
        },
        {
            "feature_name": "hour",
            "version": "v1",
            "formula": "Giờ trong ngày trích xuất từ unixtime",
            "description": "Giờ trong ngày",
            "created_at": "2025-06-01 10:00:00"
        },
    ]

    errors = client.insert_rows_json(table, rows_to_insert)

    if not errors:
        print("✅ Đã chèn bản ghi vào bảng feature_metadata.")
    else:
        print(f"❌ Lỗi khi chèn dữ liệu: {errors}")