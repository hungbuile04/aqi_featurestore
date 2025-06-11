import redis
from urllib.parse import urlparse
import ssl

# Redis URL
redis_url = "redis://default:7kJpfxzQJlyxxCJ4aO5vVc4hzkYOgUtu@redis-11719.c334.asia-southeast2-1.gce.redns.redis-cloud.com:11719"
parsed = urlparse(redis_url)

# Cách 1: Thử kết nối không SSL trước
print("Thử kết nối không SSL...")
try:
    r = redis.StrictRedis(
        host=parsed.hostname,
        port=parsed.port,
        username=parsed.username,
        password=parsed.password,
        ssl=False,  # Tắt SSL
        socket_timeout=5,
        socket_connect_timeout=5
    )
    
    # Test connection
    r.ping()
    print("✅ Kết nối thành công không SSL!")
    
    # Dữ liệu mẫu
    sample_data = {
        "lat": 21.0,
        "lon": 105.75,
        "dt": 1717483200,
        "aqi": 75
    }

    # Tạo key
    key = f"aqi:{sample_data['lat']}:{sample_data['lon']}"

    # Đẩy dữ liệu lên Redis (dạng hash)
    r.hset(key, mapping=sample_data)
    
    result = r.hgetall(key)
    print("Data:", {k.decode(): v.decode() for k, v in result.items()})
    
except Exception as e:
    print(f"❌ Kết nối không SSL thất bại: {e}")