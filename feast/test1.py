from feast.infra.online_stores.redis import RedisOnlineStoreConfig
from urllib.parse import urlparse

# Test URL parsing
url = "redis://default:7kJpfxzQJlyxxCJ4aO5vVc4hzkYOgUtu@redis-11719.c334.asia-southeast2-1.gce.redns.redis-cloud.com:11719"
parsed = urlparse(url)

print(f"scheme: {parsed.scheme}")
print(f"username: {parsed.username}")
print(f"password: {parsed.password}")
print(f"hostname: {parsed.hostname}")
print(f"port: {parsed.port}")
print(f"path: {parsed.path}")