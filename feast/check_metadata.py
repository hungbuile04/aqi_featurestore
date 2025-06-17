from feast import FeatureStore

store = FeatureStore(repo_path=".")

# Xem toàn bộ feature view
feature_views = store.list_feature_views()

for fv in feature_views:
    print(f"Feature View Name: {fv.name}")
    print("Features:")
    for f in fv.schema:
        print(f"  {f.name}: {f.dtype}")
    print(f"Entities: {fv.entities}")
    print(f"TTL: {fv.ttl}")
    print(f"Source: {fv.source}")
