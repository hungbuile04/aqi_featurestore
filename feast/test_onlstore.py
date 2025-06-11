import pandas as pd
from datetime import datetime
from pytz import timezone
from feast import FeatureStore

store = FeatureStore(repo_path=".")

df = pd.DataFrame([
    {
        "entity_id": "21.0_105.75",
        "hour": 14,
        "day": 15,
        "dayOfWeek": 2,
        "aqi": 75.0,
        "feature_timestamp": datetime.now(tz=timezone("Asia/Ho_Chi_Minh")),
    },
])

store.write_to_online_store(feature_view_name="aqi_info", df=df)