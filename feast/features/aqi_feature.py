from feast import Entity, Field, FeatureView
from feast.types import Int32, Float32
from feast import BigQuerySource
from datetime import timedelta

aqi_entity = Entity(
    name="entity_id",
    join_keys=["entity_id"],
    description="City ID (lat_lon string)",
)

aqi_bq_source = BigQuerySource(
    table="iron-envelope-455716-g8.aq_data.aqi_info",  # thay bằng project.dataset.table của bạn
    timestamp_field="feature_timestamp",
)

aqi_feature_view_v1 = FeatureView(
    name="aqi_info_v1",
    entities=[aqi_entity],
    ttl=timedelta(days=7),
    schema=[
        Field(name="hour", dtype=Int32, description="Hour of the day"),
        Field(name="day", dtype=Int32, description="Day of the month"),
        Field(name="dayOfWeek", dtype=Int32, description="Day of the week (0=Sunday, 6=Saturday)"),
        Field(name="aqi", dtype=Float32, description="Air Quality Index(US) value, calculated from PM2.5 concentration"),
    ],
    source=aqi_bq_source,
    online=True,
    tags={"owner": "ml_team", "project": "air_quality_prediction", "created_date": "2025-06-01" },
)

aqi_feature_view_v2 = FeatureView(
    name="aqi_info_v2",
    entities=[aqi_entity],
    ttl=timedelta(days=7),
    schema=[
        Field(name="hour", dtype=Int32, description="Hour of the day"),
        Field(name="day", dtype=Int32, description="Day of the month"),
        Field(name="dayOfWeek", dtype=Int32, description="Day of the week (0=Sunday, 6=Saturday)"),
        Field(name="aqi", dtype=Float32, description="Air Quality Index(EU) value, calculated from PM2.5 concentration"),
    ],
    source=aqi_bq_source,
    online=True,
    tags={"owner": "ml_team", "project": "air_quality_prediction", "created_date": "2025-06-0" },
)