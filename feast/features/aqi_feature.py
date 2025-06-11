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

aqi_feature_view = FeatureView(
    name="aqi_info",
    entities=[aqi_entity],
    ttl=timedelta(days=7),
    schema=[
        Field(name="hour", dtype=Int32),
        Field(name="day", dtype=Int32),
        Field(name="dayOfWeek", dtype=Int32),
        Field(name="aqi", dtype=Float32),
    ],
    source=aqi_bq_source,
    online=True,
)
