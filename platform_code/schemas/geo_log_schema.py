from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from schemas.tables_attributes import (DEL_TIME, CALL_SIGN, GEOFENCE_ID, GEOFENCE_NAME, MAX_INTRUSION,
                                       INTRUSION_LATITUDE, INTRUSION_LONGITUDE, INTRUSION_TIME, SCENARIO_NAME, GEO_ID,
                                       VIOLATION_SEVERITY, OPEN_AIRSPACE, LOITERING_NFZ)

GEO_LOG_FILE_SCHEMA = StructType([
    StructField(DEL_TIME, DoubleType(), False),
    StructField(CALL_SIGN, StringType(), False),
    StructField(GEOFENCE_ID, IntegerType(), False),
    StructField(GEOFENCE_NAME, StringType(), False),
    StructField(MAX_INTRUSION, DoubleType(), False),
    StructField(INTRUSION_LATITUDE, DoubleType(), False),
    StructField(INTRUSION_LONGITUDE, DoubleType(), False),
    StructField(INTRUSION_TIME, DoubleType(), False)
])

# Final GEOLOG columns in dataframe
GEO_LOG_COLUMNS = [
    GEO_ID,
    SCENARIO_NAME,
    DEL_TIME,
    GEOFENCE_NAME,
    MAX_INTRUSION,
    VIOLATION_SEVERITY,
    OPEN_AIRSPACE,
    LOITERING_NFZ
]

COLUMNS_TO_DROP = [
    CALL_SIGN,
    GEOFENCE_ID,
    INTRUSION_LATITUDE,
    INTRUSION_LONGITUDE,
    INTRUSION_TIME
]
