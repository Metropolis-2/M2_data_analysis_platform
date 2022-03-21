from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

from schemas.tables_attributes import (DEL_TIME, CALL_SIGN, GEOFENCE_ID, GEOFENCE_NAME, MAX_INTRUSION,
                                       INTRUSION_LATITUDE, INTRUSION_LONGITUDE, INTRUSION_TIME, SCENARIO_NAME, GEO_ID,
                                       VIOLATION_SEVERITY, OPEN_AIRSPACE)

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

GEO_LOG_SCHEMA = StructType([
    StructField(GEO_ID, IntegerType(), False),
    StructField(SCENARIO_NAME, StringType(), False),
    StructField(DEL_TIME, DoubleType(), False),
    StructField(GEOFENCE_NAME, StringType(), False),
    StructField(MAX_INTRUSION, DoubleType(), False),
    StructField(VIOLATION_SEVERITY, BooleanType(), False),
    StructField(OPEN_AIRSPACE, BooleanType(), False),
])

GEO_LOG_COLUMNS = [GEO_ID,
                   SCENARIO_NAME,
                   DEL_TIME,
                   GEOFENCE_NAME,
                   MAX_INTRUSION,
                   VIOLATION_SEVERITY,
                   OPEN_AIRSPACE]
COLUMNS_TO_DROP = [CALL_SIGN, GEOFENCE_ID, INTRUSION_LATITUDE, INTRUSION_LONGITUDE, INTRUSION_TIME]