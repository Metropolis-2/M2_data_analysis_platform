from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType

GEO_LOG_SCHEMA = StructType([
    StructField("Deletion_time", FloatType(), False),
    StructField("Call_sign", FloatType(), False),
    StructField("Geofence_ID", FloatType(), False),
    StructField("Geofence_name", FloatType(), False),
    StructField("Max_intrusion", FloatType(), False),
    StructField("Intrusion_LAT", FloatType(), False),
    StructField("Intrusion_LON", FloatType(), False),
    StructField("Intrusion_time", FloatType(), False)
])