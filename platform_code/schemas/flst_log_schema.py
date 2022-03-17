from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType

FLST_LOG_SCHEMA = StructType([
    StructField("Deletion_Time", FloatType(), False),
    StructField("Call_sign", FloatType(), False),
    StructField("Spawn_Time", FloatType(), False),
    StructField("Flight_time", StringType(), False),
    StructField("Distance_2D", StringType(), False),
    StructField("Distance_3D", DoubleType(), False),
    StructField("Distance_ALT", DoubleType(), False),
    StructField("Work_Done", DoubleType(), False),
    StructField("Latitude", DoubleType(), False),
    StructField("Longitude", DoubleType(), False),
    StructField("Altitude", DoubleType(), False),
    StructField("TAS", DoubleType(), False),
    StructField("Vertical_Speed", DoubleType(), False),
    StructField("Heading", DoubleType(), False),
    StructField("ASAS_Active", DoubleType(), False),
    StructField("Pilot_ALT", DoubleType(), False),
    StructField("Pilot_SPD", DoubleType(), False),
    StructField("Pilot_HDG", DoubleType(), False),
    StructField("Pilot_VS", DoubleType(), False)
])
