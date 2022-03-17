from pyspark.sql.types import StructType, StructField, StringType, DoubleType

CONF_LOG_SCHEMA = StructType([
    StructField("simt", DoubleType(), False),
    StructField("ACID1", StringType(), False),
    StructField("ACID2", StringType(), False),
    StructField("LAT1", DoubleType(), False),
    StructField("LON1", DoubleType(), False),
    StructField("ALT1", DoubleType(), False),
    StructField("LAT2", DoubleType(), False),
    StructField("LON2", DoubleType(), False),
    StructField("ALT2", DoubleType(), False),
    StructField("CPALAT", DoubleType(), False),
    StructField("CPALON", DoubleType(), False)
])
