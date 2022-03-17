from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from schemas.tables_attributes import SCENARIO_NAME

REG_LOG_SCHEMA = StructType([
    StructField("REG_ID", IntegerType(), False),
    StructField(SCENARIO_NAME, StringType(), False),
    StructField("Simulation_time", DoubleType(), False),
    StructField("ACID", StringType(), False),
    StructField("ALT", DoubleType(), False),
    StructField("LAT", DoubleType(), False),
    StructField("LON", DoubleType(), False)
])
