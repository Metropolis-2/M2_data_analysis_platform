from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

from schemas.tables_attributes import (SCENARIO_NAME, ALTITUDE, LONGITUDE, LATITUDE,
                                       ACID, REG_ID, SIMULATION_TIME, SIMT_VALID)

REG_LOG_SCHEMA = StructType([
    StructField(REG_ID, IntegerType(), False),
    StructField(SCENARIO_NAME, StringType(), False),
    StructField(ACID, StringType(), False),
    StructField(SIMULATION_TIME, DoubleType(), False),
    StructField(SIMT_VALID, BooleanType(), False),
    StructField(ALTITUDE, DoubleType(), False),
    StructField(LATITUDE, DoubleType(), False),
    StructField(LONGITUDE, DoubleType(), False)
])
