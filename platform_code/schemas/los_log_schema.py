from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# LOSLOG file schema
from schemas.tables_attributes import (ACID, LATITUDE, LONGITUDE, ALTITUDE,
                                       DISTANCE, LOS_EXIT_TIME, LOS_START_TIME,
                                       LOS_DURATION_TIME, LOS_TIME_MIN_DISTANCE, CRASH)

LOS_LOG_FILE_SCHEMA = StructType([
    StructField(LOS_EXIT_TIME, DoubleType(), False),
    StructField(LOS_START_TIME, DoubleType(), False),
    StructField(LOS_TIME_MIN_DISTANCE, DoubleType(), False),
    StructField(f'{ACID}_1', StringType(), False),
    StructField(f'{ACID}_2', StringType(), False),
    StructField(f'{LATITUDE}_1', DoubleType(), False),
    StructField(f'{LONGITUDE}_1', DoubleType(), False),
    StructField(f'{ALTITUDE}_1', DoubleType(), False),
    StructField(f'{LATITUDE}_2', DoubleType(), False),
    StructField(f'{LONGITUDE}_2', DoubleType(), False),
    StructField(f'{ALTITUDE}_2', DoubleType(), False),
    StructField(DISTANCE, DoubleType(), False)
])

# Final LOSLOG schema
LOS_LOG_SCHEMA = StructType([
    StructField(LOS_EXIT_TIME, DoubleType(), False),
    StructField(LOS_START_TIME, DoubleType(), False),
    # TODO: To be created during parsing
    StructField(LOS_DURATION_TIME, DoubleType(), False),
    StructField(f'{ACID}_1', StringType(), False),
    StructField(f'{ACID}_2', StringType(), False),
    StructField(f'{LATITUDE}_1', DoubleType(), False),
    StructField(f'{LONGITUDE}_1', DoubleType(), False),
    StructField(f'{ALTITUDE}_1', DoubleType(), False),
    StructField(f'{LATITUDE}_2', DoubleType(), False),
    StructField(f'{LONGITUDE}_2', DoubleType(), False),
    StructField(f'{ALTITUDE}_2', DoubleType(), False),
    StructField(DISTANCE, DoubleType(), False),
    # TODO: To be created during parsing
    StructField(CRASH, BooleanType(), False)
])
