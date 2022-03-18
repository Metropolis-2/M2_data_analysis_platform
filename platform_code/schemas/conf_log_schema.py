from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from schemas.tables_attributes import (CONF_DETECTED_TIME, ACID, LATITUDE,
                                       ALTITUDE, LONGITUDE, CPALON, CPALAT,
                                       SCENARIO_NAME, CONF_ID)

CONF_LOG_FILE_SCHEMA = StructType([
    StructField(CONF_DETECTED_TIME, DoubleType(), False),
    StructField(f'{ACID}_1', StringType(), False),
    StructField(f'{ACID}_2', StringType(), False),
    StructField(f'{LATITUDE}_1', DoubleType(), False),
    StructField(f'{LONGITUDE}_1', DoubleType(), False),
    StructField(f'{ALTITUDE}_1', DoubleType(), False),
    StructField(f'{LATITUDE}_2', DoubleType(), False),
    StructField(f'{LONGITUDE}_2', DoubleType(), False),
    StructField(f'{ALTITUDE}_2', DoubleType(), False),
    StructField(CPALAT, DoubleType(), False),
    StructField(CPALON, DoubleType(), False)
])

CONF_LOG_SCHEMA = StructType([
    StructField(CONF_ID, IntegerType(), False),
    StructField(SCENARIO_NAME, StringType(), False),
    StructField(CONF_DETECTED_TIME, DoubleType(), False),
    StructField(CPALAT, DoubleType(), False),
    StructField(CPALON, DoubleType(), False)
])

CONF_LOG_COLUMNS = [CONF_ID,
                    SCENARIO_NAME,
                    CONF_DETECTED_TIME,
                    CPALAT,
                    CPALON]
COLUMNS_TO_DROP = [f'{ACID}_1', f'{ACID}_2', f'{LATITUDE}_1', f'{LONGITUDE}_1',
                   f'{ALTITUDE}_1', f'{LATITUDE}_2', f'{LONGITUDE}_2', f'{ALTITUDE}_2']
