from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from schemas.tables_attributes import (ACID, SCENARIO_NAME, DEL_TIME, SPAWN_TIME, DISTANCE_2D, FLIGHT_TIME, DISTANCE_3D,
                                       VERTICAL_DISTANCE, WORK_DONE, DEL_LATITUDE, DEL_LONGITUDE, DEL_ALTITUDE, DEL_X,
                                       DEL_Y, TAS, VERTICAL_SPEED, HEADING, ASAS_ACTIVE, PILOT_ALT, PILOT_SPD,
                                       PILOT_HDG, PILOT_VS)

# The heading of the FLSTLOG is the following, with the names changed made:
# - Deletion Time [s] -> DELETION_TIME
# - Call sign [-] -> ACID
# - Spawn Time [s] -> SPAWN_TIME
# - Flight time [s] -> FLIGHT_TIME
# - Distance 2D [m] -> DISTANCE_2D
# - Distance 3D [m] -> DISTANCE_3D
# - Distance ALT [ft] -> VERTICAL_DISTANCE this includes the
# - Work Done [MJ] -> Removed, wrong values that have to be calculated
# - Latitude [deg] -> DEL_LATITUDE
# - Longitude [deg] -> DEL_LONGITUDE
# - Altitude [ft] -> DEL_ALTITUDE
# - TAS [kts] -> Removed
# - Vertical Speed [fpm] -> Removed
# - Heading [deg] -> Removed
# - ASAS Active [bool] -> Removed
# - Pilot ALT [ft] -> Removed
# - Pilot SPD (TAS) [kts] -> Removed
# - Pilot HDG [deg] -> Removed
# - Pilot VS [fpm] -> Removed

FLST_LOG_FILE_SCHEMA = StructType([
    StructField(DEL_TIME, DoubleType(), False),
    StructField(ACID, StringType(), False),
    StructField(SPAWN_TIME, DoubleType(), False),
    StructField(FLIGHT_TIME, StringType(), False),
    StructField(DISTANCE_2D, StringType(), False),
    StructField(DISTANCE_3D, DoubleType(), False),
    StructField(VERTICAL_DISTANCE, DoubleType(), False),
    StructField(WORK_DONE, DoubleType(), False),
    StructField(DEL_LATITUDE, DoubleType(), False),
    StructField(DEL_LONGITUDE, DoubleType(), False),
    StructField(DEL_ALTITUDE, DoubleType(), False),
    StructField(TAS, DoubleType(), False),
    StructField(VERTICAL_SPEED, DoubleType(), False),
    StructField(HEADING, DoubleType(), False),
    StructField(ASAS_ACTIVE, DoubleType(), False),
    StructField(PILOT_ALT, DoubleType(), False),
    StructField(PILOT_SPD, DoubleType(), False),
    StructField(PILOT_HDG, DoubleType(), False),
    StructField(PILOT_VS, DoubleType(), False)
])

# Final FLST LOG columns in dataframe
FLST_LOG_COLUMNS = [
    SCENARIO_NAME,
    ACID,
    DEL_TIME,
    SPAWN_TIME,
    FLIGHT_TIME,
    DISTANCE_2D,
    DISTANCE_3D,
    VERTICAL_DISTANCE,
    DEL_LATITUDE,
    DEL_LONGITUDE,
    DEL_ALTITUDE,
    DEL_X,
    DEL_Y
]

# Removed FLST LOG columns from file
COLUMNS_TO_DROP = [
    TAS,
    VERTICAL_SPEED,
    HEADING,
    ASAS_ACTIVE,
    PILOT_ALT,
    PILOT_SPD,
    PILOT_HDG,
    PILOT_VS
]
