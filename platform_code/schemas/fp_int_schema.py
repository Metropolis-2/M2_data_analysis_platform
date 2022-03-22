from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

from schemas.tables_attributes import (ACID, FLST_ID, RECEPTION_TIME, VEHICLE, DEPARTURE_TIME,
                                       FINAL_LOCATION, INITIAL_LOCATION, PRIORITY, GEOFENCE_DURATION,
                                       GEOFENCE_BBOX_POINT1_LON, GEOFENCE_BBOX_POINT2_LON, GEOFENCE_BBOX_POINT1_LAT,
                                       GEOFENCE_BBOX_POINT2_LAT, DESTINATION_LON, DESTINATION_LAT,
                                       BASELINE_DEPARTURE_TIME, CRUISING_SPEED, LOITERING, BASELINE_2D_DISTANCE,
                                       BASELINE_VERTICAL_DISTANCE, BASELINE_ASCENDING_DISTANCE, BASELINE_3D_DISTANCE,
                                       BASELINE_FLIGHT_TIME, BASELINE_ARRIVAL_TIME, DESTINATION_Y, ORIGIN_LAT,
                                       ORIGIN_LON, DESTINATION_X)

FP_INT_FILE_SCHEMA = StructType([
    StructField(RECEPTION_TIME, StringType(), False),
    StructField(ACID, StringType(), False),
    StructField(VEHICLE, StringType(), False),
    StructField(DEPARTURE_TIME, StringType(), False),
    StructField(INITIAL_LOCATION, StringType(), False),
    StructField(FINAL_LOCATION, StringType(), False),
    StructField(PRIORITY, IntegerType(), False),
    StructField(GEOFENCE_DURATION, DoubleType(), True),
    StructField(GEOFENCE_BBOX_POINT1_LON, DoubleType(), True),
    StructField(GEOFENCE_BBOX_POINT2_LON, DoubleType(), True),
    StructField(GEOFENCE_BBOX_POINT1_LAT, DoubleType(), True),
    StructField(GEOFENCE_BBOX_POINT2_LAT, DoubleType(), True)
])

FP_INT_SCHEMA = StructType([
    StructField(FLST_ID, IntegerType(), False),
    StructField(ACID, StringType(), False),
    StructField(ORIGIN_LAT, DoubleType(), False),
    StructField(ORIGIN_LON, DoubleType(), False),
    StructField(DESTINATION_LAT, DoubleType(), False),
    StructField(DESTINATION_LON, DoubleType(), False),
    StructField(BASELINE_DEPARTURE_TIME, DoubleType(), False),
    StructField(CRUISING_SPEED, DoubleType(), False),
    StructField(PRIORITY, DoubleType(), False),
    StructField(LOITERING, BooleanType(), False),
    StructField(BASELINE_2D_DISTANCE, DoubleType(), False),
    StructField(BASELINE_VERTICAL_DISTANCE, DoubleType(), False),
    StructField(BASELINE_ASCENDING_DISTANCE, DoubleType(), False),
    StructField(BASELINE_3D_DISTANCE, DoubleType(), False),
    StructField(BASELINE_FLIGHT_TIME, DoubleType(), False),
    StructField(BASELINE_ARRIVAL_TIME, DoubleType(), False),
    StructField(DESTINATION_X, DoubleType(), False),
    StructField(DESTINATION_Y, DoubleType(), False)
])

FP_INT_COLUMNS = [FLST_ID,
                  ACID,
                  ORIGIN_LAT,
                  ORIGIN_LON,
                  DESTINATION_LAT,
                  DESTINATION_LON,
                  BASELINE_DEPARTURE_TIME,
                  CRUISING_SPEED,
                  PRIORITY,
                  LOITERING,
                  BASELINE_2D_DISTANCE,
                  BASELINE_VERTICAL_DISTANCE,
                  BASELINE_ASCENDING_DISTANCE,
                  BASELINE_3D_DISTANCE,
                  BASELINE_FLIGHT_TIME,
                  BASELINE_ARRIVAL_TIME,
                  DESTINATION_X,
                  DESTINATION_Y]

COLUMNS_TO_DROP = [VEHICLE,
                   DEPARTURE_TIME,
                   INITIAL_LOCATION,
                   FINAL_LOCATION,
                   GEOFENCE_DURATION,
                   GEOFENCE_BBOX_POINT1_LON,
                   GEOFENCE_BBOX_POINT2_LON,
                   GEOFENCE_BBOX_POINT1_LAT,
                   GEOFENCE_BBOX_POINT2_LAT]
