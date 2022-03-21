from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, col, when, hour, minute, second, split, regexp_replace
from pyspark.sql.types import DoubleType

from config import settings
from parse.parser_utils import add_dataframe_counter, get_drone_speed, transform_location
from schemas.fp_int_schema import COLUMNS_TO_DROP, FP_INTENTION_COLUMNS, FP_INTENTION_FILE_SCHEMA
from schemas.tables_attributes import (BASELINE_VERTICAL_DISTANCE, BASELINE_ASCENDING_DISTANCE, BASELINE_3D_DISTANCE,
                                       BASELINE_FLIGHT_TIME, BASELINE_ARRIVAL_TIME, BASELINE_2D_DISTANCE, DESTINATION_X,
                                       DESTINATION_Y, LOITERING, FLST_ID, CRUISING_SPEED, ORIGIN_LON, ORIGIN_LAT,
                                       BASELINE_DEPARTURE_TIME, VEHICLE, GEOFENCE_DURATION, DEPARTURE_TIME,
                                       RECEPTION_TIME, INITIAL_LOCATION, FINAL_LOCATION, DESTINATION_LAT,
                                       DESTINATION_LON, LATITUDE, LONGITUDE)

PARENTHESIS_PATTERN = "\(|\)"
COMMA_PATTERN = ', '


def remove_fp_int_unused_columns(dataframe: DataFrame) -> DataFrame:
    """ Removes the unused columns from the flight plan intention dataframe.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the columns removed.
    """
    return dataframe.drop(*COLUMNS_TO_DROP)


def reorder_fp_int_columns(dataframe: DataFrame) -> DataFrame:
    """ Reorder the columns of the flight plan intention dataframe.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the columns reordered.
    """
    return dataframe.select(FP_INTENTION_COLUMNS)


def add_fp_int_counter(dataframe: DataFrame) -> DataFrame:
    """ Add a log counter column to the CONFLOG dataframe.

    :param dataframe: dataframe with the CONFLOG data read from the file.
    :return: dataframe with the column loss duration added.
    """
    return add_dataframe_counter(dataframe, FLST_ID)


def transform_timestamps_to_seconds(dataframe: DataFrame) -> DataFrame:
    """ Transform the timestamp in format HH:MM:SS to seconds.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the column loitering added.
    """
    dataframe = dataframe.withColumn(RECEPTION_TIME, (hour(col(RECEPTION_TIME)) * 3600 +
                                                      minute(col(RECEPTION_TIME)) * 60 +
                                                      second(col(RECEPTION_TIME))))
    dataframe = dataframe.withColumn(DEPARTURE_TIME, (hour(col(DEPARTURE_TIME)) * 3600 +
                                                      minute(col(DEPARTURE_TIME)) * 60 +
                                                      second(col(DEPARTURE_TIME))))
    return dataframe


def check_if_loitering_mission(dataframe: DataFrame) -> DataFrame:
    """ Checks if the flight plan is a loitering mission.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the column loitering added.
    """
    return dataframe.withColumn(LOITERING, when(col(GEOFENCE_DURATION).isNotNull(), True).otherwise(False))


def calculate_cruising_speed(dataframe: DataFrame) -> DataFrame:
    """ Adds the cruising to the dataframe depending on the drone type.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the column cruising speed added.
    """
    return dataframe.withColumn(CRUISING_SPEED, get_drone_speed(col(VEHICLE)))


def parse_locations(dataframe: DataFrame) -> DataFrame:
    """ Parses the initial and destination locations that are given
    in a tuple in the csv file.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: processed location with the columns ORIGIN_LAT, ORIGIN_LON, DESTINATION_LAT and DESTINATION_LON.
    """
    # First remove the parenthesis at the beginning and the end, split by the comma and get the items
    dataframe = dataframe.withColumn(INITIAL_LOCATION, regexp_replace(col(INITIAL_LOCATION), PARENTHESIS_PATTERN, ""))
    dataframe = dataframe.withColumn(INITIAL_LOCATION, split(col(INITIAL_LOCATION), COMMA_PATTERN))
    dataframe = dataframe.withColumn(ORIGIN_LAT, col(INITIAL_LOCATION).getItem(0))
    dataframe = dataframe.withColumn(ORIGIN_LON, col(INITIAL_LOCATION).getItem(1))

    dataframe = dataframe.withColumn(FINAL_LOCATION, regexp_replace(col(FINAL_LOCATION), PARENTHESIS_PATTERN, ""))
    dataframe = dataframe.withColumn(FINAL_LOCATION, split(col(FINAL_LOCATION), COMMA_PATTERN))
    dataframe = dataframe.withColumn(DESTINATION_LAT, col(FINAL_LOCATION).getItem(0))
    dataframe = dataframe.withColumn(DESTINATION_LON, col(FINAL_LOCATION).getItem(1))

    return dataframe


def calculate_destination_position(dataframe: DataFrame) -> DataFrame:
    """ Calculates the destination position in the axis X and Y.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the columns with the destination position added.
    """
    # First we parse the location and transform the CRS with `transform_location`()`
    dataframe = dataframe.withColumn('FINAL_POS', transform_location(col(DESTINATION_LAT), col(DESTINATION_LON)))
    # Recover the calculated columns and move to the first level of the table.
    dataframe = dataframe.withColumn(DESTINATION_X, col(f'FINAL_POS.{LATITUDE}'))
    dataframe = dataframe.withColumn(DESTINATION_Y, col(f'FINAL_POS.{LONGITUDE}'))
    # Remove intermediate column
    dataframe = dataframe.drop('FINAL_POS')
    return dataframe


def calculate_baseline_metrics(dataframe: DataFrame) -> DataFrame:
    """

    :param dataframe:
    :return:
    """
    dataframe = dataframe.withColumn(BASELINE_2D_DISTANCE, lit(0).cast(DoubleType()))
    dataframe = dataframe.withColumn(BASELINE_VERTICAL_DISTANCE, lit(0).cast(DoubleType()))
    dataframe = dataframe.withColumn(BASELINE_ASCENDING_DISTANCE, lit(0).cast(DoubleType()))
    dataframe = dataframe.withColumn(BASELINE_3D_DISTANCE, lit(0).cast(DoubleType()))
    dataframe = dataframe.withColumn(BASELINE_FLIGHT_TIME, lit(0).cast(DoubleType()))
    dataframe = dataframe.withColumn(BASELINE_DEPARTURE_TIME, lit(0).cast(DoubleType()))
    dataframe = dataframe.withColumn(BASELINE_ARRIVAL_TIME, lit(0).cast(DoubleType()))
    return dataframe


FP_INTENTION_TRANSFORMATIONS = [add_fp_int_counter, transform_timestamps_to_seconds, check_if_loitering_mission,
                                calculate_cruising_speed, parse_locations, calculate_destination_position,
                                calculate_baseline_metrics, remove_fp_int_unused_columns, reorder_fp_int_columns]


def parse_fp_int(spark: SparkSession, fp_int_name: str) -> DataFrame:
    """ Parses and process the flight plan intention of the file with the given name.

    :param spark: spark session.
    :param fp_int_name: path to the FLST LOG.
    :return: parsed and processed FLST LOG.
    """
    fp_int_path = Path(settings.flight_intention_path, f'{fp_int_name}.csv')
    fp_int_dataframe = spark.read.csv(str(fp_int_path), header=False, schema=FP_INTENTION_FILE_SCHEMA)

    for transformation in FP_INTENTION_TRANSFORMATIONS:
        logger.trace('Applying data transformation: {}.', transformation)
        fp_int_dataframe = transformation(fp_int_dataframe)

    return fp_int_dataframe
