from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, hour, minute, second, split, regexp_replace

from schemas.fp_int_schema import COLUMNS_TO_DROP, FP_INT_COLUMNS
from schemas.tables_attributes import (BASELINE_VERTICAL_DISTANCE, BASELINE_ASCENDING_DISTANCE, BASELINE_3D_DISTANCE,
                                       BASELINE_FLIGHT_TIME, BASELINE_ARRIVAL_TIME, BASELINE_2D_DISTANCE, DESTINATION_X,
                                       DESTINATION_Y, LOITERING, FLST_ID, CRUISING_SPEED, ORIGIN_LON, ORIGIN_LAT,
                                       BASELINE_DEPARTURE_TIME, GEOFENCE_DURATION, DEPARTURE_TIME,
                                       INITIAL_LOCATION, FINAL_LOCATION, DESTINATION_LAT,
                                       DESTINATION_LON, LATITUDE, LONGITUDE, VERTICAL_SPEED, VEHICLE)
from utils.config import settings
from utils.parser_utils import add_dataframe_counter, transform_location, get_coordinates_distance, get_drone_speed

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
    return dataframe.select(FP_INT_COLUMNS)


def add_fp_int_counter(dataframe: DataFrame) -> DataFrame:
    """ Add a log counter column to the CONFLOG dataframe.

    :param dataframe: dataframe with the CONFLOG data read from the file.
    :return: dataframe with the column loss duration added.
    """
    return add_dataframe_counter(dataframe, FLST_ID)


def check_if_loitering_mission(dataframe: DataFrame) -> DataFrame:
    """ Checks if the flight plan is a loitering mission.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the column loitering added.
    """
    return dataframe.withColumn(LOITERING,
                                when(col(GEOFENCE_DURATION).isNotNull(), True)
                                .otherwise(False))


def calculate_speeds(dataframe: DataFrame) -> DataFrame:
    """ Adds the cruising to the dataframe depending on the drone type.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the column cruising speed added.
    """
    # First we retrieve the speeds in one call, to avoid two context switches
    dataframe = dataframe.withColumn('SPEEDS', get_drone_speed(col(VEHICLE)))
    # Recover the calculated columns and move to the first level of the table.
    dataframe = dataframe.withColumn(CRUISING_SPEED, col(f'SPEEDS.{CRUISING_SPEED}'))
    dataframe = dataframe.withColumn(VERTICAL_SPEED, col(f'SPEEDS.{VERTICAL_SPEED}'))
    # Remove intermediate column
    dataframe = dataframe.drop('SPEEDS')
    return dataframe


def parse_locations(dataframe: DataFrame) -> DataFrame:
    """ Parses the initial and destination locations that are given
    in a tuple in the csv file.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: processed location with the columns ORIGIN_LAT, ORIGIN_LON, DESTINATION_LAT and DESTINATION_LON.
    """
    # First remove the parenthesis at the beginning and the end, split by the comma and get the items
    dataframe = dataframe.withColumn(INITIAL_LOCATION, regexp_replace(col(INITIAL_LOCATION), PARENTHESIS_PATTERN, ""))
    dataframe = dataframe.withColumn(INITIAL_LOCATION, split(col(INITIAL_LOCATION), COMMA_PATTERN))
    dataframe = dataframe.withColumn(ORIGIN_LAT, col(INITIAL_LOCATION).getItem(1))
    dataframe = dataframe.withColumn(ORIGIN_LON, col(INITIAL_LOCATION).getItem(0))

    dataframe = dataframe.withColumn(FINAL_LOCATION, regexp_replace(col(FINAL_LOCATION), PARENTHESIS_PATTERN, ""))
    dataframe = dataframe.withColumn(FINAL_LOCATION, split(col(FINAL_LOCATION), COMMA_PATTERN))
    dataframe = dataframe.withColumn(DESTINATION_LAT, col(FINAL_LOCATION).getItem(1))
    dataframe = dataframe.withColumn(DESTINATION_LON, col(FINAL_LOCATION).getItem(0))

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


def transform_timestamps_to_seconds(dataframe: DataFrame) -> DataFrame:
    """ Transform the timestamp in format HH:MM:SS to seconds for the departure
    time and set it as the baseline departure time.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the column loitering added.
    """
    # For the optimal time, leave as soon it is set in the flight plan intention in the departure time
    dataframe = dataframe.withColumn(BASELINE_DEPARTURE_TIME, (hour(col(DEPARTURE_TIME)) * 3600 +
                                                               minute(col(DEPARTURE_TIME)) * 60 +
                                                               second(col(DEPARTURE_TIME))))
    return dataframe


def calculate_baseline_metrics(dataframe: DataFrame) -> DataFrame:
    """ Calculates the ideal metrics for the flight intentions.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the baseline metrics calculated.
    """
    dataframe = dataframe.withColumn(BASELINE_2D_DISTANCE,
                                     get_coordinates_distance(col(ORIGIN_LAT), col(ORIGIN_LON),
                                                              col(DESTINATION_LAT), col(DESTINATION_LON)))

    # TODO: Ask for the loitering flight altitude
    # The flight time is the same for both types, however the loitering mission is removed in the air.
    # Therefore, the vertical distance = ascending distance for them
    dataframe = dataframe.withColumn(BASELINE_ASCENDING_DISTANCE,
                                     when(col(LOITERING), settings.flight_altitude.loitering)
                                     .otherwise(settings.flight_altitude.lowest))
    dataframe = dataframe.withColumn(BASELINE_VERTICAL_DISTANCE,
                                     when(col(LOITERING), col(BASELINE_ASCENDING_DISTANCE))
                                     .otherwise(col(BASELINE_ASCENDING_DISTANCE) * 2))
    dataframe = dataframe.withColumn(BASELINE_FLIGHT_TIME,
                                     col(BASELINE_2D_DISTANCE) / col(CRUISING_SPEED) +
                                     col(BASELINE_ASCENDING_DISTANCE) / col(VERTICAL_SPEED))

    dataframe = dataframe.withColumn(BASELINE_3D_DISTANCE,
                                     col(BASELINE_2D_DISTANCE) + col(BASELINE_VERTICAL_DISTANCE))
    # The BASELINE_DEPARTURE_TIME is calculated in `transform_timestamps_to_seconds()`
    dataframe = dataframe.withColumn(BASELINE_ARRIVAL_TIME,
                                     col(BASELINE_DEPARTURE_TIME) + col(BASELINE_FLIGHT_TIME))
    return dataframe


FP_INT_TRANSFORMATIONS = [add_fp_int_counter, check_if_loitering_mission, calculate_speeds, parse_locations,
                          calculate_destination_position, transform_timestamps_to_seconds,
                          calculate_baseline_metrics, remove_fp_int_unused_columns, reorder_fp_int_columns]
