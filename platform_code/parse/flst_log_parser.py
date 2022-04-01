from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from schemas.flst_log_schema import COLUMNS_TO_DROP, FLST_LOG_COLUMNS
from schemas.tables_attributes import (ASCENDING_DISTANCE, WORK_DONE, DEL_Y, DEL_X, DEL_LATITUDE, DEL_LONGITUDE,
                                       LATITUDE, LONGITUDE, VERTICAL_DISTANCE, DEL_ALTITUDE, FLIGHT_TIME)
from utils.parser_utils import transform_location, convert_feet_to_meters


def remove_flst_log_unused_columns(dataframe: DataFrame) -> DataFrame:
    """ Removes the unused columns from the FLSTLOG dataframe.

    :param dataframe: dataframe with the FLSTLOG data read from the file.
    :return: dataframe with the columns removed.
    """
    return dataframe.drop(*COLUMNS_TO_DROP)


def reorder_flst_log_columns(dataframe: DataFrame) -> DataFrame:
    """ Reorder the columns of the FLSTLOG dataframe.

    :param dataframe: dataframe with the FLSTLOG data read from the file.
    :return: dataframe with the columns reordered.
    """
    return dataframe.select(FLST_LOG_COLUMNS)


def convert_altitudes_to_meter(dataframe: DataFrame) -> DataFrame:
    """ Converts all the altitudes fields from feet to meters.

    :param dataframe: dataframe with the FLSTLOG data read from the file.
    :return: dataframe with the columns in feet transformed to meter.
    """
    # The altitude distance comprises both up and down movements
    dataframe = convert_feet_to_meters(dataframe, VERTICAL_DISTANCE)
    dataframe = convert_feet_to_meters(dataframe, DEL_ALTITUDE)
    return dataframe


def calculate_ascending_distance(dataframe: DataFrame) -> DataFrame:
    """ Calculates the ascending distance navigated by the drone.
    This distance is calculated by halving the altitude distance, to take
    only the up movements, and subtracting the deletion altitude.

    :param dataframe: dataframe with the FLSTLOG data read from the file.
    :return: dataframe with the column ascending distance added.
    """
    return dataframe.withColumn(ASCENDING_DISTANCE,
                                col(VERTICAL_DISTANCE) / 2 - col(DEL_ALTITUDE))


def calculate_work_done(dataframe: DataFrame) -> DataFrame:
    """ Calculates the energy employed during the flight.

    :param dataframe: dataframe with the FLSTLOG data read from the file.
    :return: dataframe with the column work done added.
    """
    # For the moment the formula is work_done = 2 * ascending_distance + flight_time
    return dataframe.withColumn(WORK_DONE,
                                2 * col(ASCENDING_DISTANCE) + col(FLIGHT_TIME))


def calculate_deletion_position(dataframe: DataFrame) -> DataFrame:
    """ Calculates the position where the drone was deleted in the axis X and Y.
    It performs the transformation from the coordinates to X and Y units.

    :param dataframe: dataframe with the FLSTLOG data read from the file.
    :return: dataframe with the columns for the deletion of the drone in the X and Y added.
    """
    # First we parse the location and transform the CRS with `transform_location`()`
    dataframe = dataframe.withColumn('DEL_POS', transform_location(col(DEL_LATITUDE), col(DEL_LONGITUDE)))
    # Recover the calculated columns and move to the first level of the table.
    dataframe = dataframe.withColumn(DEL_X, col(f'DEL_POS.{LATITUDE}'))
    dataframe = dataframe.withColumn(DEL_Y, col(f'DEL_POS.{LONGITUDE}'))
    # Remove intermediate column
    dataframe = dataframe.drop('DEL_POS')
    return dataframe


FLST_LOG_TRANSFORMATIONS = [remove_flst_log_unused_columns, convert_altitudes_to_meter, calculate_ascending_distance,
                            calculate_deletion_position, calculate_work_done, reorder_flst_log_columns]
