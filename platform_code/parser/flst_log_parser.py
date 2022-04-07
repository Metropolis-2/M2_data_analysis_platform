from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from schemas.flst_log_schema import COLUMNS_TO_DROP, FLST_LOG_COLUMNS
from schemas.tables_attributes import (DEL_Y, DEL_X, DEL_LATITUDE, DEL_LONGITUDE,
                                       LATITUDE, LONGITUDE, VERTICAL_DISTANCE, DEL_ALTITUDE)
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


FLST_LOG_TRANSFORMATIONS = [remove_flst_log_unused_columns, convert_altitudes_to_meter,
                            calculate_deletion_position, reorder_flst_log_columns]
