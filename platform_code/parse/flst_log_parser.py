from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import DoubleType

from schemas.flst_log_schema import COLUMNS_TO_DROP, FLST_LOG_COLUMNS, FLST_LOG_FILE_SCHEMA
from schemas.tables_attributes import (ASCEND_DIST, WORK_DONE, DEL_Y, DEL_X, SCENARIO_NAME, DEL_LATITUDE, DEL_LONGITUDE,
                                       LATITUDE, LONGITUDE)
from utils.parser_utils import build_scenario_name, remove_commented_log_lines, transform_location


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


# TODO : create a function to compute that with inputs ALT_dist=float(line_list[6]) and DEL_ALT=float(line_list[10])
def calculate_ascending_distance(dataframe: DataFrame) -> DataFrame:
    """ Calculates the ascending distance navigated by the drone.

    :param dataframe: dataframe with the FLSTLOG data read from the file.
    :return: dataframe with the column ascending distance added.
    """
    return dataframe.withColumn(ASCEND_DIST, lit(0).cast(DoubleType()))


# TODO : create a function to compute that with inputs ascend_dist and FLIGHT_time=float(line_list[3])
def calculate_work_done(dataframe: DataFrame) -> DataFrame:
    """ Calculates the energy employed during the flight.

    :param dataframe: dataframe with the FLSTLOG data read from the file.
    :return: dataframe with the column work done added.
    """
    return dataframe.withColumn(WORK_DONE, lit(0).cast(DoubleType()))


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


FLST_LOG_TRANSFORMATIONS = [remove_flst_log_unused_columns, calculate_ascending_distance, calculate_work_done,
                            calculate_deletion_position, reorder_flst_log_columns]


def parse_flst_log(spark: SparkSession, log_path: Path) -> DataFrame:
    """ Parses and process the FLSTLOG of the given file.

    :param spark: spark session.
    :param log_path: path to the FLST LOG.
    :return: parsed and processed FLST LOG.
    """
    # First load the FLSTLOG
    scenario_name = build_scenario_name(log_path)

    flst_log_dataframe = spark.read.csv(str(log_path), header=False, schema=FLST_LOG_FILE_SCHEMA)
    flst_log_dataframe = remove_commented_log_lines(flst_log_dataframe)
    flst_log_dataframe = flst_log_dataframe.withColumn(SCENARIO_NAME, lit(scenario_name))

    for transformation in FLST_LOG_TRANSFORMATIONS:
        logger.trace('Applying data transformation: {}.', transformation)
        flst_log_dataframe = transformation(flst_log_dataframe)

    return flst_log_dataframe
