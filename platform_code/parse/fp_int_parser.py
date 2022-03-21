from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import DoubleType

from config import settings
from parse.parser_utils import add_dataframe_counter
from schemas.fp_int_schema import COLUMNS_TO_DROP, FP_INTENTION_COLUMNS, FP_INTENTION_FILE_SCHEMA
from schemas.tables_attributes import (BASELINE_VERTICAL_DISTANCE, BASELINE_ASCENDING_DISTANCE, BASELINE_3D_DISTANCE,
                                       BASELINE_FLIGHT_TIME, BASELINE_ARRIVAL_TIME, BASELINE_2D_DISTANCE, DESTINATION_X,
                                       DESTINATION_Y, LOITERING, FLST_ID, CRUISING_SPEED, DESTINATION_LON,
                                       DESTINATION_LAT, ORIGIN_LON, ORIGIN_LAT, BASELINE_DEPARTURE_TIME)


def add_fp_int_counter(dataframe: DataFrame) -> DataFrame:
    """ Add a log counter column to the CONFLOG dataframe.

    :param dataframe: dataframe with the CONFLOG data read from the file.
    :return: dataframe with the column loss duration added.
    """
    return add_dataframe_counter(dataframe, FLST_ID)


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


def parse_locations(dataframe: DataFrame) -> DataFrame:
    """ Parses the initial and destination locations that are given
    in a tuple in the csv file.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return:
    """
    dataframe = dataframe.withColumn(ORIGIN_LAT, lit(0).cast(DoubleType()))
    dataframe = dataframe.withColumn(ORIGIN_LON, lit(0).cast(DoubleType()))
    dataframe = dataframe.withColumn(DESTINATION_LAT, lit(0).cast(DoubleType()))
    dataframe = dataframe.withColumn(DESTINATION_LON, lit(0).cast(DoubleType()))
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


# TODO:
def check_if_loitering_mission(dataframe: DataFrame) -> DataFrame:
    """ Checks if the flight plan is a loitering mission.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the column loitering added.
    """
    return dataframe.withColumn(LOITERING, lit(0).cast(DoubleType()))


# TODO:
def calculate_cruising_speed(dataframe: DataFrame) -> DataFrame:
    """ Adds the cruising to the dataframe depending on the drone type.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the column cruising speed added.
    """
    return dataframe.withColumn(CRUISING_SPEED, lit(0).cast(DoubleType()))


# TODO: Implement
def calculate_destination_position(dataframe: DataFrame) -> DataFrame:
    """ Calculates the destination position in the axis X and Y.

    :param dataframe: dataframe with the flight plan intention data read from the file.
    :return: dataframe with the columns with the destination position added.
    """
    # transformer = Transformer.from_crs('epsg:4326', 'epsg:32633')
    # p = transformer.transform(dest_lat, dest_lon)
    # dest_x = p[0]
    # dest_y = p[1]
    dataframe = dataframe.withColumn(DESTINATION_X, lit(0).cast(DoubleType()))
    return dataframe.withColumn(DESTINATION_Y, lit(0).cast(DoubleType()))


FP_INTENTION_TRANSFORMATIONS = [add_fp_int_counter, parse_locations, calculate_baseline_metrics,
                                check_if_loitering_mission, calculate_cruising_speed, calculate_destination_position,
                                remove_fp_int_unused_columns, reorder_fp_int_columns]


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
