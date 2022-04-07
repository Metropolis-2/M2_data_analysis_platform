from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, abs

from schemas.los_log_schema import LOS_LOG_COLUMNS
from schemas.tables_attributes import (CRASH, DISTANCE, ALTITUDE, LOS_DURATION_TIME,
                                       LOS_EXIT_TIME, LOS_START_TIME, LOS_ID)
from utils.config import settings
from utils.parser_utils import add_dataframe_counter, convert_feet_to_meters


def add_loss_log_counter(los_log_dataframe: DataFrame):
    """ Add a log counter column to the LOSLOG dataframe.

    :param los_log_dataframe: dataframe with the LOSLOG data read from the file.
    :return: dataframe with the column loss duration added.
    """
    return add_dataframe_counter(los_log_dataframe, LOS_ID)


def generate_loss_duration_column(los_log_dataframe):
    """ Add the loss duration column to the LOSLOG dataframe.

    :param los_log_dataframe: dataframe with the LOSLOG data read from the file.
    :return: dataframe with the column loss duration added.
    """
    return los_log_dataframe.withColumn(LOS_DURATION_TIME, col(LOS_EXIT_TIME) - col(LOS_START_TIME))


def convert_altitudes_to_meter(dataframe: DataFrame) -> DataFrame:
    """ Converts all the altitudes fields from feet to meters.

    :param dataframe: dataframe with the LOS log data read from the file.
    :return: dataframe with the columns in feet transformed to meter.
    """
    # The altitude distance comprises both up and down movements
    dataframe = convert_feet_to_meters(dataframe, f'{ALTITUDE}_1')
    dataframe = convert_feet_to_meters(dataframe, f'{ALTITUDE}_2')
    return dataframe


def generate_crash_column(los_log_dataframe):
    """ Add the crash column to the LOSLOG dataframe.

    :param los_log_dataframe: dataframe with the LOSLOG data read from the file.
    :return: dataframe with the column crash added.
    """
    return los_log_dataframe \
        .withColumn(CRASH,
                    when((col(DISTANCE) <= settings.thresholds.crash_horizontal_distance) &
                         (abs(col(f'{ALTITUDE}_1') - col(f'{ALTITUDE}_2'))
                          < settings.thresholds.crash_vertical_distance), True)
                    .otherwise(False))


def reorder_loss_log_columns(dataframe: DataFrame) -> DataFrame:
    """ Reorder the columns of the LOSLOG dataframe.

    :param dataframe: dataframe with the LOSLOG data read from the file.
    :return: dataframe with the columns reordered.
    """
    return dataframe.select(LOS_LOG_COLUMNS)


LOS_LOG_TRANSFORMATIONS = [generate_loss_duration_column, generate_crash_column,
                           add_loss_log_counter, convert_altitudes_to_meter, reorder_loss_log_columns]
