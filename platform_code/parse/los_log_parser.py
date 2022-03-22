from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, abs

from parse.parser_utils import add_dataframe_counter
from schemas.los_log_schema import LOS_LOG_COLUMNS
from schemas.tables_attributes import (CRASH, DISTANCE, ALTITUDE, LOS_DURATION_TIME,
                                       LOS_EXIT_TIME, LOS_START_TIME, LOS_ID)


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


def generate_crash_column(los_log_dataframe):
    """ Add the crash column to the LOSLOG dataframe.

    :param los_log_dataframe: dataframe with the LOSLOG data read from the file.
    :return: dataframe with the column crash added.
    """
    return los_log_dataframe.withColumn(
        CRASH,
        when((col(DISTANCE) <= 1.7) &
             (abs(col(f'{ALTITUDE}_1') - col(f'{ALTITUDE}_2')) < 2.46), True).otherwise(False)
    )


def reorder_loss_log_columns(dataframe: DataFrame) -> DataFrame:
    """ Reorder the columns of the LOSLOG dataframe.

    :param dataframe: dataframe with the LOSLOG data read from the file.
    :return: dataframe with the columns reordered.
    """
    return dataframe.select(LOS_LOG_COLUMNS)


LOS_LOG_TRANSFORMATIONS = [generate_loss_duration_column, generate_crash_column,
                           add_loss_log_counter, reorder_loss_log_columns]
