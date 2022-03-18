from pyspark.sql import DataFrame

from parse.parser_utils import add_dataframe_counter
from schemas.conf_log_schema import COLUMNS_TO_DROP, CONF_LOG_COLUMNS
from schemas.tables_attributes import CONF_ID


def add_conf_log_counter(dataframe: DataFrame) -> DataFrame:
    """ Add a log counter column to the CONFLOG dataframe.

    :param dataframe: dataframe with the CONFLOG data read from the file.
    :return: dataframe with the column loss duration added.
    """
    return add_dataframe_counter(dataframe, CONF_ID)


def remove_conf_log_unused_columns(dataframe: DataFrame) -> DataFrame:
    """ Removes the unused columns from the CONFLOG dataframe.

    :param dataframe: dataframe with the CONFLOG data read from the file.
    :return: dataframe with the columns removed.
    """
    return dataframe.drop(*COLUMNS_TO_DROP)


def reorder_conf_log_columns(dataframe: DataFrame) -> DataFrame:
    """ Reorder the columns of the CONFLOG dataframe.

    :param dataframe: dataframe with the CONFLOG data read from the file.
    :return: dataframe with the columns reordered.
    """
    return dataframe.select(CONF_LOG_COLUMNS)


CONF_LOG_TRANSFORMATIONS = [add_conf_log_counter, remove_conf_log_unused_columns, reorder_conf_log_columns]
