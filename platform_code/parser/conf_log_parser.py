from pyspark.sql import DataFrame

from schemas.conf_log_schema import CONF_LOG_COLUMNS
from schemas.tables_attributes import CONF_ID
from utils.parser_utils import add_dataframe_counter


def add_conf_log_counter(dataframe: DataFrame) -> DataFrame:
    """ Add a log counter column to the CONFLOG dataframe.

    :param dataframe: dataframe with the CONFLOG data read from the file.
    :return: dataframe with the column loss duration added.
    """
    return add_dataframe_counter(dataframe, CONF_ID)


def reorder_conf_log_columns(dataframe: DataFrame) -> DataFrame:
    """ Reorder the columns of the CONFLOG dataframe.

    :param dataframe: dataframe with the CONFLOG data read from the file.
    :return: dataframe with the columns reordered.
    """
    return dataframe.select(CONF_LOG_COLUMNS)


CONF_LOG_TRANSFORMATIONS = [add_conf_log_counter, reorder_conf_log_columns]
