from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col

from schemas.conf_log_schema import CONF_LOG_COLUMNS
from schemas.tables_attributes import CONF_ID, SIMT_VALID, CONF_DETECTED_TIME
from utils.config import settings
from utils.parser_utils import add_dataframe_counter


def add_conf_log_counter(dataframe: DataFrame) -> DataFrame:
    """ Add a log counter column to the CONFLOG dataframe.

    :param dataframe: dataframe with the CONFLOG data read from the file.
    :return: dataframe with the column loss duration added.
    """
    return add_dataframe_counter(dataframe, CONF_ID)

def add_simt_flag(dataframe: DataFrame) -> DataFrame:
    """ Add a simulation time flag column to the CONFLOG dataframe.

    :param dataframe: dataframe with the CONFLOG data read from the file.
    :return: dataframe with the column SIMT_VALID added.
    """
    return dataframe \
        .withColumn(SIMT_VALID,
                    when(col(CONF_DETECTED_TIME) > settings.simulation.max_time, False)
                    .otherwise(True))


def reorder_conf_log_columns(dataframe: DataFrame) -> DataFrame:
    """ Reorder the columns of the CONFLOG dataframe.

    :param dataframe: dataframe with the CONFLOG data read from the file.
    :return: dataframe with the columns reordered.
    """
    return dataframe.select(CONF_LOG_COLUMNS)


CONF_LOG_TRANSFORMATIONS = [add_conf_log_counter, add_simt_flag, reorder_conf_log_columns]
