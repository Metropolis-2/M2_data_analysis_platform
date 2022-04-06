from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col

from schemas.geo_log_schema import GEO_LOG_COLUMNS, COLUMNS_TO_DROP
from schemas.tables_attributes import (VIOLATION_SEVERITY, MAX_INTRUSION, GEOFENCE_NAME, OPEN_AIRSPACE, GEO_ID,
                                       LOITERING_NFZ)
from utils.config import settings
from utils.parser_utils import add_dataframe_counter


def add_geo_log_counter(dataframe: DataFrame) -> DataFrame:
    """ Add a log counter column to the GEOLOG dataframe.

    :param dataframe: dataframe with the GEOLOG data read from the file.
    :return: dataframe with the column loss duration added.
    """
    return add_dataframe_counter(dataframe, GEO_ID)


def remove_geo_log_unused_columns(dataframe: DataFrame) -> DataFrame:
    """ Removes the unused columns from the GEOLOG dataframe.

    :param dataframe: dataframe with the GEOLOG data read from the file.
    :return: dataframe with the columns removed.
    """
    return dataframe.drop(*COLUMNS_TO_DROP)


def check_intrusion_severity(dataframe: DataFrame) -> DataFrame:
    """ Check if the intrusion is severe. Currently, if the intrusion
    is lower than 1 meter is not considered severe.

    :param dataframe: dataframe with the GEOLOG data read from the file.
    :return: dataframe with the columns indicating if the intrusion is severe.
    """
    return dataframe \
        .withColumn(VIOLATION_SEVERITY,
                    when(col(MAX_INTRUSION) < settings.thresholds.intrusion_distance, False)
                    .otherwise(True))


def check_geofence_location(dataframe: DataFrame) -> DataFrame:
    """ Check if the geofence is in open airspace. Currently, If the geofence
    name starts with g it is in open airspace.

    :param dataframe: dataframe with the GEOLOG data read from the file.
    :return: dataframe with the columns indicating if the geofence is in the open space.
    """
    dataframe = dataframe.withColumn(OPEN_AIRSPACE,
                                     when(col(GEOFENCE_NAME).startswith('G'), True).otherwise(False))
    dataframe = dataframe.withColumn(LOITERING_NFZ,
                                     when(col(GEOFENCE_NAME).startswith('L'), True).otherwise(False))
    return dataframe


def reorder_geo_log_columns(dataframe: DataFrame) -> DataFrame:
    """ Reorder the columns of the GEOLOG dataframe.

    :param dataframe: dataframe with the GEOLOG data read from the file.
    :return: dataframe with the columns reordered.
    """
    return dataframe.select(GEO_LOG_COLUMNS)


GEO_LOG_TRANSFORMATIONS = [add_geo_log_counter, check_geofence_location, check_intrusion_severity,
                           remove_geo_log_unused_columns, reorder_geo_log_columns]
