from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

from config import settings
from schemas.tables_attributes import DEL_Y, DEL_X, DESTINATION_Y, DESTINATION_X, ARRIVAL_DELAY, DEPARTURE_DELAY, \
    SPAWNED, MISSION_COMPLETED, DEL_TIME, BASELINE_ARRIVAL_TIME, SPAWN_TIME, BASELINE_DEPARTURE_TIME

COLUMNS_TO_DROP = [DESTINATION_X, DESTINATION_Y, DEL_X, DEL_Y]


def remove_combined_unused_columns(dataframe: DataFrame) -> DataFrame:
    """ Removes the unused columns from the FLSTLOG data and flight intentions dataframe.

    :param dataframe: dataframe with the FLSTLOG data and flight intentions read from the files.
    :return: dataframe with the columns removed.
    """
    return dataframe.drop(*COLUMNS_TO_DROP)


def calculate_arrival_delay(dataframe: DataFrame) -> DataFrame:
    """ Calculates the delay in the arrival with respect to the baseline.

    :param dataframe: dataframe with the FLSTLOG data and flight intentions read from the files.
    :return: dataframe with the delay column added.
    """
    return dataframe.withColumn(ARRIVAL_DELAY, col(DEL_TIME) - col(BASELINE_ARRIVAL_TIME))


def calculate_departure_delay(dataframe: DataFrame) -> DataFrame:
    """ Calculates the delay in the takeoff with respect to the baseline.

    :param dataframe: dataframe with the FLSTLOG data and flight intentions read from the files.
    :return: dataframe with the delay column added.
    """
    return dataframe.withColumn(DEPARTURE_DELAY, col(SPAWN_TIME) - col(BASELINE_DEPARTURE_TIME))


def was_spawned(dataframe: DataFrame) -> DataFrame:
    """ Checks if the drone was really spawned during the simulation.

    :param dataframe: dataframe with the FLSTLOG data and flight intentions read from the files.
    :return: dataframe with the check of the takeoff column added.
    """
    return dataframe.withColumn(SPAWNED, when(col(SPAWN_TIME).isNull(), False).otherwise(True))


def was_mission_completed(dataframe: DataFrame) -> DataFrame:
    """ Checks if the drone reached the desired destination during the simulation.

    :param dataframe: dataframe with the FLSTLOG data and flight intentions read from the files.
    :return: dataframe with the check if the destination was reached column added.
    """
    return dataframe.withColumn(MISSION_COMPLETED, when(
        ((col(DESTINATION_X) - col(DEL_X)) * (col(DESTINATION_X) - col(DEL_X))) +
        ((col(DESTINATION_Y) - col(DEL_Y)) * (col(DESTINATION_Y) - col(DEL_Y))) >
        settings.thresholds.destination_distance, False).otherwise(True))


COMBINED_FLST_FP_INT_TRANSFORMATIONS = [calculate_arrival_delay, calculate_departure_delay, was_spawned,
                                        was_mission_completed, remove_combined_unused_columns]
