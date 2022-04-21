from typing import Dict

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

from schemas.tables_attributes import (DEL_Y, DEL_X, DESTINATION_Y, DESTINATION_X, ARRIVAL_DELAY, DEPARTURE_DELAY,
                                       SPAWNED, MISSION_COMPLETED, DEL_TIME, BASELINE_ARRIVAL_TIME, SPAWN_TIME,
                                       BASELINE_DEPARTURE_TIME, ACID, FLIGHT_TIME, DISTANCE_2D, DISTANCE_3D,
                                       VERTICAL_DISTANCE, DEL_LATITUDE, DEL_LONGITUDE, DEL_ALTITUDE, ASCENDING_DISTANCE,
                                       WORK_DONE, FLST_ID, SCENARIO_NAME, ORIGIN_LAT, ORIGIN_LON, DESTINATION_LAT,
                                       DESTINATION_LON, CRUISING_SPEED, PRIORITY, LOITERING, BASELINE_2D_DISTANCE,
                                       BASELINE_VERTICAL_DISTANCE, BASELINE_ASCENDING_DISTANCE, BASELINE_3D_DISTANCE,
                                       BASELINE_FLIGHT_TIME, VERTICAL_SPEED, VEHICLE, PRIORITY_WEIGHT)
from utils.config import settings
from utils.parser_utils import get_fp_int_key_from_scenario_name

COLUMNS_TO_DROP = [DESTINATION_X, DESTINATION_Y, DEL_X, DEL_Y]
COMBINED_COLUMNS = [
    FLST_ID,
    SCENARIO_NAME,
    ACID,
    VEHICLE,
    ORIGIN_LAT,
    ORIGIN_LON,
    DESTINATION_LAT,
    DESTINATION_LON,
    BASELINE_DEPARTURE_TIME,
    CRUISING_SPEED,
    VERTICAL_SPEED,
    PRIORITY,
    PRIORITY_WEIGHT,
    LOITERING,
    BASELINE_2D_DISTANCE,
    BASELINE_VERTICAL_DISTANCE,
    BASELINE_ASCENDING_DISTANCE,
    BASELINE_3D_DISTANCE,
    BASELINE_FLIGHT_TIME,
    BASELINE_ARRIVAL_TIME,
    SPAWNED,
    SPAWN_TIME,
    DEL_TIME,
    MISSION_COMPLETED,
    FLIGHT_TIME,
    DISTANCE_2D,
    DISTANCE_3D,
    VERTICAL_DISTANCE,
    ASCENDING_DISTANCE,
    ARRIVAL_DELAY,
    DEPARTURE_DELAY,
    WORK_DONE,
    DEL_LATITUDE,
    DEL_LONGITUDE,
    DEL_ALTITUDE
]


def remove_combined_unused_columns(dataframe: DataFrame) -> DataFrame:
    """ Removes the unused columns from the FLSTLOG data and flight intentions dataframe.

    :param dataframe: dataframe with the FLSTLOG data and flight intentions read from the files.
    :return: dataframe with the columns removed.
    """
    return dataframe.drop(*COLUMNS_TO_DROP)


def reorder_combined_columns(dataframe: DataFrame) -> DataFrame:
    """ Reorder the columns of the combined FLST log and flight intentions dataframe.

    :param dataframe: dataframe with the combined FLST log and flight intentions dataframe.
    :return: dataframe with the columns reordered.
    """
    return dataframe.select(COMBINED_COLUMNS)


def calculate_ascending_distance(dataframe: DataFrame) -> DataFrame:
    """ Calculates the ascending distance navigated by the drone.

    For most of the flights, this distance is calculated by halving the
    vertical distance, to take only the up movements, and subtracting the
    deletion altitude.

    However, for the loitering missions, which are removed in the air. The
    ascending distance is the same as the vertical distance.

    :param dataframe: dataframe with the FLSTLOG and fli data read from the file.
    :return: dataframe with the column ascending distance added.
    """
    return dataframe.withColumn(ASCENDING_DISTANCE,
                                when(col(LOITERING), col(VERTICAL_DISTANCE))
                                .otherwise(col(VERTICAL_DISTANCE) / 2 - col(DEL_ALTITUDE)))


def calculate_work_done(dataframe: DataFrame) -> DataFrame:
    """ Calculates the energy employed during the flight.

    :param dataframe: dataframe with the FLSTLOG data read from the file.
    :return: dataframe with the column work done added.
    """
    # For the moment the formula is work_done = 2 * ascending_distance + flight_time
    return dataframe.withColumn(WORK_DONE,
                                col(FLIGHT_TIME) + col(ASCENDING_DISTANCE)/col(VERTICAL_SPEED))

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
    return dataframe.withColumn(SPAWNED,
                            when(col(SPAWN_TIME).isNull(), False)
                            .otherwise(when(col(SPAWN_TIME) > settings.simulation.max_time, False)
                            .otherwise(True)))


def was_mission_completed(dataframe: DataFrame) -> DataFrame:
    """ Checks if the drone reached the desired destination during the simulation.

    :param dataframe: dataframe with the FLSTLOG data and flight intentions read from the files.
    :return: dataframe with the check if the destination was reached column added.
    """
    return dataframe \
        .withColumn(MISSION_COMPLETED,
                    when(col(DEL_X).isNull() & col(DEL_Y).isNull(), False)
                    .otherwise(when(
                        (col(DESTINATION_X) - col(DEL_X)) * (col(DESTINATION_X) - col(DEL_X)) +
                        ((col(DESTINATION_Y) - col(DEL_Y)) * (col(DESTINATION_Y) - col(DEL_Y)))
                        <= settings.thresholds.destination_distance, True)
                    .otherwise(when(
                        col(SPAWN_TIME) > settings.simulation.max_time, False)
                    .otherwise(False))))


def create_priority_weights(dataframe: DataFrame) -> DataFrame:
    """ Adds a column with the weight of given priority.

    :param dataframe: dataframe with the FLSTLOG data and flight intentions read from the files.
    :return: dataframe with priority weight column added.
    """
    dataframe = dataframe.withColumn(PRIORITY_WEIGHT,
                                     when(col(PRIORITY) == 1, settings.weights.priority_1).otherwise(
                                         when(col(PRIORITY) == 2, settings.weights.priority_2).otherwise(
                                             when(col(PRIORITY) == 3, settings.weights.priority_3).otherwise(
                                                 settings.weights.priority_4))))
    return dataframe


COMBINED_FLST_FP_INT_TRANSFORMATIONS = [calculate_ascending_distance, calculate_work_done, calculate_arrival_delay,
                                        calculate_departure_delay, was_spawned, was_mission_completed,
                                        create_priority_weights, remove_combined_unused_columns,
                                        reorder_combined_columns]


@logger.catch
def generate_combined_dataframe(scenario_name: str,
                                flst_log_dataframe: DataFrame,
                                flight_intentions: Dict[str, DataFrame]) -> DataFrame:
    """ Combines the FLST log with the reference flight intention file.

    :param scenario_name: scenario name of the file being processed.
    :param flst_log_dataframe: parsed flst log dataframe.
    :param flight_intentions: set of flight intentions dataframes.
    :return: joined flst log and flight intention dataframe.
    """
    fp_int_key = get_fp_int_key_from_scenario_name(scenario_name)
    fp_int_dataframe = flight_intentions.get(fp_int_key, None)
    if fp_int_dataframe:
        # Join flst log with flight plan using the ship id
        dataframe_tmp = flst_log_dataframe.join(fp_int_dataframe,
                                                on=ACID,
                                                how='outer')
        # Those intentions not contained in FLST log have scenario name null,
        # add scenario name by hand
        dataframe_tmp = dataframe_tmp \
            .withColumn(SCENARIO_NAME, lit(scenario_name))

        for transformation in COMBINED_FLST_FP_INT_TRANSFORMATIONS:
            logger.trace('Applying data transformation: {}.', transformation)
            dataframe_tmp = transformation(dataframe_tmp)
    else:
        raise KeyError('The flight intention file `Flight_intention_{}` was not parsed.', fp_int_key)

    return dataframe_tmp
