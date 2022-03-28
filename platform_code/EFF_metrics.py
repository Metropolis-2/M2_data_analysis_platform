from typing import Dict

from loguru import logger
from pyspark.pandas import DataFrame
from pyspark.sql.functions import col

from parse.parser_constants import FLST_LOG_PREFIX
from results.result_dataframes import build_result_df_by_scenario_and_acid
from results.results_constants import EFF_METRICS_RESULTS
from schemas.tables_attributes import (SCENARIO_NAME, ACID, BASELINE_2D_DISTANCE, DISTANCE_2D, EFF1, DISTANCE_ALT,
                                       BASELINE_VERTICAL_DISTANCE, EFF2, DISTANCE_ASCEND, BASELINE_ASCENDING_DISTANCE,
                                       EFF3, DISTANCE_3D, BASELINE_3D_DISTANCE, EFF4, FLIGHT_TIME, BASELINE_FLIGHT_TIME,
                                       EFF5, SPAWN_TIME, BASELINE_DEPARTURE_TIME, EFF6)


@logger.catch
def compute_eff1_metric(dataframe: DataFrame) -> DataFrame:
    """ EFF-1: Number of conflicts

    Ratio representing the length of the ideal horizontal route to the actual
    horizontal route.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the EFF1 per scenario and drone id.
    """
    return dataframe \
        .select(SCENARIO_NAME, ACID, BASELINE_2D_DISTANCE, DISTANCE_2D) \
        .withColumn(EFF1, col(BASELINE_2D_DISTANCE) / col(DISTANCE_2D)) \
        .select(SCENARIO_NAME, ACID, EFF1)


@logger.catch
def compute_eff2_metric(dataframe: DataFrame) -> DataFrame:
    """ EFF-2: Vertical distance route efficiency

    Ratio representing the length of the ideal vertical route to the actual
    vertical route.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the EFF2 per scenario and drone id.
    """
    return dataframe \
        .select(SCENARIO_NAME, ACID, DISTANCE_ALT, BASELINE_VERTICAL_DISTANCE) \
        .withColumn(EFF2, col(BASELINE_VERTICAL_DISTANCE) / col(DISTANCE_ALT)) \
        .select(SCENARIO_NAME, ACID, EFF2)


@logger.catch
def compute_eff3_metric(dataframe: DataFrame) -> DataFrame:
    """ EFF-3: Ascending route efficiency

    Ratio representing the length of the ascending distance in the ideal route
    to the length of the ascending distance of the actual route.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the EFF3 per scenario and drone id.
    """
    return dataframe \
        .select(SCENARIO_NAME, ACID, DISTANCE_ASCEND, BASELINE_ASCENDING_DISTANCE) \
        .withColumn(EFF3, col(BASELINE_ASCENDING_DISTANCE) / col(DISTANCE_ASCEND)) \
        .select(SCENARIO_NAME, ACID, EFF3)


@logger.catch
def compute_eff4_metric(dataframe: DataFrame) -> DataFrame:
    """ EFF-4: 3D distance route efficiency

    Ratio representing the 3D length of the ideal route to the 3D length
    of the actual route.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the EFF4 per scenario and drone id.
    """
    return dataframe \
        .select(SCENARIO_NAME, ACID, DISTANCE_3D, BASELINE_3D_DISTANCE) \
        .withColumn(EFF4, col(BASELINE_3D_DISTANCE) / col(DISTANCE_3D)) \
        .select(SCENARIO_NAME, ACID, EFF4)


@logger.catch
def compute_eff5_metric(dataframe: DataFrame) -> DataFrame:
    """ EFF-5: Route duration efficiency

    Ratio representing the time duration of the ideal route to the time
    duration of the actual route.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the EFF5 per scenario and drone id.
    """
    return dataframe \
        .select(SCENARIO_NAME, ACID, FLIGHT_TIME, BASELINE_FLIGHT_TIME) \
        .withColumn(EFF5, col(BASELINE_FLIGHT_TIME) / col(FLIGHT_TIME)) \
        .select(SCENARIO_NAME, ACID, EFF5)


@logger.catch
def compute_eff6_metric(dataframe: DataFrame) -> DataFrame:
    """ EFF-6: Departure delay

    Time duration from the planned departure time until the actual
    departure time of the aircraft.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the EFF6 per scenario and drone id.
    """
    # TODO: The departure delay per ACID is calculated here, check optimization
    return dataframe \
        .select(SCENARIO_NAME, ACID, SPAWN_TIME, BASELINE_DEPARTURE_TIME) \
        .withColumn(EFF6, col(SPAWN_TIME) - col(BASELINE_DEPARTURE_TIME)) \
        .select(SCENARIO_NAME, ACID, EFF6)


EFF_METRICS = [compute_eff1_metric, compute_eff2_metric, compute_eff3_metric,
               compute_eff4_metric, compute_eff5_metric, compute_eff6_metric]


def compute_efficiency_metrics(input_dataframes: Dict[str, DataFrame],
                               output_dataframes: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """ Calculates all the efficiency metrics and add to the output dataframes dictionary
    their results.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :param output_dataframes: dictionary with the dataframes where the results are saved.
    :return: updated results dataframes with the efficiency metrics.
    """
    # For this metrics we only use the combined FLST log with the flight plan intentions
    dataframe = input_dataframes[FLST_LOG_PREFIX]
    result_dataframe = build_result_df_by_scenario_and_acid(input_dataframes)

    for metric in EFF_METRICS:
        logger.trace('Calculating metric: {}.', metric)
        query_result = metric(dataframe)
        result_dataframe = result_dataframe.join(query_result,
                                                 on=[SCENARIO_NAME, ACID],
                                                 how='left')

    output_dataframes[EFF_METRICS_RESULTS] = result_dataframe
    return output_dataframes
