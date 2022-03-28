from typing import Dict

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum

from parse.parser_constants import FLST_LOG_PREFIX
from results.result_dataframes import build_result_df_by_scenario_and_priority, build_result_df_by_scenario
from results.results_constants import PRI_METRICS_RESULTS_SCENARIO, PRI_METRICS_RESULTS_SCENARIO_PRIORITY
from schemas.tables_attributes import (FLIGHT_TIME, SCENARIO_NAME, PRIORITY, PRI1, PRI2, DISTANCE_3D, SPAWN_TIME,
                                       BASELINE_DEPARTURE_TIME, BASELINE_FLIGHT_TIME, PRI5, PRI3, PRI4)

FLIGHT_TIME_DELAY = "flight_time_delay"
DEPARTURE_DELAY = "departure_delay"
DISTANCE_PER_PRIORITY = 'Dprio'
MISSIONS_PER_PRIORITY = 'Nprio'
TIME_PER_PRIORITY = 'Tprio'


@logger.catch
def compute_pri1_metric(dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ PRI-1: Weighted mission duration

    Total duration of missions weighted in function of priority level.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the PRI1 per scenario and priority.
    """
    return dataframe \
        .groupby(SCENARIO_NAME, PRIORITY) \
        .agg((sum(FLIGHT_TIME) * col(PRIORITY)).alias(PRI1)) \
        .groupby(SCENARIO_NAME) \
        .agg(sum(PRI1).alias(PRI1))


@logger.catch
def compute_pri2_metric(dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ PRI-2: Weighted mission track length

    Total distance travelled weighted in function of priority level.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the PRI2 per scenario and priority.
    """
    return dataframe \
        .groupby(SCENARIO_NAME, PRIORITY) \
        .agg((sum(DISTANCE_3D) * col(PRIORITY)).alias(PRI2)) \
        .groupby(SCENARIO_NAME) \
        .agg(sum(PRI2).alias(PRI2))


# TODO: Check possible optimization joining flights per priority in PRI3 and PRI4
@logger.catch
def compute_pri3_metric(dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ PRI-3: Average mission duration per priority level

    The average mission duration for each priority level per aircraft.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the PRI3 per scenario and priority.
    """
    time_per_priority = dataframe. \
        groupby(SCENARIO_NAME, PRIORITY) \
        .agg(sum(FLIGHT_TIME).alias(TIME_PER_PRIORITY))

    # TODO: The flight time per priority is calculated here, check optimization
    flights_per_priority = dataframe \
        .groupby(SCENARIO_NAME, PRIORITY) \
        .count() \
        .withColumnRenamed('count', MISSIONS_PER_PRIORITY)

    return time_per_priority \
        .join(flights_per_priority, on=[SCENARIO_NAME, PRIORITY]) \
        .withColumn(PRI3, col(TIME_PER_PRIORITY) / col(MISSIONS_PER_PRIORITY)) \
        .select(SCENARIO_NAME, PRIORITY, PRI3)


@logger.catch
def compute_pri4_metric(dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ PRI-4: Average mission track length per priority level

    The average distance travelled for each priority level per aircraft.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the PRI4 per scenario and priority.
    """
    distance_per_priority = dataframe \
        .groupby(SCENARIO_NAME, PRIORITY) \
        .agg(sum(DISTANCE_3D).alias(DISTANCE_PER_PRIORITY))

    # TODO: The flight time per priority is calculated here, check optimization
    total_flights_per_priority = dataframe \
        .groupby(SCENARIO_NAME, PRIORITY) \
        .count() \
        .withColumnRenamed('count', MISSIONS_PER_PRIORITY)

    return distance_per_priority \
        .join(total_flights_per_priority, on=[SCENARIO_NAME, PRIORITY]) \
        .withColumn(PRI4, col(DISTANCE_PER_PRIORITY) / col(MISSIONS_PER_PRIORITY)) \
        .select(SCENARIO_NAME, PRIORITY, PRI4)


@logger.catch
def compute_pri5_metric(dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ PRI-5: Total delay per priority level

    The total delay experienced by aircraft in a certain priority category
    relative to ideal conditions.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the PRI5 per scenario and priority.
    """
    # TODO: The departure delay per ACID is calculated here, check optimization
    departure_delay = dataframe \
        .withColumn(DEPARTURE_DELAY, col(SPAWN_TIME) - col(BASELINE_DEPARTURE_TIME)) \
        .groupby(SCENARIO_NAME, PRIORITY) \
        .agg(sum(DEPARTURE_DELAY).alias(DEPARTURE_DELAY))

    # TODO: The flight time delay per ACID is calculated here, check optimization
    flight_time_delay = dataframe \
        .withColumn(FLIGHT_TIME_DELAY, col(FLIGHT_TIME) - col(BASELINE_FLIGHT_TIME)) \
        .groupby(SCENARIO_NAME, PRIORITY) \
        .agg(sum(FLIGHT_TIME_DELAY).alias(FLIGHT_TIME_DELAY))

    return departure_delay \
        .join(flight_time_delay, on=[SCENARIO_NAME, PRIORITY]) \
        .withColumn(PRI5, col(DEPARTURE_DELAY) + col(FLIGHT_TIME_DELAY)) \
        .select(SCENARIO_NAME, PRIORITY, PRI5)


PRI_METRICS_SCENARIO = [
    compute_pri1_metric,
    compute_pri2_metric
]
PRI_METRICS_SCENARIO_PRIORITY = [
    compute_pri3_metric,
    compute_pri4_metric,
    compute_pri5_metric
]


def compute_priority_metrics(input_dataframes: Dict[str, DataFrame],
                             output_dataframes: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """ Calculates all the priority metrics and add to the output dataframes dictionary
    their results.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :param output_dataframes: dictionary with the dataframes where the results are saved.
    :return: updated results dataframes with the security metrics.
    """
    # For this metrics we only use the combined FLST log with the flight plan intentions
    dataframe = input_dataframes[FLST_LOG_PREFIX]

    result_dataframe = build_result_df_by_scenario(input_dataframes)
    for metric in PRI_METRICS_SCENARIO:
        logger.trace('Calculating metric: {}.', metric)
        query_result = metric(dataframe=dataframe)
        result_dataframe = result_dataframe.join(query_result,
                                                 on=SCENARIO_NAME,
                                                 how='left')

    output_dataframes[PRI_METRICS_RESULTS_SCENARIO] = result_dataframe

    result_dataframe = build_result_df_by_scenario_and_priority(input_dataframes)
    for metric in PRI_METRICS_SCENARIO_PRIORITY:
        logger.trace('Calculating metric: {}.', metric)
        query_result = metric(dataframe=dataframe)
        result_dataframe = result_dataframe.join(query_result,
                                                 on=[SCENARIO_NAME, PRIORITY],
                                                 how='left')

    output_dataframes[PRI_METRICS_RESULTS_SCENARIO_PRIORITY] = result_dataframe
    return output_dataframes
