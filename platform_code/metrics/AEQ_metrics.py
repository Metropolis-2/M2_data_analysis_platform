# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""
from typing import Dict

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, stddev, mean, abs, max

from parse.parser_constants import FLST_LOG_PREFIX
from results.result_dataframes import build_result_df_by_scenario
from results.results_constants import AEQ_METRICS_RESULTS, NUM_FLIGHTS, COUNT
from schemas.tables_attributes import (SCENARIO_NAME, PRIORITY, LOITERING, AEQ1, AEQ2,
                                       ACID, FLIGHT_TIME, VEHICLE, AEQ2_1, AEQ1_1, AEQ3, AEQ4, AEQ5, AEQ5_1,
                                       ARRIVAL_DELAY, MISSION_COMPLETED, SPAWNED)
from utils.config import settings

AUTONOMY = 'Autonomy'
AVG_DELAY = 'Average_delay'
CANCELLATION_LIMIT = 'Cancellation_limit'
DELAY_INCREMENT = 'Delay_increment'
INOPERATIVE_TRAJECTORY = 'Inoperative'


def calculate_number_of_flights_per_scenario(dataframe: DataFrame) -> DataFrame:
    """ Calculates the number of flights in the scenario
    using the flight intention.

    :param dataframe: combined FLST log and Flight intention dataframe.
    :return: dataframe with the number of flights
    """
    return dataframe \
        .select(SCENARIO_NAME, ACID) \
        .groupby(SCENARIO_NAME) \
        .count() \
        .withColumnRenamed(COUNT, NUM_FLIGHTS)


@logger.catch
def calculate_avg_delay(dataframe: DataFrame) -> DataFrame:
    """ Calculates the average arrival delay per scenario.

    :param dataframe: combined FLST log and Flight intention dataframe.
    :return: average delay per scenario.
    """
    avg_delay = dataframe \
        .select(SCENARIO_NAME, ARRIVAL_DELAY) \
        .groupby(SCENARIO_NAME) \
        .agg(mean(ARRIVAL_DELAY).alias(AVG_DELAY))
    return avg_delay


@logger.catch
def compute_aeq1_metric(dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ AEQ-1: Number of cancelled demands
    Number of situations when realized arrival time of a given flight intention
    is greater than ideal expected arrival time by more or equal than some given
    cancellation delay limit that depends on mission type.

    The missions not completed are filtered from this metric.

    :param dataframe: combined FLST log and Flight intention dataframe.
    :return: query result with the AEQ1 metric per scenario.
    """
    return dataframe \
        .select(SCENARIO_NAME, PRIORITY, LOITERING, ARRIVAL_DELAY) \
        .withColumn(CANCELLATION_LIMIT,
                    when(col(PRIORITY) == 4, settings.thresholds.emergency_mission_delay).otherwise(
                        when(col(LOITERING), settings.thresholds.loitering_mission_delay).otherwise(
                            settings.thresholds.delivery_mission_delay))) \
        .select(SCENARIO_NAME, ARRIVAL_DELAY, CANCELLATION_LIMIT) \
        .withColumn(AEQ1, col(ARRIVAL_DELAY) >= col(CANCELLATION_LIMIT)) \
        .select(SCENARIO_NAME, AEQ1) \
        .where(col(AEQ1)) \
        .groupby(SCENARIO_NAME) \
        .count() \
        .select(SCENARIO_NAME, col(COUNT).alias(AEQ1))


@logger.catch
def compute_aeq1_1_metric(intermediate_results: DataFrame,
                          flights_per_scenario: DataFrame, *args, **kwargs) -> DataFrame:
    """ AEQ-1.1 Percentage of cancelled demands
    Calculated as the ratio of AEQ-1 and the total number of flight intentions
    in the given scenario.

    :param intermediate_results: current aeq results table.
    :param flights_per_scenario: number of flights planed for the scenario.
    :return: query result with the AEQ1-1 metric per scenario.
    """
    return intermediate_results \
        .select(SCENARIO_NAME, AEQ1) \
        .join(flights_per_scenario, on=[SCENARIO_NAME]) \
        .withColumn(AEQ1_1, (col(AEQ1) / col(NUM_FLIGHTS)) * 100) \
        .select(SCENARIO_NAME, AEQ1_1)


@logger.catch
def compute_aeq2_metric(dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ AEQ-2: Number of inoperative trajectories
    Number of situations when realized total mission duration is greater
    than specific drone autonomy.
    Realized trajectories and hence realized total mission duration comes
    directly from a simulation)

    :param dataframe: combined FLST log and Flight intention dataframe.
    :return: query result with the AEQ2 metric per scenario.
    """
    # TODO: ? Define autonomy.
    return dataframe \
        .select(SCENARIO_NAME, ACID, FLIGHT_TIME, VEHICLE) \
        .withColumn(AUTONOMY, when(col(VEHICLE) == "MP20", settings.MP20.autonomy)
                    .otherwise(settings.MP30.autonomy)) \
        .withColumn(INOPERATIVE_TRAJECTORY, when(col(FLIGHT_TIME) >= col(AUTONOMY), True)
                    .otherwise(False)) \
        .select(SCENARIO_NAME, col(INOPERATIVE_TRAJECTORY)) \
        .where(col(INOPERATIVE_TRAJECTORY)).groupby(SCENARIO_NAME) \
        .count() \
        .withColumnRenamed(COUNT, AEQ2)


@logger.catch
def compute_aeq2_1_metric(intermediate_results: DataFrame,
                          flights_per_scenario: DataFrame, *args, **kwargs) -> DataFrame:
    """ AEQ-2.1: Percentage of inoperative trajectories

    Calculated as the ratio of AEQ-2 and the total number of flight
    intentions in the given scenario.

    :param intermediate_results: current aeq results table.
    :param flights_per_scenario: number of flights planed for the scenario.
    :return: query result with the AEQ2-1 metric per scenario.
    """
    return intermediate_results \
        .select(SCENARIO_NAME, AEQ2) \
        .join(flights_per_scenario, on=SCENARIO_NAME) \
        .withColumn(AEQ2_1, (col(AEQ2) / col(NUM_FLIGHTS)) * 100) \
        .select(SCENARIO_NAME, AEQ2_1)


@logger.catch
def compute_aeq3_metric(dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ AEQ-3: The demand delay dispersion

    Measured as standard deviation of delay of all flight intentions,
    where delay for each flight intention is calculated as a difference between
    realized arrival time and ideal expected arrival time.

    Ideal expected arrival time is computed as arrival time of the fastest
    trajectory from origin to destination departing at the requested time as
    if a user were alone in the system, respecting all concept airspace rules.

    Realized arrival time comes directly from the simulations.
    The missions not completed are filtered from this metric.

    :param dataframe: combined FLST log and Flight intention dataframe.
    :return: query result with the AEQ3 metric per scenario.
    """
    return dataframe \
        .where(SPAWNED) \
        .where(MISSION_COMPLETED) \
        .select(SCENARIO_NAME, ACID, ARRIVAL_DELAY) \
        .groupby(SCENARIO_NAME) \
        .agg(stddev(ARRIVAL_DELAY).alias(AEQ3))


@logger.catch
def compute_aeq4_metric(dataframe: DataFrame,
                        avg_delay: DataFrame, *args, **kwargs) -> DataFrame:
    """ AEQ-4: The worst demand delay
    Computed as the maximal difference between any individual flight intention
    delay and the average delay, where delay for each flight intention is
    calculated as the difference between realized arrival time and
    ideal expected arrival time.

    The missions not started and completed are filtered from this metric.

    :param dataframe: combined FLST log and Flight intention dataframe.
    :param avg_delay: average delay per dataframe per scenario.
    :return: query result with the AEQ4 metric per scenario.
    """
    return dataframe \
        .where(SPAWNED) \
        .where(MISSION_COMPLETED) \
        .select(SCENARIO_NAME, ACID, ARRIVAL_DELAY) \
        .join(avg_delay, on=SCENARIO_NAME, how='left') \
        .withColumn(DELAY_INCREMENT, abs(col(ARRIVAL_DELAY) - col(AVG_DELAY))) \
        .groupby(SCENARIO_NAME) \
        .agg(max(DELAY_INCREMENT).alias(AEQ4))


@logger.catch
def compute_aeq5_metric(dataframe: DataFrame,
                        avg_delay: DataFrame, *args, **kwargs) -> DataFrame:
    """ AEQ-5: Number of inequitable delayed demands

    Number of flight intentions whose delay is greater than a given threshold
    from the average delay in absolute sense,
    where delay for each flight intention is calculated as the difference between
    realized arrival time and ideal expected arrival time.

    :param dataframe: combined FLST log and Flight intention dataframe.
    :param avg_delay: average delay per dataframe per scenario.
    :return: query result with the AEQ5 metric per scenario.
    """
    # TODO: ? Define the threshold for the AEQ-5
    return dataframe \
        .select(SCENARIO_NAME, ACID, ARRIVAL_DELAY) \
        .join(avg_delay, on=SCENARIO_NAME, how='left') \
        .select(SCENARIO_NAME, ACID) \
        .where(((col(ARRIVAL_DELAY) > col(AVG_DELAY) + settings.threshold.AEQ5) |
                (col(ARRIVAL_DELAY) < col(AVG_DELAY) - settings.threshold.AEQ5))) \
        .groupby(SCENARIO_NAME).count().withColumnRenamed(COUNT, AEQ5)


@logger.catch
def compute_aeq5_1_metric(intermediate_results: DataFrame,
                          flights_per_scenario: DataFrame, *args, **kwargs) -> DataFrame:
    """ AEQ-5-1: Percentage of inequitable delayed demands

    Calculated as the ratio of AEQ-5 and the total number of flight intentions in the given scenario.

    :param intermediate_results: current aeq results table.
    :param flights_per_scenario: number of flights planed for the scenario.
    :return: query result with the AEQ5-1 metric per scenario.
    """
    return intermediate_results \
        .select(SCENARIO_NAME, AEQ5) \
        .join(flights_per_scenario, on=SCENARIO_NAME) \
        .withColumn(AEQ5_1, (col(AEQ5) / col(NUM_FLIGHTS)) * 100) \
        .select(SCENARIO_NAME, AEQ5_1)


AEQ_METRICS = [
    compute_aeq1_metric,
    compute_aeq1_1_metric,
    compute_aeq2_metric,
    compute_aeq2_1_metric,
    compute_aeq3_metric,
    compute_aeq4_metric,
    compute_aeq5_metric,
    compute_aeq5_1_metric
]


def compute_accessibility_and_equality_metrics(input_dataframes: Dict[str, DataFrame],
                                               output_dataframes: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """ Calculates all the security metrics and add to the output dataframes dictionary
    their results.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :param output_dataframes: dictionary with the dataframes where the results are saved.
    :return: updated results dataframes with the security metrics.
    """
    logger.info('Calculating accessibility and equality metrics.')
    dataframe = input_dataframes[FLST_LOG_PREFIX]
    avg_delay = calculate_avg_delay(dataframe)
    flights_per_scenario = calculate_number_of_flights_per_scenario(dataframe)
    result_dataframe = build_result_df_by_scenario(input_dataframes)

    for metric in AEQ_METRICS:
        logger.trace('Calculating metric: {}.', metric)
        query_result = metric(dataframe=dataframe,
                              intermediate_results=result_dataframe,
                              avg_delay=avg_delay,
                              flights_per_scenario=flights_per_scenario)
        result_dataframe = result_dataframe.join(query_result,
                                                 on=SCENARIO_NAME,
                                                 how='left')

    output_dataframes[AEQ_METRICS_RESULTS] = result_dataframe
    return output_dataframes
