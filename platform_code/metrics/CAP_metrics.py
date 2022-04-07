from typing import Dict

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, mean, length, lit

from parser.parser_constants import FLST_LOG_PREFIX
from results.result_dataframes import build_result_df_by_scenario
from results.results_constants import (SAF_METRICS_RESULTS, CAP_METRICS_RESULTS,
                                       NUM_FLIGHTS, CAP1, CAP2, CAP3, CAP4, SAF2)
from schemas.tables_attributes import (SCENARIO_NAME, ARRIVAL_DELAY, SPAWNED,
                                       MISSION_COMPLETED)

REF_CAP1 = f'Ref_{CAP1}'
REF_CAP2 = f'Ref_{CAP2}'
REF_SCENARIO_NAME = f'Ref_{SCENARIO_NAME}'
SCENARIO_NAME_LENGTH = f'{SCENARIO_NAME}_LENGTH'


@logger.catch
def compute_cap1_metric(input_dataframes: Dict[str, DataFrame], *args, **kwargs) -> DataFrame:
    """ CAP-1: Average demand delay

    Average demand delay is computed as the arithmetic mean of the delays
    of all flight intentions in a scenario.

    :param input_dataframes: dataframes with the logs data.
    :return: query result with the CAP1 metric per scenario.
    """
    dataframe = input_dataframes[FLST_LOG_PREFIX]
    return dataframe \
        .where(SPAWNED) \
        .where(MISSION_COMPLETED) \
        .select(SCENARIO_NAME, ARRIVAL_DELAY) \
        .groupby(SCENARIO_NAME) \
        .agg(mean(ARRIVAL_DELAY).alias(CAP1))


@logger.catch
def compute_cap2_metric(input_dataframes: Dict[str, DataFrame],
                        output_dataframes: Dict[str, DataFrame],
                        *args, **kwargs) -> DataFrame:
    """ CAP-2: Average number of intrusions

    Is a ratio of the total number of intrusions with respect of the number of
    flight intention in the scenario.

    :param input_dataframes: dataframes with the logs data.
    :param output_dataframes: dataframes with the results computed previously.
    :return: query result with the CAP1 metric per scenario.
    """
    # Take the SAF-2 metric
    saf2_data = output_dataframes[SAF_METRICS_RESULTS].select(SCENARIO_NAME, SAF2)

    # Calculate the total number of flights deployed
    flst_log_df = input_dataframes[FLST_LOG_PREFIX]
    number_of_flights = flst_log_df \
        .where(SPAWNED) \
        .groupby(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col('count').alias(NUM_FLIGHTS)])

    # Divide the numer of intrusions by the total number of flights
    return saf2_data.join(number_of_flights, on=SCENARIO_NAME) \
        .withColumn(CAP2, col(SAF2) / col(NUM_FLIGHTS)) \
        .select(SCENARIO_NAME, CAP2)


@logger.catch
def compute_cap3_and_cap4_metrics(results: DataFrame,
                                  *args, **kwargs) -> DataFrame:
    """ Utility method that handles all the calculation of the CAP-3 and
    CAP-4 metrics efficiently.

    Generates a results temporary table with relates the scenarios with
    rogue carriers with their baseline, so the CAP 3 and CAP 4 can be calculated.

    :param results: table with the results of the CAP-1 and CAP-2.
    :return:
    """
    # First, get the dataframe with the baseline data
    scenarios_without_uncertainty = results \
        .where(~col(SCENARIO_NAME).rlike('.*_[R|W][1,2,3,5]')) \
        .select(col(SCENARIO_NAME).alias(REF_SCENARIO_NAME),
                col(CAP1).alias(REF_CAP1),
                col(CAP2).alias(REF_CAP2))

    # Then get only the scenarios with rogue carriers, and generate a column with
    # the name of its related baseline
    scenarios_with_rogue = results \
        .where(col(SCENARIO_NAME).rlike('.*_R[1,2,3]')) \
        .withColumn(SCENARIO_NAME_LENGTH, length(col(SCENARIO_NAME))) \
        .withColumn(REF_SCENARIO_NAME,
                    (col(SCENARIO_NAME).substr(lit(0), col(SCENARIO_NAME_LENGTH) - lit(3)))) \
        .drop(SCENARIO_NAME_LENGTH)

    # Join both tables so we can calculate CAP-3 and CAP-4
    query_result = scenarios_with_rogue.join(scenarios_without_uncertainty,
                                             on=REF_SCENARIO_NAME)

    return query_result \
        .withColumn(CAP3, col(REF_CAP1) - col(CAP1)) \
        .withColumn(CAP4, col(REF_CAP2) - col(CAP2)) \
        .select(SCENARIO_NAME, CAP3, CAP4)


CAP_METRICS = [
    compute_cap1_metric,
    compute_cap2_metric,
    compute_cap3_and_cap4_metrics
]


def compute_capacity_metrics(input_dataframes: Dict[str, DataFrame],
                             output_dataframes: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """ Calculates all the capacity metrics and add to the output dataframes dictionary
    their results.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :param output_dataframes: dictionary with the dataframes where the results are saved.
    :return: updated results dataframes with the capacity metrics.
    """
    logger.info('Generating plan for capacity metrics.')
    result_dataframe = build_result_df_by_scenario(input_dataframes)

    for metric in CAP_METRICS:
        logger.trace('Generating plan for metric: {}.', metric)
        query_result = metric(input_dataframes=input_dataframes,
                              output_dataframes=output_dataframes,
                              results=result_dataframe)
        result_dataframe = result_dataframe.join(query_result,
                                                 on=SCENARIO_NAME,
                                                 how='left')

    output_dataframes[CAP_METRICS_RESULTS] = result_dataframe
    return output_dataframes
