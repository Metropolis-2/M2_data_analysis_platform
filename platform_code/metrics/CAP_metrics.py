from typing import Dict

from loguru import logger
from pyspark.pandas import DataFrame
from pyspark.sql.functions import col, mean

from parse.parser_constants import FLST_LOG_PREFIX
from results.result_dataframes import build_result_df_by_scenario
from results.results_constants import SAF_METRICS_RESULTS, CAP_METRICS_RESULTS, NUM_FLIGHTS
from schemas.tables_attributes import (SCENARIO_NAME, BASELINE_ARRIVAL_TIME, DEL_TIME,
                                       CAP1, SAF2, CAP2)


@logger.catch
def compute_cap1_metric(input_dataframes: Dict[str, DataFrame], *args, **kwargs) -> DataFrame:
    """ CAP-1: Average demand delay

        Average demand delay is computed as the arithmetic mean of the delays
        of all flight intentions in a scenario.

        :param input_dataframes: dataframes with the logs data.
        :return: query result with the CAP1 metric per scenario.
        """
    dataframe = input_dataframes[FLST_LOG_PREFIX]
    # TODO: The delay per ACID is calculated here, check optimization
    return dataframe \
        .select(SCENARIO_NAME, BASELINE_ARRIVAL_TIME, DEL_TIME) \
        .groupby(SCENARIO_NAME) \
        .agg(mean(col(DEL_TIME) - col(BASELINE_ARRIVAL_TIME)).alias(CAP1))


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

    # Calculate the total number of flights executed
    # TODO: The number of flights per scenario is calculated here, check optimization
    flst_log_df = input_dataframes[FLST_LOG_PREFIX]
    number_of_flights = flst_log_df \
        .groupby(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col('count').alias(NUM_FLIGHTS)])

    # Divide the numer of intrusions by the total number of flights
    return saf2_data.join(number_of_flights, on=SCENARIO_NAME) \
        .withColumn(CAP2, col(SAF2) / col(NUM_FLIGHTS)) \
        .select(SCENARIO_NAME, CAP2)


@logger.catch
def compute_cap3_metric(dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ CAP-3: Additional demand delay

    Calculates the magnitude of delay increase (CAP-1) due to the fact of the existence of rogue aircraft.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the EFF3 per scenario and drone id.
    """
    pass


@logger.catch
def compute_cap4_metric(dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ CAP-4: Additional number of intrusions

    Calculates the degradation produced in the intrusion safety indicator
    when rogue aircraft are introduced.

    :param dataframe: data required to calculate the metrics.
    :return: query result with the EFF4 per scenario and drone id.
    """
    pass


CAP_METRICS = [
    compute_cap1_metric,
    compute_cap2_metric,
]


def compute_capacity_metrics(input_dataframes: Dict[str, DataFrame],
                             output_dataframes: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """ Calculates all the capacity metrics and add to the output dataframes dictionary
    their results.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :param output_dataframes: dictionary with the dataframes where the results are saved.
    :return: updated results dataframes with the capacity metrics.
    """
    result_dataframe = build_result_df_by_scenario(input_dataframes)

    for metric in CAP_METRICS:
        logger.trace('Calculating metric: {}.', metric)
        query_result = metric(input_dataframes=input_dataframes,
                              output_dataframes=output_dataframes)
        result_dataframe = result_dataframe.join(query_result,
                                                 on=SCENARIO_NAME,
                                                 how='left')

    output_dataframes[CAP_METRICS_RESULTS] = result_dataframe
    return output_dataframes
