from typing import Dict, Union

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, min

from parse.parser_constants import CONF_LOG_PREFIX, LOS_LOG_PREFIX, GEO_LOG_PREFIX
from results.result_dataframes import build_result_df_by_scenario
from results.results_constants import SAF_METRICS_RESULTS, COUNT
from schemas.tables_attributes import SCENARIO_NAME, SAF1, SAF2, SAF3, DISTANCE, SAF4, SAF6, SAF5, LOS_DURATION_TIME


@logger.catch
def compute_saf1_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """
    SAF-1: Number of conflicts

    Number of aircraft pairs that will experience a loss of separation
    within the look-ahead time.
    """
    dataframe = input_dataframes[CONF_LOG_PREFIX]
    return dataframe \
        .groupBy(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col(COUNT).alias(SAF1)])


@logger.catch
def compute_saf2_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """ SAF-2: Number of intrusions

    Number of aircraft pairs that experience loss of separation.
    """
    dataframe = input_dataframes[LOS_LOG_PREFIX]
    return dataframe \
        .groupBy(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col(COUNT).alias(SAF2)])


@logger.catch
def compute_saf3_metric(intermediate_results: DataFrame, *args, **kwargs):
    """ SAF-3: Intrusion prevention rate
    Ratio representing the proportion of conflicts that did
    not result in a loss of separation.
    """
    return intermediate_results \
        .withColumn(SAF3, col(SAF1) / col(SAF2)) \
        .select(SCENARIO_NAME, SAF3)


@logger.catch
def compute_saf4_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """
    SAF-4: Minimum separation
    (The minimum separation between aircraft during conflicts)
    """
    dataframe = input_dataframes[LOS_LOG_PREFIX]
    return dataframe \
        .groupby(SCENARIO_NAME) \
        .agg(min(DISTANCE).alias(SAF4))


@logger.catch
def compute_saf5_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """ SAF-5: Time spent in LOS

    Total time spent in a state of intrusion.
    """
    dataframe = input_dataframes[LOS_LOG_PREFIX]
    # TODO: ? Define the representation. Mean/Sum of scenario
    return dataframe \
        .select(SCENARIO_NAME, col(LOS_DURATION_TIME).alias(SAF5))


@logger.catch
def compute_saf6_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """
    SAF-6: Geofence violations
    (The number of geofence/building area violations)
    """
    dataframe = input_dataframes[GEO_LOG_PREFIX]
    return dataframe \
        .groupBy(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col('count').alias(SAF6)])


SAF_METRICS = [compute_saf1_metric, compute_saf2_metric, compute_saf3_metric,
               compute_saf4_metric, compute_saf5_metric, compute_saf6_metric]


def compute_security_metrics(input_dataframes: Dict[str, DataFrame],
                             output_dataframes: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """ Calculates all the security metrics and add to the output dataframes dictionary
    their results.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :param output_dataframes: dictionary with the dataframes where the results are saved.
    :return: updated results dataframes with the security metrics.
    """
    result_dataframe = build_result_df_by_scenario(input_dataframes)

    for metric in SAF_METRICS:
        logger.trace('Calculating metric: {}.', metric)
        query_result = metric(input_dataframes=input_dataframes,
                              intermediate_results=result_dataframe)
        result_dataframe = result_dataframe.join(query_result,
                                                 on=SCENARIO_NAME,
                                                 how='left')

    output_dataframes[SAF_METRICS_RESULTS] = result_dataframe
    return output_dataframes