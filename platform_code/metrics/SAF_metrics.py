from typing import Dict, Union

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, min, sum

from parse.parser_constants import CONF_LOG_PREFIX, LOS_LOG_PREFIX, GEO_LOG_PREFIX
from results.result_dataframes import build_result_df_by_scenario
from results.results_constants import (SAF_METRICS_RESULTS, COUNT, SAF1, SAF2,
                                       SAF2_1, SAF3, SAF4, SAF5, SAF6, SAF6_1, SAF6_4, SAF6_5, SAF6_3, SAF6_2)
from schemas.tables_attributes import (SCENARIO_NAME, DISTANCE, LOS_DURATION_TIME,
                                       VIOLATION_SEVERITY, CRASH, OPEN_AIRSPACE, LOITERING_NFZ)


@logger.catch
def compute_saf1_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """ SAF-1: Number of conflicts

    Number of aircraft pairs that will experience a loss of separation
    within the look-ahead time.

    :param input_dataframes: dataframes with the logs data.
    :return: query result with the SAF1 metric per scenario.
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

    :param input_dataframes: dataframes with the logs data.
    :return: query result with the SAF2 metric per scenario.
    """
    dataframe = input_dataframes[LOS_LOG_PREFIX]
    return dataframe \
        .groupBy(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col(COUNT).alias(SAF2)])


@logger.catch
def compute_saf2_1_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """ SAF-2-1: Count of crashes

    Count of crashes for each scenario (each aircraft logged in the FLSTlog has a boolean flag called crash)
    if that is true ir counts as a crash and the number of the times crash is true is the result of this metric.

    :param input_dataframes: dataframes with the logs data.
    :return: query result with the SAF2-1 metric per scenario.
    """
    dataframe = input_dataframes[LOS_LOG_PREFIX]
    return dataframe \
        .where(CRASH) \
        .groupBy(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col(COUNT).alias(SAF2_1)])


@logger.catch
def compute_saf3_metric(intermediate_results: DataFrame, *args, **kwargs):
    """ SAF-3: Intrusion prevention rate

    Ratio representing the proportion of conflicts that did
    not result in a loss of separation.

    :param intermediate_results: intermediate results dataframe for SAF metrics.
    :return: query result with the SAF3 metric per scenario.
    """
    return intermediate_results \
        .withColumn(SAF3, col(SAF1) / col(SAF2)) \
        .select(SCENARIO_NAME, SAF3)


@logger.catch
def compute_saf4_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """ SAF-4: Minimum separation

    The minimum separation between aircraft during conflicts.

    :param input_dataframes: dataframes with the logs data.
    :return: query result with the SAF4 metric per scenario.
    """
    dataframe = input_dataframes[LOS_LOG_PREFIX]
    return dataframe \
        .groupby(SCENARIO_NAME) \
        .agg(min(DISTANCE).alias(SAF4))


@logger.catch
def compute_saf5_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """ SAF-5: Time spent in LOS

    Total time spent in a state of intrusion.

    :param input_dataframes: dataframes with the logs data.
    :return: query result with the SAF5 metric per scenario.
    """
    dataframe = input_dataframes[LOS_LOG_PREFIX]
    return dataframe \
        .select(SCENARIO_NAME, LOS_DURATION_TIME) \
        .groupby(SCENARIO_NAME) \
        .agg(sum(LOS_DURATION_TIME).alias(SAF5)) \
        .select(SCENARIO_NAME, SAF5)


@logger.catch
def compute_saf6_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """ SAF-6: Geofence violations

    The number of geofence/building area violations.

    :param input_dataframes: dataframes with the logs data.
    :return: query result with the SAF6 metric per scenario.
    """
    dataframe = input_dataframes[GEO_LOG_PREFIX]
    return dataframe \
        .groupBy(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col('count').alias(SAF6)])


@logger.catch
def compute_saf6_1_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """ SAF-6_1: Severe Geofence violations

    The number of severe geofence/building area violations.
    Every geofence violation in the GEOlog has a severity flag.

    :param input_dataframes: dataframes with the logs data.
    :return: query result with the SAF6-1 metric per scenario.
    """
    dataframe = input_dataframes[GEO_LOG_PREFIX]
    return dataframe \
        .where(col(VIOLATION_SEVERITY)) \
        .groupBy(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col('count').alias(SAF6_1)])


@logger.catch
def compute_saf6_2_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """ SAF-6_2: Severe and Open Geofence violations

    The number of severe and open geofence/building area violations.
    Every geofence violation in the GEOlog has a severity flag and open flag.

    :param input_dataframes: dataframes with the logs data.
    :return: query result with the SAF6-2 metric per scenario.
    """
    dataframe = input_dataframes[GEO_LOG_PREFIX]
    return dataframe \
        .where(col(VIOLATION_SEVERITY)) \
        .where(col(OPEN_AIRSPACE)) \
        .groupBy(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col('count').alias(SAF6_2)])

@logger.catch
def compute_saf6_3_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """ SAF-6_3: Severe and non Open and non Loitering Geofence violations

    The number of severe and not open and not loitering geofence/building area violations.
    Every geofence violation in the GEOlog has a severity flag and false open and false loitering flags.

    :param input_dataframes: dataframes with the logs data.
    :return: query result with the SAF6-3 metric per scenario.
    """
    dataframe = input_dataframes[GEO_LOG_PREFIX]
    return dataframe \
        .where(col(VIOLATION_SEVERITY)) \
        .where(~col(OPEN_AIRSPACE)) \
        .where(~col(LOITERING_NFZ)) \
        .groupBy(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col('count').alias(SAF6_3)])

@logger.catch
def compute_saf6_4_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """ SAF-6_4: Severe and Loitering Geofence violations

    The number of severe and loitering geofence/building area violations.
    Every geofence violation in the GEOlog has a severity and loitering flags.

    :param input_dataframes: dataframes with the logs data.
    :return: query result with the SAF6-4 metric per scenario.
    """
    dataframe = input_dataframes[GEO_LOG_PREFIX]
    return dataframe \
        .where(col(VIOLATION_SEVERITY)) \
        .where(col(LOITERING_NFZ)) \
        .groupBy(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col('count').alias(SAF6_4)])

@logger.catch
def compute_saf6_5_metric(input_dataframes: Union[str, DataFrame], *args, **kwargs):
    """ SAF-6_5: Severe and non Loitering Geofence violations

    The number of severe and non loitering geofence/building area violations.
    Every geofence violation in the GEOlog has a severity and non loitering flags.

    :param input_dataframes: dataframes with the logs data.
    :return: query result with the SAF6-5 metric per scenario.
    """
    dataframe = input_dataframes[GEO_LOG_PREFIX]
    return dataframe \
        .where(col(VIOLATION_SEVERITY)) \
        .where(~col(LOITERING_NFZ)) \
        .groupBy(SCENARIO_NAME) \
        .count() \
        .select([SCENARIO_NAME, col('count').alias(SAF6_5)])




SAF_METRICS = [compute_saf1_metric, compute_saf2_metric, compute_saf2_1_metric, compute_saf3_metric,
               compute_saf4_metric, compute_saf5_metric, compute_saf6_metric, compute_saf6_1_metric, compute_saf6_2_metric, compute_saf6_3_metric]


def compute_safety_metrics(input_dataframes: Dict[str, DataFrame],
                           output_dataframes: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """ Calculates all the security metrics and add to the output dataframes dictionary
    their results.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :param output_dataframes: dictionary with the dataframes where the results are saved.
    :return: updated results dataframes with the security metrics.
    """
    logger.info('Generating plan for safety metrics.')
    result_dataframe = build_result_df_by_scenario(input_dataframes)

    for metric in SAF_METRICS:
        logger.trace('Generating plan for metric: {}.', metric)
        query_result = metric(input_dataframes=input_dataframes,
                              intermediate_results=result_dataframe)
        result_dataframe = result_dataframe.join(query_result,
                                                 on=SCENARIO_NAME,
                                                 how='left')

    output_dataframes[SAF_METRICS_RESULTS] = result_dataframe
    return output_dataframes
