from typing import Dict

from pyspark.sql import DataFrame

from parse.parser_constants import FLST_LOG_PREFIX
from schemas.tables_attributes import SCENARIO_NAME, ACID


def build_results_dataframes(log_dataframes: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """ Generates a dictionary with the dataframes that will save the results.

    :param log_dataframes: dataframes of the parsed log files to analyze.
    :return: dataframes where the results of the metrics will be calculated.
    """
    output_dataframes = dict()
    # Take a dataframe that includes data for all the scenarios
    dataframe = log_dataframes[FLST_LOG_PREFIX]
    output_dataframes['OUTPUT'] = dataframe.select(SCENARIO_NAME).distinct()
    # TODO: generate sound output dataframe

    return output_dataframes


def build_result_df_by_scenario_and_acid(log_dataframes: Dict[str, DataFrame]) -> DataFrame:
    """ Generates a dataframe where the key are the scenarios and the IDs of the drones.
    Intended for those metrics that perform the calculations differentiating by
    the scenario name and the ACID.

    :param log_dataframes: dataframes of the parsed log files to analyze.
    :return: dataframe with the scenario name and ACID columsn.
    """
    # Take a dataframe that includes data for all the scenarios
    dataframe = log_dataframes[FLST_LOG_PREFIX]
    result_df = dataframe.select(SCENARIO_NAME, ACID).distinct()

    return result_df
