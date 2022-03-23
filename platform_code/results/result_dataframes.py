from typing import Dict

from pyspark.sql import DataFrame

from parse.parser_constants import FLST_LOG_PREFIX
from schemas.tables_attributes import SCENARIO_NAME


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
