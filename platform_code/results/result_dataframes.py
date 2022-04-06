from pathlib import Path
from typing import Dict

from loguru import logger
from pyspark.sql import DataFrame
from tqdm import tqdm

from parse.parser_constants import FLST_LOG_PREFIX
from results.results_constants import SCENARIO_DATAFRAMES, SCENARIO_RESULTS, SCENARIO_PRIORITY_RESULTS
from schemas.tables_attributes import SCENARIO_NAME, PRIORITY
from utils.config import settings


def save_results_dataframes(results_dict: Dict[str, DataFrame]) -> None:
    """ Generates a dictionary with the dataframes that will save the results.

    :param results_dict: results for each of the metrics. Dictionary with
     the name of the metric as key and the results as value.
    """
    logger.info('Joining dataframes')
    scenario_results = None
    for name in tqdm(SCENARIO_DATAFRAMES):
        # Take a dataframe that includes data for all the scenarios
        logger.trace('Processing {} results.', name)
        dataframe = results_dict[name]

        if scenario_results:
            scenario_results.join(dataframe, on=SCENARIO_NAME, how='left')
        else:
            scenario_results = dataframe

    scenario_saving_path = Path(settings.saving_path,
                                f'{SCENARIO_RESULTS}.parquet')
    logger.info('Saving scenario results in: `{}`.', scenario_saving_path)
    scenario_results.write.parquet(str(scenario_saving_path))

    scenario_priority_saving_path = Path(settings.saving_path,
                                         f'{SCENARIO_PRIORITY_RESULTS}.parquet')
    logger.info('Saving scenario and priority results in `{}`.',
                scenario_priority_saving_path)
    scenario_results.write.parquet(str(scenario_priority_saving_path))


def build_result_df_by_scenario(log_dataframes: Dict[str, DataFrame]) -> DataFrame:
    """ Generates a dataframe where the key is the scenarios name.
    Intended for those metrics that perform the calculations differentiating by
    the scenario name.

    :param log_dataframes: dataframes of the parsed log files to analyze.
    :return: dataframes where the results of the metrics will be calculated.
    """
    dataframe = log_dataframes[FLST_LOG_PREFIX]
    result_df = dataframe.select(SCENARIO_NAME).distinct()

    return result_df


def build_result_df_by_scenario_and_priority(log_dataframes: Dict[str, DataFrame]) -> DataFrame:
    """ Generates a dataframe where the key are the scenarios and the IDs of the drones.
    Intended for those metrics that perform the calculations differentiating by
    the scenario name and the ACID.

    :param log_dataframes: dataframes of the parsed log files to analyze.
    :return: dataframe with the scenario name and ACID columsn.
    """
    # Take a dataframe that includes data for all the scenarios
    dataframe = log_dataframes[FLST_LOG_PREFIX]
    result_df = dataframe.select(SCENARIO_NAME, PRIORITY).distinct()

    return result_df
