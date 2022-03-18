from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col

from parse.parser_constants import SCENARIOS, LINE_COUNT


def build_scenario_name(file_name: Path) -> str:
    """ Generates the scenario name for the file to process.

    :param file_name: Path to the log file to parse.
    :return: the scenario name generated from the file.
    """
    logger.trace('Obtaining the scenario name for file: {}.', file_name)
    concept = file_name.parent.name
    concept_index = str(SCENARIOS.index(concept) + 1)

    fp_intention = file_name.stem.split("_")[3:-2]
    distribution = fp_intention[-3]
    repetition = fp_intention[-2]
    uncertainty = fp_intention[-1]

    # If the flight intention is very low, the split generates 5 elements, otherwise 4.
    if len(fp_intention) == 5:
        density = "very_low"
    else:
        density = fp_intention[0]

    scenario_name = concept_index + "_" + density + "_" + distribution + "_" + repetition + "_" + uncertainty
    logger.debug('Scenario name obtained: {}.', scenario_name)
    return scenario_name


def add_dataframe_counter(dataframe: DataFrame, counter_name: str) -> DataFrame:
    """ Adds a counter for each of the rows in the dataframe.

    :param dataframe: dataframe to add the counter.
    :param counter_name: attribute name of the counter.
    :return: dataframe with the counter added.
    """
    return dataframe.withColumn(counter_name, monotonically_increasing_id())


def remove_commented_log_lines(dataframe: DataFrame) -> DataFrame:
    """ Function that removes from the dataframe the 9 lines of
    comments that exist in each log file.

    :param dataframe: loaded dataframe from the log file.
    :return: filtered dataframe without the comment lines.
    """
    dataframe = add_dataframe_counter(dataframe, LINE_COUNT)
    dataframe = dataframe.filter(col(LINE_COUNT) >= 9)
    return dataframe.drop(col(LINE_COUNT))
