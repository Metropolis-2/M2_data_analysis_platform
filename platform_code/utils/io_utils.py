import os
from pathlib import Path
from typing import Dict, List

from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from config import settings


def save_dataframes_dict(dataframes: Dict[str, DataFrame]) -> None:
    """ Saves the dataframes of a dictionary using as filename its key.

    :param dataframes: dictionary with dataframes saved as element and
     string as keys.
    """
    os.makedirs(settings.saving_path, exist_ok=True)
    for name, df in dataframes.items():
        logger.info('Saving log dataframe {}.', name)
        saving_path = Path(settings.saving_path, f'{name.lower()}.parquet')
        df.write.parquet(str(saving_path))


def load_dataframes(files_names: List[str], spark: SparkSession) -> Dict[str, DataFrame]:
    """ Loads the dataframes which macht the file names passed by arguments.
    The method read from the config the path were to read the files, which
    matches the folder where the files are saved in `save_dataframes_dict()`.

    :param files_names: list of the names of the files.
    :param spark: spark session.
    :return: dictionary with the dataframes loaded from the files, with the
     file name as key.
    """
    dataframes = dict()

    for file_name in files_names:
        file_path = Path(settings.saving_path, f'{file_name.lower()}.parquet')
        logger.info('Loading dataframe from `{}`.', file_path)
        df = spark.read.parquet(str(file_path))
        dataframes[file_name] = df

    return dataframes
