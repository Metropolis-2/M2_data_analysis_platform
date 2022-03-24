from itertools import islice
from pathlib import Path
from typing import Tuple, List, Union

from loguru import logger
from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from utils.parser_utils import build_scenario_name

# There are no transformations yet for the REGLOG dataframe
REG_LOG_TRANSFORMATIONS = list()


def read_reglog(log_file: Union[str, Path]) -> Tuple[List[float], List[str], List[str], List[str], List[str]]:
    """ Read the regular log file generating the structures with
    the information of each ACID in the timestamp saved.

    In the regular log, a snapshot of the simulation is saved every 30 seconds.
    Four lines are saved, with the timestamp of the simulation as first
    element in each of them. Then, the first line contain the IDs of the
    flying elements, the second, their altitudes, the third, the latitudes and the
    fourth, the longitudes.

    :param log_file: path to the REGLOG file.
    :return: List with the timestamps, ACIDs, ALTs, LATs and LONs of the file.
    """
    timestamp_list = list()
    acid_lines_list = list()
    alt_lines_list = list()
    lon_lines_list = list()
    lat_lines_list = list()

    with open(log_file, 'r') as reglog_file:
        cnt = 0
        # To ignore the first 9 lines of comments
        for line in islice(reglog_file, 9, None, 1):
            # Remove '\n' and divide
            split_line = line.strip().split(',')
            elements = split_line[1:]
            if cnt == 0:
                cnt += 1
                timestamp_list.append(float(split_line[0]))
                acid_lines_list.append(elements)
            elif cnt == 1:
                cnt += 1
                alt_lines_list.append(elements)
            elif cnt == 2:
                cnt += 1
                lat_lines_list.append(elements)
            else:
                cnt = 0
                lon_lines_list.append(elements)

    return timestamp_list, acid_lines_list, alt_lines_list, lat_lines_list, lon_lines_list


def generate_reg_log_dataframe(log_files: List[Path],
                               schema: StructType,
                               transformations: List,
                               spark: SparkSession) -> DataFrame:
    """ Parses the REGLOG files. This kind of log file is more complex
    as every four lines of the file contains the information of a set
    of rows of the final dataframe.

    :param log_files: list of log files paths of the same type.
    :param schema: file schema of the log files to read.
    :param transformations: set of functions that perform a
     transformation in the dataframe.
    :param spark: Spark session of the execution.
    :return: final dataframe.
    """
    dataframe = None

    for log_file in log_files:
        reg_log_list = list()
        reg_log_object_counter = 0
        reg_log_data = read_reglog(log_file)
        scenario_name = build_scenario_name(log_file)
        logger.debug('Processing file: `{}` with scenario name: {}.', log_file.name, scenario_name)

        for timestamp, acids, alts, lats, lons in zip(*reg_log_data):
            for acid, alt, lat, lon in zip(acids, alts, lats, lons):
                logger.trace('Creating regular log line for ACID `{}` flying in `{}, {}` at {} meters.',
                             acid, lat, lon, alt)
                data_line = [reg_log_object_counter, scenario_name,
                             timestamp, acid, float(alt), float(lat), float(lon)]
                reg_log_object_counter += 1
                reg_log_list.append(data_line)

        dataframe_tmp = spark.createDataFrame(reg_log_list, schema)
        for transformation in transformations:
            logger.trace('Applying data transformation: {}.', transformation)
            dataframe_tmp = transformation(dataframe_tmp)

        if dataframe:
            dataframe = dataframe.union(dataframe_tmp)
        else:
            dataframe = dataframe_tmp

    return dataframe
