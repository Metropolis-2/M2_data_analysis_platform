from itertools import islice
from pathlib import Path
from typing import Tuple, List, Union

from loguru import logger
from pyspark.sql import SparkSession

from parse.parser_utils import build_scenario_name
from schemas.reg_log_schema import REG_LOG_SCHEMA


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


def generate_reg_log_dataframe(spark, log_files: List[Path]):
    """

    :param log_files:
    :param schema:
    :return:
    """
    dataframe = None

    for log_file in log_files:
        reg_log_list = list()
        reg_log_object_counter = 0
        reg_log_data = read_reglog(log_file)
        scenario_name = build_scenario_name(log_file)
        logger.info('Processing file: `{}` with scenario name: {}.', log_file.name, scenario_name)

        for timestamp, acids, alts, lats, lons in zip(*reg_log_data):
            for acid, alt, lat, lon in zip(acids, alts, lats, lons):
                logger.trace('Creating regular log line for ACID `{}` flying in `{}, {}` at {} meters.',
                             acid, lat, lon, alt)
                data_line = [reg_log_object_counter, scenario_name,
                             timestamp, acid, float(alt), float(lat), float(lon)]
                reg_log_object_counter += 1
                reg_log_list.append(data_line)

        dataframe_tmp = spark.createDataFrame(reg_log_list, REG_LOG_SCHEMA)
        if dataframe:
            dataframe = dataframe.union(dataframe_tmp)
        else:
            dataframe = dataframe_tmp

    return dataframe


if __name__ == '__main__':
    file = Path(
        "C:\\Users\\arodgril\\Repos\\Metropolis\\M2_data_analysis_platform\\platform_code\\example_logs\\Centralised\\REGLOG_Flight_intention_very_low_40_8_W1_20220201_17-13-56.log")

    spark = SparkSession.builder.appName('Platform Analysis.com').getOrCreate()
    generate_reg_log_dataframe(spark, [file])
