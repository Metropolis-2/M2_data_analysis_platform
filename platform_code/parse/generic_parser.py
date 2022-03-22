import sys
from pathlib import Path
from typing import List, Tuple, Dict

from loguru import logger
from pyarrow import StructType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from config import settings
from parse.combined_flst_fp_int_parser import generate_combined_dataframe
from parse.conf_log_parser import CONF_LOG_TRANSFORMATIONS
from parse.flst_log_parser import FLST_LOG_TRANSFORMATIONS
from parse.fp_int_parser import FP_INT_TRANSFORMATIONS
from parse.geo_log_parser import GEO_LOG_TRANSFORMATIONS
from parse.los_log_parser import LOS_LOG_TRANSFORMATIONS
from parse.parser_constants import FLST_LOG_PREFIX, FP_INT_PREFIX, CONF_LOG_PREFIX, LOS_LOG_PREFIX, GEO_LOG_PREFIX, \
    REG_LOG_PREFIX
from parse.parser_utils import (build_scenario_name, remove_commented_log_lines, get_scenario_data_from_fp_int)
from parse.reg_log_parser import REG_LOG_TRANSFORMATIONS, generate_reg_log_dataframe
from schemas.conf_log_schema import CONF_LOG_FILE_SCHEMA
from schemas.flst_log_schema import FLST_LOG_FILE_SCHEMA
from schemas.fp_int_schema import FP_INT_FILE_SCHEMA
from schemas.geo_log_schema import GEO_LOG_FILE_SCHEMA
from schemas.los_log_schema import LOS_LOG_FILE_SCHEMA
from schemas.reg_log_schema import REG_LOG_SCHEMA
from schemas.tables_attributes import SCENARIO_NAME

# Configuration for the log names with the schema associated and the transformations
# required after the read of the log file.
# Prefix: (schema, transformations)
PARSE_CONFIG = {
    CONF_LOG_PREFIX: (CONF_LOG_FILE_SCHEMA, CONF_LOG_TRANSFORMATIONS),
    LOS_LOG_PREFIX: (LOS_LOG_FILE_SCHEMA, LOS_LOG_TRANSFORMATIONS),
    GEO_LOG_PREFIX: (GEO_LOG_FILE_SCHEMA, GEO_LOG_TRANSFORMATIONS),
    FLST_LOG_PREFIX: (FLST_LOG_FILE_SCHEMA, FLST_LOG_TRANSFORMATIONS),
    REG_LOG_PREFIX: (REG_LOG_SCHEMA, REG_LOG_TRANSFORMATIONS)
}


def parse_flight_intentions(spark: SparkSession) -> Dict[str, DataFrame]:
    """ Generates a dataframe from each of the flight intentions, saving them
    in a dictionary using the scenario data as key, ignoring the concept, as
    the flight intention is the same for each concept.

    :param spark: Spark session of the execution.
    :return: dictionary with each of the flight intentions, using the
     scenario data as key.
    """
    fp_int_dataframes = dict()
    fp_int_paths = list(path.rglob(f"{FP_INT_PREFIX}*.csv"))

    for fp_int_path in fp_int_paths:
        logger.trace('Reading file: `{}`.', fp_int_path)
        scenario_data = get_scenario_data_from_fp_int(fp_int_path)
        dataframe = spark.read.csv(str(fp_int_path), header=False, schema=FP_INT_FILE_SCHEMA)

        for transformation in FP_INT_TRANSFORMATIONS:
            logger.trace('Applying data transformation: {}.', transformation)
            dataframe = transformation(dataframe)

        fp_int_dataframes[scenario_data] = dataframe

    return fp_int_dataframes


def parse_log_files(parse_config: Dict[str, Tuple],
                    flight_intentions: Dict[str, DataFrame],
                    spark: SparkSession) -> Dict[str, DataFrame]:
    """ Parses the different log files indicated in the parsing configuration.

    :param parse_config: dictionary with the prefix of the log file as key,
     containing the table schema and the transformation to perform.
    :param flight_intentions: set of flight intentions dataframes.
    :param spark: Spark session of the execution.
    :return: dictionary of the dataframes for each log type.
    """
    dataframes = dict()

    for log_type_config in parse_config.items():
        log_prefix, (schema, transformations) = log_type_config
        logger.info('Processing {} log files.', log_prefix)

        log_paths = list(path.rglob(f"{log_prefix}*.log"))

        if log_prefix is not REG_LOG_PREFIX:
            dataframe = parse_log_file(log_paths, schema, transformations, flight_intentions, spark)
        else:
            dataframe = generate_reg_log_dataframe(log_paths, schema, transformations, spark)

        dataframes[log_prefix] = dataframe

    return dataframes


def parse_log_file(log_files: List[Path],
                   schema: StructType,
                   transformations: List,
                   flight_intentions: Dict[str, DataFrame],
                   spark: SparkSession) -> DataFrame:
    """ Generates a dataframe from all the log files of the same type.
    This method reads the file directly using the schema indicated and
    performs the transformations to add, transform or removed the desired
    columns.

    This method adds the scenario name for each of the log files.

    :param log_files: list of log files paths of the same type.
    :param schema: file schema of the log files to read.
    :param transformations: set of functions that perform a
     transformation in the dataframe.
    :param flight_intentions: set of flight intentions dataframes.
    :param spark: Spark session of the execution.
    :return: final dataframe.
    """
    dataframe = None

    for log_file in log_files:
        logger.trace('Reading file: `{}`.', log_file)

        dataframe_tmp = spark.read.csv(str(log_file), header=False, schema=schema)

        scenario_name = build_scenario_name(log_file)
        dataframe_tmp = remove_commented_log_lines(dataframe_tmp)
        dataframe_tmp = dataframe_tmp.withColumn(SCENARIO_NAME, lit(scenario_name))

        for transformation in transformations:
            logger.trace('Applying data transformation: {}.', transformation)
            dataframe_tmp = transformation(dataframe_tmp)

        # For the FLST LOG join with the flight intentions
        if FLST_LOG_PREFIX in log_file.stem:
            dataframe_tmp = generate_combined_dataframe(scenario_name, dataframe_tmp, flight_intentions)

        if dataframe:
            dataframe = dataframe.union(dataframe_tmp)
        else:
            dataframe = dataframe_tmp

    return dataframe


if __name__ == '__main__':
    logger.remove()
    logger.add(sys.stderr, level=settings.logging.level)

    path = Path(settings.data_path)
    spark = SparkSession.builder.appName(settings.spark.app_name).getOrCreate()

    fp_intentions_dfs = parse_flight_intentions(spark)
    log_files_dfs = parse_log_files(PARSE_CONFIG, fp_intentions_dfs, spark)

    for log_type, df in log_files_dfs.items():
        logger.info('Showing dataframe for {} logs', log_type)
        df.show()
