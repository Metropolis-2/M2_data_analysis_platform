import sys
from pathlib import Path
from typing import List

from loguru import logger
from pyarrow import StructType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from parse.conf_log_parser import CONF_LOG_TRANSFORMATIONS
from parse.geo_log_parser import GEO_LOG_TRANSFORMATIONS
from parse.los_log_parser import LOS_LOG_TRANSFORMATIONS
from parse.parser_constants import LOS_LOG_PREFIX, CONF_LOG_PREFIX, GEO_LOG_PREFIX
from parse.parser_utils import build_scenario_name, remove_commented_log_lines
from schemas.conf_log_schema import CONF_LOG_FILE_SCHEMA
from schemas.geo_log_schema import GEO_LOG_FILE_SCHEMA
from schemas.los_log_schema import LOS_LOG_FILE_SCHEMA
from schemas.tables_attributes import SCENARIO_NAME

# Configuration for the log names with the schema associated and the transformations
# required after the read of the log file. The REGLOG is not included as it has to be
# parsed differently.
# Prefix: (schema, transformations)
PARSE_CONFIG = {
    CONF_LOG_PREFIX: (CONF_LOG_FILE_SCHEMA, CONF_LOG_TRANSFORMATIONS),
    LOS_LOG_PREFIX: (LOS_LOG_FILE_SCHEMA, LOS_LOG_TRANSFORMATIONS),
    GEO_LOG_PREFIX: (GEO_LOG_FILE_SCHEMA, GEO_LOG_TRANSFORMATIONS),
}


def build_dataframe(spark: SparkSession,
                    log_files: List[Path],
                    schema: StructType,
                    transformations: List) -> DataFrame:
    """ Generates a dataframe from all the log files of the same type.
    This method reads the file directly using the schema indicated and
    performs the transformations to add, transform or removed the desired
    columns.

    This method adds the scenario name for each of the log files.

    :param spark: Spark session of the execution.
    :param log_files: list of log files paths of the same type.
    :param schema: file schema of the log files to read.
    :param transformations: set of functions that perform a
     transformation in the dataframe.
    :return: final dataframe.
    """
    dataframe = None

    for log_file in log_files:
        logger.trace('Reading file: `{}`.', log_file)
        scenario_name = build_scenario_name(log_file)
        dataframe_tmp = spark.read.csv(str(log_file), header=False, schema=schema)
        dataframe_tmp = remove_commented_log_lines(dataframe_tmp)
        dataframe_tmp = dataframe_tmp.withColumn(SCENARIO_NAME, lit(scenario_name))

        for transformation in transformations:
            logger.trace('Applying data transformation: {}.', transformation)
            dataframe_tmp = transformation(dataframe_tmp)

        if dataframe:
            dataframe = dataframe.union(dataframe_tmp)
        else:
            dataframe = dataframe_tmp

    return dataframe


if __name__ == '__main__':
    logger.remove()
    logger.add(sys.stderr, level="TRACE")

    path = Path("C:\\Users\\arodgril\\Repos\\Metropolis\\M2_data_analysis_platform\\platform_code\\example_logs\\input")
    spark = SparkSession.builder.appName('Platform Analysis.com').getOrCreate()

    dataframes = dict()

    for log_type_config in PARSE_CONFIG.items():
        log_prefix, (schema, transformations) = log_type_config
        logger.info('Processing {} log files.', log_prefix)
        log_list = list(path.rglob(f"{log_prefix}*.log"))
        dataframe = build_dataframe(spark, log_list, schema, transformations)
        dataframes[log_prefix] = dataframe

    pass
