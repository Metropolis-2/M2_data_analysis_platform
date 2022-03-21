import sys
from pathlib import Path
from typing import List

from loguru import logger
from pyarrow import StructType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from config import settings
from parse.flst_log_parser import parse_flst_log
from parse.fp_int_parser import parse_fp_int
from parse.parser_constants import FLST_LOG_PREFIX
from parse.parser_utils import build_scenario_name, remove_commented_log_lines, get_fp_int_name_from_scenario_name
from schemas.tables_attributes import SCENARIO_NAME, ACID

# Configuration for the log names with the schema associated and the transformations
# required after the read of the log file. The REGLOG is not included as it has to be
# parsed differently.
# Prefix: (schema, transformations)
PARSE_CONFIG = {
    # CONF_LOG_PREFIX: (CONF_LOG_FILE_SCHEMA, CONF_LOG_TRANSFORMATIONS),
    # LOS_LOG_PREFIX: (LOS_LOG_FILE_SCHEMA, LOS_LOG_TRANSFORMATIONS),
    # GEO_LOG_PREFIX: (GEO_LOG_FILE_SCHEMA, GEO_LOG_TRANSFORMATIONS),
    # FLST_LOG_PREFIX: (FLST_LOG_FILE_SCHEMA, FLST_LOG_TRANSFORMATIONS),
}


def build_flst_log_fp_int_combined_dataframe(spark: SparkSession) -> DataFrame:
    dataframe = None

    log_files = list(path.rglob(f"{FLST_LOG_PREFIX}*"))
    for log_file in log_files:
        logger.trace('Reading file: `{}`.', log_file)

        # Parse FLST LOG and Flight intention
        flst_log_dataframe = parse_flst_log(spark, log_file)

        scenario_name = build_scenario_name(log_file)
        fp_int_name = get_fp_int_name_from_scenario_name(scenario_name)
        fp_int_dataframe = parse_fp_int(spark, fp_int_name)

        # Join flst log with flight plan using the ship id
        dataframe = flst_log_dataframe.join(fp_int_dataframe,
                                            on=ACID,
                                            how='outer')

        if dataframe:
            dataframe = dataframe.union(flst_log_dataframe)
        else:
            dataframe = flst_log_dataframe

    return dataframe


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
    logger.add(sys.stderr, level=settings.logging.level)

    path = Path(settings.data_path)
    spark = SparkSession.builder.appName(settings.spark.app_name).getOrCreate()

    dataframe = build_flst_log_fp_int_combined_dataframe(spark)

    dataframes = dict()

    for log_type_config in PARSE_CONFIG.items():
        log_prefix, (schema, transformations) = log_type_config
        logger.info('Processing {} log files.', log_prefix)
        log_list = list(path.rglob(f"{log_prefix}*.log"))
        dataframe = build_dataframe(spark, log_list, schema, transformations)
        dataframes[log_prefix] = dataframe

    pass
