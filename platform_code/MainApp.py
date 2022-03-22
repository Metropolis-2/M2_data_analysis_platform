import sys
from pathlib import Path
from typing import List

from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from pyarrow import StructType
from pyspark.sql.types import StructField, StringType

from AEQ_metrics import *
from CAP_metrics import *
from EFF_metrics import *
from ENV_metrics import *
from SAF_metrics import *
from PRI_metrics import *
from parse.parser_utils import build_scenario_name, remove_commented_log_lines
from platform_code.parse.conf_log_parser import CONF_LOG_TRANSFORMATIONS
from platform_code.parse.geo_log_parser import GEO_LOG_TRANSFORMATIONS
from platform_code.parse.los_log_parser import LOS_LOG_TRANSFORMATIONS
from platform_code.parse.parser_constants import CONF_LOG_PREFIX, GEO_LOG_PREFIX, LOS_LOG_PREFIX
from platform_code.schemas.conf_log_schema import CONF_LOG_FILE_SCHEMA
from platform_code.schemas.geo_log_schema import GEO_LOG_FILE_SCHEMA
from platform_code.schemas.los_log_schema import LOS_LOG_FILE_SCHEMA
from schemas.ouput_schema import OUTPUT_SCHEMA
from schemas.tables_attributes import SCENARIO_NAME

PARSE_CONFIG = {
    CONF_LOG_PREFIX: (CONF_LOG_FILE_SCHEMA, CONF_LOG_TRANSFORMATIONS),
    LOS_LOG_PREFIX: (LOS_LOG_FILE_SCHEMA, LOS_LOG_TRANSFORMATIONS),
    # GEO_LOG_PREFIX: (GEO_LOG_FILE_SCHEMA, GEO_LOG_TRANSFORMATIONS)
}


def build_input_dataframe(spark: SparkSession,
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

    scenario_names = list()
    for log_file in log_files:
        logger.trace('Reading file: `{}`.', log_file)
        scenario_name = build_scenario_name(log_file)
        dataframe_tmp = spark.read.csv(str(log_file), header=False, schema=schema)
        dataframe_tmp = remove_commented_log_lines(dataframe_tmp)
        dataframe_tmp = dataframe_tmp.withColumn(SCENARIO_NAME, lit(scenario_name))
        scenario_names.append(scenario_name)

        for transformation in transformations:
            logger.trace('Applying data transformation: {}.', transformation)
            dataframe_tmp = transformation(dataframe_tmp)

        if dataframe:
            dataframe = dataframe.union(dataframe_tmp)
        else:
            dataframe = dataframe_tmp

    output_dataframes["OUTPUT"] = spark.createDataFrame(scenario_names, "string").toDF(SCENARIO_NAME)
    return dataframe


def build_output_dataframe(spark: SparkSession, schema: StructType) -> DataFrame:
    # Creating an empty RDD to make a DataFrame
    # with no data
    emp_RDD = spark.sparkContext.emptyRDD()
    # Creating an empty DataFrame
    dataframe = spark.createDataFrame(data=emp_RDD,schema=schema)
    return dataframe


def calculate_AEQ_metrics():
    compute_AEQ1_metric (spark, input_dataframes, output_dataframes)
    compute_AEQ1_1_metric (spark, input_dataframes, output_dataframes)
    compute_AEQ2_metric (spark, input_dataframes, output_dataframes)
    compute_AEQ2_1_metric (spark, input_dataframes, output_dataframes)
    compute_AEQ3_metric (spark, input_dataframes, output_dataframes)
    compute_AEQ4_metric (spark, input_dataframes, output_dataframes)
    compute_AEQ5_metric (spark, input_dataframes, output_dataframes)
    compute_AEQ5_1_metric (spark, input_dataframes, output_dataframes)

def calculate_CAP_metrics():
    compute_CAP1_metric (spark, input_dataframes, output_dataframes)
    compute_CAP2_metric (spark, input_dataframes, output_dataframes)
    compute_CAP3_metric (spark, input_dataframes, output_dataframes)
    compute_CAP4_metric (spark, input_dataframes, output_dataframes)


def calculate_EFF_metrics():
    compute_EFF1_metric (spark, input_dataframes, output_dataframes)
    compute_EFF2_metric (spark, input_dataframes, output_dataframes)
    compute_EFF3_metric (spark, input_dataframes, output_dataframes)
    compute_EFF4_metric (spark, input_dataframes, output_dataframes)
    compute_EFF5_metric (spark, input_dataframes, output_dataframes)
    compute_EFF6_metric (spark, input_dataframes, output_dataframes)

def calculate_ENV_metrics():
    compute_ENV1_metric (spark, input_dataframes, output_dataframes)
    compute_ENV2_metric (spark, input_dataframes, output_dataframes)
    compute_ENV3_metric (spark, input_dataframes, output_dataframes)
    compute_ENV4_metric (spark, input_dataframes, output_dataframes)

def calculate_SAF_metrics():
    compute_SAF1_metric (spark, input_dataframes, output_dataframes)
    compute_SAF2_metric (spark, input_dataframes, output_dataframes)
    compute_SAF3_metric (spark, input_dataframes, output_dataframes)
    compute_SAF4_metric (spark, input_dataframes, output_dataframes)
    compute_SAF5_metric (spark, input_dataframes, output_dataframes)
    compute_SAF6_metric (spark, input_dataframes, output_dataframes)

def calculate_PRI_metrics():
    compute_PRI1_metric (spark, input_dataframes, output_dataframes)
    compute_PRI2_metric (spark, input_dataframes, output_dataframes)
    compute_PRI3_metric (spark, input_dataframes, output_dataframes)
    compute_PRI4_metric (spark, input_dataframes, output_dataframes)
    compute_PRI5_metric (spark, input_dataframes, output_dataframes)

def calculate_ALL_metrics():
    calculate_AEQ_metrics()
    calculate_CAP_metrics()
    calculate_EFF_metrics()
    calculate_ENV_metrics()
    calculate_SAF_metrics()
    calculate_PRI_metrics()


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stderr, level="TRACE")

    path = Path("example_logs/input")
    spark = SparkSession.builder.appName('Platform Analysis.com').getOrCreate()

    input_dataframes = {}
    output_dataframes = {}

    #output_dataframes["OUTPUT"] = build_output_dataframe(spark, OUTPUT_SCHEMA)

    for log_type_config in PARSE_CONFIG.items():
        log_prefix, (schema, transformations) = log_type_config
        logger.info('Processing {} log files.', log_prefix)
        log_list = list(path.rglob(f"{log_prefix}*.log"))
        dataframe = build_input_dataframe(spark, log_list, schema, transformations)
        input_dataframes[log_prefix] = dataframe


    output_dataframes["OUTPUT"] = compute_SAF1_metric (spark, input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"] = compute_SAF2_metric(spark, input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"] = compute_SAF3_metric(spark, input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"] = compute_SAF4_metric(spark, input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"].show()