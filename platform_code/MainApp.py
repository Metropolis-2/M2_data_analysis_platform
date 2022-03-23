import sys

from loguru import logger
from pyspark.sql import SparkSession

from PRI_metrics import compute_PRI1_metric, compute_PRI2_metric, compute_PRI3_metric, compute_PRI4_metric, \
    compute_PRI5_metric
from SAF_metrics import *
from config import settings
from parse.flst_log_parser import FLST_LOG_TRANSFORMATIONS
from parse.generic_parser import parse_flight_intentions, parse_log_files
from parse.reg_log_parser import REG_LOG_TRANSFORMATIONS
from platform_code.parse.conf_log_parser import CONF_LOG_TRANSFORMATIONS
from platform_code.parse.geo_log_parser import GEO_LOG_TRANSFORMATIONS
from platform_code.parse.los_log_parser import LOS_LOG_TRANSFORMATIONS
from platform_code.parse.parser_constants import (CONF_LOG_PREFIX, GEO_LOG_PREFIX, LOS_LOG_PREFIX,
                                                  REG_LOG_PREFIX, FLST_LOG_PREFIX)
from platform_code.schemas.conf_log_schema import CONF_LOG_FILE_SCHEMA
from platform_code.schemas.geo_log_schema import GEO_LOG_FILE_SCHEMA
from platform_code.schemas.los_log_schema import LOS_LOG_FILE_SCHEMA
from results.result_dataframes import build_results_dataframes
from schemas.flst_log_schema import FLST_LOG_FILE_SCHEMA
from schemas.reg_log_schema import REG_LOG_SCHEMA

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


def calculate_AEQ_metrics():
    compute_AEQ1_metric(input_dataframes, output_dataframes)
    compute_AEQ1_1_metric(input_dataframes, output_dataframes)
    compute_AEQ2_metric(input_dataframes, output_dataframes)
    compute_AEQ2_1_metric(input_dataframes, output_dataframes)
    compute_AEQ3_metric(input_dataframes, output_dataframes)
    compute_AEQ4_metric(input_dataframes, output_dataframes)
    compute_AEQ5_metric(input_dataframes, output_dataframes)
    compute_AEQ5_1_metric(input_dataframes, output_dataframes)


def calculate_CAP_metrics():
    compute_CAP1_metric(input_dataframes, output_dataframes)
    compute_CAP2_metric(input_dataframes, output_dataframes)
    compute_CAP3_metric(input_dataframes, output_dataframes)
    compute_CAP4_metric(input_dataframes, output_dataframes)


def calculate_EFF_metrics():
    compute_EFF1_metric(input_dataframes, output_dataframes)
    compute_EFF2_metric(input_dataframes, output_dataframes)
    compute_EFF3_metric(input_dataframes, output_dataframes)
    compute_EFF4_metric(input_dataframes, output_dataframes)
    compute_EFF5_metric(input_dataframes, output_dataframes)
    compute_EFF6_metric(input_dataframes, output_dataframes)


def calculate_ENV_metrics():
    compute_ENV1_metric(input_dataframes, output_dataframes)
    compute_ENV2_metric(input_dataframes, output_dataframes)
    compute_ENV3_metric(input_dataframes, output_dataframes)
    compute_ENV4_metric(input_dataframes, output_dataframes)


def calculate_SAF_metrics():
    output_dataframes["OUTPUT"] = compute_SAF1_metric(input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"] = compute_SAF2_metric(input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"] = compute_SAF3_metric(input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"] = compute_SAF4_metric(input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"] = compute_SAF5_metric(input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"] = compute_SAF6_metric(input_dataframes, output_dataframes)


def calculate_PRI_metrics():
    output_dataframes["OUTPUT"] = compute_PRI1_metric(input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"] = compute_PRI2_metric(input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"] = compute_PRI3_metric(input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"] = compute_PRI4_metric(input_dataframes, output_dataframes)
    output_dataframes["OUTPUT"] = compute_PRI5_metric(input_dataframes, output_dataframes)


def calculate_ALL_metrics():
    calculate_AEQ_metrics()
    calculate_CAP_metrics()
    calculate_EFF_metrics()
    calculate_ENV_metrics()
    calculate_SAF_metrics()
    calculate_PRI_metrics()


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stderr, level=settings.logging.level)

    spark = SparkSession.builder.appName(settings.spark.app_name).getOrCreate()

    fp_intentions_dfs = parse_flight_intentions(spark)
    input_dataframes = parse_log_files(PARSE_CONFIG, fp_intentions_dfs, spark)
    output_dataframes = build_results_dataframes(input_dataframes)

    calculate_SAF_metrics()
    calculate_PRI_metrics()
    output_dataframes["OUTPUT"].show()
