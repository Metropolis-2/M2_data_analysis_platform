import sys

from pyspark.sql import SparkSession

from metrics.AEQ_metrics import compute_accessibility_and_equality_metrics
from metrics.CAP_metrics import compute_capacity_metrics
from metrics.EFF_metrics import compute_efficiency_metrics
from metrics.ENV_metrics import compute_environment_metrics
from metrics.PRI_metrics import compute_priority_metrics
from metrics.SAF_metrics import *
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
from schemas.flst_log_schema import FLST_LOG_FILE_SCHEMA
from schemas.reg_log_schema import REG_LOG_SCHEMA
from utils.config import settings

# Configuration for the log names with the schema associated and the transformations
# required after the read of the log file.
# Prefix: (schema, transformations)
from utils.io_utils import save_dataframes_dict, load_dataframes

PARSE_CONFIG = {
    CONF_LOG_PREFIX: (CONF_LOG_FILE_SCHEMA, CONF_LOG_TRANSFORMATIONS),
    LOS_LOG_PREFIX: (LOS_LOG_FILE_SCHEMA, LOS_LOG_TRANSFORMATIONS),
    GEO_LOG_PREFIX: (GEO_LOG_FILE_SCHEMA, GEO_LOG_TRANSFORMATIONS),
    FLST_LOG_PREFIX: (FLST_LOG_FILE_SCHEMA, FLST_LOG_TRANSFORMATIONS),
    REG_LOG_PREFIX: (REG_LOG_SCHEMA, REG_LOG_TRANSFORMATIONS)
}


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stderr, level=settings.logging.level)

    spark = SparkSession.builder.appName(settings.spark.app_name).getOrCreate()

    # fp_intentions_dfs = parse_flight_intentions(spark)
    # input_dataframes = parse_log_files(PARSE_CONFIG, fp_intentions_dfs, spark)
    # save_dataframes_dict(input_dataframes)

    df_names = [CONF_LOG_PREFIX, FLST_LOG_PREFIX, GEO_LOG_PREFIX, LOS_LOG_PREFIX, REG_LOG_PREFIX]
    input_dataframes = load_dataframes(df_names, spark)

    results = dict()
    # results = compute_security_metrics(input_dataframes, results)
    # results = compute_accessibility_and_equality_metrics(input_dataframes, results)
    # results = compute_priority_metrics(input_dataframes, results)
    # results = compute_efficiency_metrics(input_dataframes, results)
    # results = compute_capacity_metrics(input_dataframes, results)
    results = compute_environment_metrics(input_dataframes, results)

    for metric_name, metric_results in results.items():
        metric_results.show()
