import sys
from pathlib import Path

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from tqdm import tqdm

from metrics.AEQ_metrics import compute_accessibility_and_equality_metrics
from metrics.CAP_metrics import compute_capacity_metrics
from metrics.EFF_metrics import compute_efficiency_metrics
from metrics.ENV_metrics import compute_environment_metrics
from metrics.PRI_metrics import compute_priority_metrics
from metrics.SAF_metrics import compute_safety_metrics
from parse.conf_log_parser import CONF_LOG_TRANSFORMATIONS
from parse.flst_log_parser import FLST_LOG_TRANSFORMATIONS
from parse.generic_parser import parse_flight_intentions, parse_log_files
from parse.geo_log_parser import GEO_LOG_TRANSFORMATIONS
from parse.los_log_parser import LOS_LOG_TRANSFORMATIONS
from parse.parser_constants import (CONF_LOG_PREFIX, GEO_LOG_PREFIX, LOS_LOG_PREFIX,
                                    REG_LOG_PREFIX, FLST_LOG_PREFIX)
from parse.reg_log_parser import REG_LOG_TRANSFORMATIONS
from schemas.conf_log_schema import CONF_LOG_FILE_SCHEMA
from schemas.flst_log_schema import FLST_LOG_FILE_SCHEMA
from schemas.geo_log_schema import GEO_LOG_FILE_SCHEMA
from schemas.los_log_schema import LOS_LOG_FILE_SCHEMA
from schemas.reg_log_schema import REG_LOG_SCHEMA
from schemas.tables_attributes import SIMT_VALID
from utils.config import settings
from utils.io_utils import load_dataframes, save_dataframes_dict

# Configuration for the log names with the schema associated and the transformations
# required after the read of the log file.
# Prefix: (schema, transformations)
PARSE_CONFIG = {
    CONF_LOG_PREFIX: (CONF_LOG_FILE_SCHEMA, CONF_LOG_TRANSFORMATIONS),
    LOS_LOG_PREFIX: (LOS_LOG_FILE_SCHEMA, LOS_LOG_TRANSFORMATIONS),
    GEO_LOG_PREFIX: (GEO_LOG_FILE_SCHEMA, GEO_LOG_TRANSFORMATIONS),
    REG_LOG_PREFIX: (REG_LOG_SCHEMA, REG_LOG_TRANSFORMATIONS),
    FLST_LOG_PREFIX: (FLST_LOG_FILE_SCHEMA, FLST_LOG_TRANSFORMATIONS),
}
DATAFRAMES_NAMES = [CONF_LOG_PREFIX, FLST_LOG_PREFIX, GEO_LOG_PREFIX, LOS_LOG_PREFIX, REG_LOG_PREFIX]

PROCESS_NEW_FILES = True

if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stderr, level=settings.logging.level)
    if settings.logging.save.file:
        logger.add(settings.logging.save.path, rotation='10 MB', level=settings.logging.level)
    
    spark = SparkSession.builder \
        .appName(settings.spark.app_name) \
        .getOrCreate()

    if PROCESS_NEW_FILES:
        logger.info('The log files in folder `{}` will be processed.', Path(settings.data_path).resolve())
        fp_intentions_dfs = parse_flight_intentions(spark)
        input_dataframes = parse_log_files(PARSE_CONFIG, fp_intentions_dfs, spark)
    else:
        logger.info('The preprocessed files in folder `{}` will be loaded.', Path(settings.saving_path).resolve())
        input_dataframes = load_dataframes(DATAFRAMES_NAMES, spark)

    for key, value in input_dataframes.items():
        if(not key == FLST_LOG_PREFIX):
            input_dataframes[key] = value.where(SIMT_VALID)



    # results = dict()
    # results = compute_safety_metrics(input_dataframes, results)
    # results = compute_accessibility_and_equality_metrics(input_dataframes, results)
    # results = compute_priority_metrics(input_dataframes, results)
    # results = compute_efficiency_metrics(input_dataframes, results)
    # results = compute_capacity_metrics(input_dataframes, results)
    # results = compute_environment_metrics(input_dataframes, results)
    #
    # save_results_dataframes(results)
    # for metric_name, metric_results in tqdm(results.items()):
    #     metric_results.show()
