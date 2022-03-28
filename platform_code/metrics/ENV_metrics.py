from typing import Dict

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, col, mean, log10, struct, lit, max, min, lag

from parse.parser_constants import REG_LOG_PREFIX
from results.result_dataframes import build_result_df_by_scenario
from results.results_constants import ENV_METRICS_RESULTS
from schemas.tables_attributes import (SCENARIO_NAME, ACID, SIMULATION_TIME, LATITUDE, LONGITUDE, ALTITUDE, ENV2, ENV4,
                                       ENV3)
from utils.config import settings
from utils.parser_utils import get_coordinates_distance, great_circle_udf


@logger.catch
def compute_env1_metric(dataframe: DataFrame, *args, **kwargs):
    """ ENV-1: ->DataFrameWork done

    Representing total energy needed to perform all flight intentions,
    computed by integrating the thrust (force) over the route displacement.
    The indicator is directly computed in the Bluesky simulator.

    :param dataframe: data required to calculate the metrics.
    :return:
    """
    pass


@logger.catch
def compute_env2_metric(dataframe: DataFrame, *args, **kwargs):
    """ ENV-2: Weighted average->DataFrame altitude

    Average flight level weighed by the length flown at each flight level.

    :param dataframe: data required to calculate the metrics.
    :return:
    """
    # compute next latitude and longitude foreach row
    window = Window.partitionBy(SCENARIO_NAME).orderBy(SIMULATION_TIME)
    dataframe = dataframe \
        .withColumn("NEXT_LATITUDE", lag(LATITUDE, -1).over(window)) \
        .withColumn("NEXT_LONGITUDE", lag(LONGITUDE, -1).over(window))
    # Remove rows with NEXT_LATITUDE and NEXT_LONGITUDE null (they are the rows of separation between scenarios)
    dataframe = dataframe.na.drop(subset=["NEXT_LATITUDE", "NEXT_LONGITUDE"])
    # # Check it
    # dataframe.filter(col("NEXT_LATITUDE").isNull() | col("NEXT_LATITUDE").isNull()).show()
    dataframe = dataframe.withColumn("DIST_NEXT_POINT",
                                     get_coordinates_distance(LATITUDE, LONGITUDE, "NEXT_LATITUDE", "NEXT_LONGITUDE"))
    dataframe = dataframe.withColumn("WEIGHT_SEGMENT", col(ALTITUDE) * col("DIST_NEXT_POINT"))

    dataframe = dataframe.groupby(SCENARIO_NAME, ACID).agg(sum(col("WEIGHT_SEGMENT")).alias("FP_ENV2"))
    # TODO: ? Check formula, if mean is needed or just divide por el segment length
    # Calculate average per scenario
    return dataframe.groupBy(SCENARIO_NAME).agg(mean("FP_ENV2").alias(ENV2))


@logger.catch
def compute_env3_metric(dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ ENV-3: Equivalent Noise Level

    Represent total sound exposure at the given point on city area surface.
    It is computed by aggregating the total sound intensity (of all sound sources)
    at that given point over the time.
    """
    point = struct(lit(settings.x_center), lit(settings.y_center))

    # TODO: ? Define formula for the sound depending on the distance to the point.
    # TODO: ? How many points and how to define.
    return dataframe.filter(col(SIMULATION_TIME) == settings.time_roi) \
        .withColumn("distance", great_circle_udf(point, struct(col(LATITUDE), col(LONGITUDE)))) \
        .filter((col("distance") <= settings.radius_roi) & (col(ALTITUDE) <= settings.altitude_roi)) \
        .withColumn("noise_level", log10(1 / pow(col("distance"), 2))) \
        .groupBy(SCENARIO_NAME).agg(sum("noise_level").alias(ENV3))


@logger.catch
def compute_env4_metric(dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ ENV-4: Altitude dispersion

    The ratio between the difference of maximum and minimum length
    flown at a flight level and average length flown at level.
    """
    # TODO: ? Define flight levels / altitude discretization
    avg_alt = dataframe \
        .groupby(SCENARIO_NAME, ACID) \
        .agg(max(ALTITUDE).alias("MAX_ALTITUDE"), min(ALTITUDE).alias("MIN_ALTITUDE")) \
        .withColumn("DIFF_ALTITUDE", col("MAX_ALTITUDE") - col("MIN_ALTITUDE")) \
        .select(SCENARIO_NAME, "DIFF_ALTITUDE") \
        .groupby(SCENARIO_NAME) \
        .agg(mean("DIFF_ALTITUDE").alias("MEAN_DIFF_ALTITUDE"))

    return dataframe.join(avg_alt, how='outer') \
        .withColumn(ENV4, col("DIFF_ALTITUDE") / col("MEAN_DIFF_ALTITUDE")) \
        .select(SCENARIO_NAME, ACID, ENV4)


AEQ_METRICS = [
    compute_env2_metric,
    compute_env3_metric,
    compute_env4_metric
]


def compute_environment_metrics(input_dataframes: Dict[str, DataFrame],
                                output_dataframes: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """ Calculates all the security metrics and add to the output dataframes dictionary
    their results.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :param output_dataframes: dictionary with the dataframes where the results are saved.
    :return: updated results dataframes with the security metrics.
    """
    dataframe = input_dataframes[REG_LOG_PREFIX]
    result_dataframe = build_result_df_by_scenario(input_dataframes)

    for metric in AEQ_METRICS:
        logger.trace('Calculating metric: {}.', metric)
        query_result = metric(dataframe=dataframe)
        result_dataframe = result_dataframe.join(query_result,
                                                 on=SCENARIO_NAME,
                                                 how='left')

    output_dataframes[ENV_METRICS_RESULTS] = result_dataframe
    return output_dataframes
