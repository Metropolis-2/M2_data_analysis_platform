from typing import Dict

from loguru import logger
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import sum, col, mean, log10, struct, lit, max, min, lag

from parse.parser_constants import REG_LOG_PREFIX, FLST_LOG_PREFIX
from results.result_dataframes import build_result_df_by_scenario
from results.results_constants import ENV_METRICS_RESULTS
from schemas.tables_attributes import (SCENARIO_NAME, ACID, SIMULATION_TIME, LATITUDE, LONGITUDE, ALTITUDE, ENV2, ENV4,
                                       ENV3, WORK_DONE, ENV1)
from utils.config import settings
from utils.parser_utils import get_coordinates_distance, great_circle_udf, loadEnv3points
from os.path import exists


@logger.catch
def compute_env1_metric(input_dataframes: Dict[str, DataFrame], *args, **kwargs):
    """ ENV-1: ->DataFrameWork done

    Representing total energy needed to perform all flight intentions,
    computed by integrating the thrust (force) over the route displacement.
    The indicator is directly computed in the Bluesky simulator.

    :param dataframe: data required to calculate the metrics.
    :return:
    """
    dataframe = input_dataframes[FLST_LOG_PREFIX]
    return dataframe.select(SCENARIO_NAME, WORK_DONE).groupby(SCENARIO_NAME).agg(sum(WORK_DONE).alias(ENV1))

@logger.catch
def compute_env2_metric(input_dataframes: Dict[str, DataFrame], *args, **kwargs):
    """ ENV-2: Weighted average->DataFrame altitude

    Average flight level weighed by the length flown at each flight level.

    :param dataframe: data required to calculate the metrics.
    :return:
    """
    dataframe = input_dataframes[REG_LOG_PREFIX]
    # compute next latitude and longitude foreach row
    window = Window.partitionBy(SCENARIO_NAME).orderBy(SIMULATION_TIME)
    dataframe = dataframe \
        .withColumn("NEXT_LATITUDE", lag(LATITUDE, -1).over(window)) \
        .withColumn("NEXT_LONGITUDE", lag(LONGITUDE, -1).over(window)) \
        .withColumn("NEXT_ALTITUDE", lag(ALTITUDE, -1).over(window))
    # Remove rows with NEXT_LATITUDE and NEXT_LONGITUDE null (they are the rows of separation between scenarios)
    dataframe = dataframe.na.drop(subset=["NEXT_LATITUDE", "NEXT_LONGITUDE", "NEXT_ALTITUDE"])
    # # Check it
    # dataframe.filter(col("NEXT_LATITUDE").isNull() | col("NEXT_LATITUDE").isNull()).show()
    dataframe = dataframe.withColumn("SEGMENT_LENGTH",
                                     get_coordinates_distance(LATITUDE, LONGITUDE, "NEXT_LATITUDE", "NEXT_LONGITUDE"))\
                            .withColumn("SEGMENT_ALTITUDE", (col(ALTITUDE)+col("NEXT_ALTITUDE"))/2)

    dataframe = dataframe.withColumn("SEGMENT_WEIGHT", col("SEGMENT_ALTITUDE") * col("SEGMENT_LENGTH"))
    dataframe = dataframe.groupby(SCENARIO_NAME).agg(sum(col("SEGMENT_WEIGHT")), sum(col("SEGMENT_LENGTH")))\
        .withColumn(ENV2,col("sum(SEGMENT_WEIGHT)") / col("sum(SEGMENT_LENGTH)")).select(SCENARIO_NAME, ENV2)
    return dataframe


@logger.catch
def compute_env3_metric(input_dataframes: Dict[str, DataFrame], *args, **kwargs) -> DataFrame:
    """ ENV-3: Equivalent Noise Level

    Represent total sound exposure at the given point on city area surface.
    It is computed by aggregating the total sound intensity (of all sound sources)
    at that given point over the time.
    """
    dataframe = input_dataframes[REG_LOG_PREFIX]
    df_points = loadEnv3points(settings.geojson.path)


    for i, (x, y) in enumerate(zip(df_points.geometry.x, df_points.geometry.y)):
        point = struct(lit(y), lit(x))
        aux_dataframe = dataframe.filter(col(SIMULATION_TIME) == settings.env3.time_roi)
        aux_dataframe = aux_dataframe.withColumn("distance",great_circle_udf(point, struct(col(LATITUDE), col(LONGITUDE))))
        aux_dataframe = aux_dataframe.filter((col("distance") <= 16))
        aux_dataframe = aux_dataframe.withColumn("sound_intensity",1 / (pow((col("distance") / settings.flight_altitude.lowest), 2)))
        aux_dataframe = aux_dataframe.groupby(SCENARIO_NAME).agg(sum("sound_intensity").alias(f"ENV3_p{i}"))

        if (i == 0):
            final_dataframe = aux_dataframe
        else:
            final_dataframe = final_dataframe.join(aux_dataframe, SCENARIO_NAME)


    return final_dataframe


@logger.catch
def compute_env4_metric(input_dataframes: Dict[str, DataFrame], *args, **kwargs) -> DataFrame:
    """ ENV-4: Altitude dispersion

    The ratio between the difference of maximum and minimum length
    flown at a flight level and average length flown at level.
    """
    """
    # TODO: ? Define flight levels / altitude discretization
    avg_alt = input_dataframes \
        .groupby(SCENARIO_NAME, ACID) \
        .agg(max(ALTITUDE).alias("MAX_ALTITUDE"), min(ALTITUDE).alias("MIN_ALTITUDE")) \
        .withColumn("DIFF_ALTITUDE", col("MAX_ALTITUDE") - col("MIN_ALTITUDE")) \
        .select(SCENARIO_NAME, "DIFF_ALTITUDE") \
        .groupby(SCENARIO_NAME) \
        .agg(mean("DIFF_ALTITUDE").alias("MEAN_DIFF_ALTITUDE"))

    return input_dataframes.join(avg_alt, how='outer') \
        .withColumn(ENV4, col("DIFF_ALTITUDE") / col("MEAN_DIFF_ALTITUDE")) \
        .select(SCENARIO_NAME, ACID, ENV4)
    """


ENV_METRICS = [
    # compute_env1_metric
    # compute_env2_metric,
    compute_env3_metric
    # compute_env4_metric
]


def compute_environment_metrics(input_dataframes: Dict[str, DataFrame],
                                output_dataframes: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """ Calculates all the security metrics and add to the output dataframes dictionary
    their results.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :param output_dataframes: dictionary with the dataframes where the results are saved.
    :return: updated results dataframes with the security metrics.
    """
    result_dataframe = build_result_df_by_scenario(input_dataframes)

    for metric in ENV_METRICS:
        logger.trace('Calculating metric: {}.', metric)
        query_result = metric(input_dataframes=input_dataframes)
        result_dataframe = result_dataframe.join(query_result,
                                                 on=SCENARIO_NAME,
                                                 how='left')

    output_dataframes[ENV_METRICS_RESULTS] = result_dataframe
    return output_dataframes
