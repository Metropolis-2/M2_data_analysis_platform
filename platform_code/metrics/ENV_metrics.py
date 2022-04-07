from typing import Dict

from loguru import logger
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import sum, col, lit, lag, max, mean, min, round, count_distinct

from parse.parser_constants import REG_LOG_PREFIX, FLST_LOG_PREFIX
from results.result_dataframes import build_result_df_by_scenario
from results.results_constants import ENV_METRICS_RESULTS, ENV1, ENV2, ENV3, ENV4
from schemas.tables_attributes import (SCENARIO_NAME, SIMULATION_TIME, LATITUDE,
                                       LONGITUDE, ALTITUDE, WORK_DONE,
                                       ACID)
from utils.config import settings
from utils.parser_utils import get_coordinates_distance, load_interest_points

DIFF_LENGTH = 'Diff_length'
DISTANCE = 'Distance'
MAX_LENGTH = 'Max_length'
MIN_LENGTH = 'Min_length'
NEXT_ALTITUDE = 'Next_altitude'
NEXT_LATITUDE = 'Next_latitude'
NEXT_LONGITUDE = 'Next_longitude'
NORM_LENGTH = 'Norm_length'
NUM_LEVELS = 'Num_levels'
SEGMENT_ALTITUDE = 'Segment_altitude'
SEGMENT_LENGTH = 'Segment_length'
SEGMENT_WEIGHT = 'Segment_weight'
SOUND_INTENSITY = 'Sound_intensity'
TOTAL_LENGTH = 'Total_length'


@logger.catch
def generate_segment_dataframe(input_dataframes: Dict[str, DataFrame]) -> DataFrame:
    """ Generates a dataframe with the length and altitude information of the
    segments flighted by the drones, obtained from the REGLOG.
    Required to calculate the ENV-2 and ENV-4.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :return: dataframe with information about the
    """
    dataframe = input_dataframes[REG_LOG_PREFIX]

    # Window required to obtain the next position of the drone flight
    window = Window \
        .partitionBy(SCENARIO_NAME, ACID) \
        .orderBy(SIMULATION_TIME)

    # Generation of the next position information using the defined window
    segment_dataframe = dataframe \
        .withColumn(NEXT_LATITUDE, lag(LATITUDE, -1).over(window)) \
        .withColumn(NEXT_LONGITUDE, lag(LONGITUDE, -1).over(window)) \
        .withColumn(NEXT_ALTITUDE, lag(ALTITUDE, -1).over(window))

    # Remove rows with NEXT_LATITUDE and NEXT_LONGITUDE null
    # (they are the rows of separation between scenarios)
    segment_dataframe = segment_dataframe.na.drop(subset=[NEXT_LATITUDE,
                                                          NEXT_LONGITUDE,
                                                          NEXT_ALTITUDE])

    # Calculates the length and altitude of the segment.
    # Using the mean if the altitude between the two points is different
    # !! Using rounding in SEGMENT_ALTITUDE to reduce the number of altitudes
    return segment_dataframe \
        .withColumn(SEGMENT_LENGTH,
                    get_coordinates_distance(LATITUDE, LONGITUDE, NEXT_LATITUDE, NEXT_LONGITUDE)) \
        .withColumn(SEGMENT_ALTITUDE, round((col(ALTITUDE) + col(NEXT_ALTITUDE)) / 2, 1)) \
        .select(SCENARIO_NAME, ACID, SEGMENT_LENGTH, SEGMENT_ALTITUDE)


@logger.catch
def compute_env1_metric(input_dataframes: Dict[str, DataFrame], *args, **kwargs) -> DataFrame:
    """ ENV-1: Work done

    Representing total energy needed to perform all flight intentions,
    computed by integrating the thrust (force) over the route displacement.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :return: query result with the ENV-1 metric per scenario.
    """
    dataframe = input_dataframes[FLST_LOG_PREFIX]
    return dataframe \
        .select(SCENARIO_NAME, WORK_DONE) \
        .groupby(SCENARIO_NAME) \
        .agg(sum(WORK_DONE).alias(ENV1))


@logger.catch
def compute_env2_metric(segment_dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ ENV-2: Weighted average altitude

    Average flight level weighed by the length flown at each flight level.
    This metric departs from the dataframe with the segments.

    :param segment_dataframe: segment information dataframe.
    :return: query result with the ENV-2 metric per scenario.
    """
    return segment_dataframe \
        .withColumn(SEGMENT_WEIGHT, col(SEGMENT_ALTITUDE) * col(SEGMENT_LENGTH)) \
        .groupby(SCENARIO_NAME).agg(sum(col(SEGMENT_WEIGHT)).alias(SEGMENT_WEIGHT),
                                    sum(col(SEGMENT_LENGTH)).alias(SEGMENT_LENGTH)) \
        .withColumn(ENV2, col(SEGMENT_WEIGHT) / col(SEGMENT_LENGTH)) \
        .select(SCENARIO_NAME, ENV2)


@logger.catch
def compute_env3_metric(input_dataframes: Dict[str, DataFrame], *args, **kwargs) -> DataFrame:
    """ ENV-3: Equivalent Noise Level

    Represent total sound exposure at the given point on city area surface.
    It is computed by aggregating the total sound intensity (of all sound sources)
    at that given point over the time.

    :param input_dataframes: dictionary with the dataframes from the log files.
    :return query result with the ENV-3 metric per scenario.
    """
    dataframe = input_dataframes[REG_LOG_PREFIX]
    result_env3 = build_result_df_by_scenario(input_dataframes)
    interest_points = load_interest_points(settings.interest_points)

    for name, (latitude, longitude, point_type) in interest_points.items():
        # TODO: Check about the time to calculate the query
        # Steps:
        # 1. Remove those rows that do not match in time
        # 2. Calculate the distance of the drone to the interest point
        # 3. Remove those rows further than the radius of interest
        # 4. Calculate the sound intensity using the reference altitude from settings
        # 5. Sum all the sound intensities
        aux_dataframe = dataframe \
            .where(col(SIMULATION_TIME) == settings.env3.time_roi) \
            .withColumn(DISTANCE,
                        get_coordinates_distance(col(LATITUDE), col(LONGITUDE), lit(latitude), lit(longitude))) \
            .where((col(DISTANCE) <= settings.env3.distance_to_point)) \
            .withColumn(SOUND_INTENSITY, 1 / (pow((col(DISTANCE) / settings.env3.reference_altitude), 2))) \
            .groupby(SCENARIO_NAME).agg(sum(SOUND_INTENSITY).alias(f"{ENV3}_P{name}"))

        # The join is to the left to keep all the scenarios from first iteration
        result_env3 = result_env3.join(aux_dataframe, on=SCENARIO_NAME, how='left')

        # TODO: Remove to calculate more than 10 points
        if name == '10':
            break

    return result_env3


@logger.catch
def compute_env4_metric(segment_dataframe: DataFrame, *args, **kwargs) -> DataFrame:
    """ ENV-4: Altitude dispersion

    The ratio between the difference of maximum and minimum length
    flown at a flight level and average length flown at level.

    :param segment_dataframe: segment information dataframe.
    :return: query result with the ENV-2 metric per scenario.
    """
    dataframe = segment_dataframe \
        .withColumn(SEGMENT_ALTITUDE, col(SEGMENT_ALTITUDE)) \
        .groupby(SCENARIO_NAME, SEGMENT_ALTITUDE) \
        .agg(max(SEGMENT_LENGTH).alias(MAX_LENGTH),
             min(SEGMENT_LENGTH).alias(MIN_LENGTH),
             sum(SEGMENT_LENGTH).alias(TOTAL_LENGTH))

    # To calculate the number of flight altitudes, count the number of
    # different flight levels
    num_levels = dataframe \
        .groupby(SCENARIO_NAME) \
        .agg(count_distinct(SEGMENT_ALTITUDE).alias(NUM_LEVELS))

    return dataframe.join(num_levels, on=SCENARIO_NAME) \
        .withColumn(DIFF_LENGTH, col(MAX_LENGTH) - col(MIN_LENGTH)) \
        .withColumn(NORM_LENGTH, col(TOTAL_LENGTH) / col(NUM_LEVELS)) \
        .select(SCENARIO_NAME, SEGMENT_ALTITUDE, DIFF_LENGTH, NORM_LENGTH) \
        .withColumn(ENV4, col(DIFF_LENGTH) / col(NORM_LENGTH)) \
        .select(SCENARIO_NAME, ENV4) \
        .groupby(SCENARIO_NAME) \
        .agg(mean(ENV4).alias(ENV4))


ENV_METRICS = [
    compute_env1_metric,
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
    logger.info('Generating plan for environment metrics.')
    result_dataframe = build_result_df_by_scenario(input_dataframes)
    segment_dataframe = generate_segment_dataframe(input_dataframes)

    for metric in ENV_METRICS:
        logger.trace('Generating plan for metric: {}.', metric)
        query_result = metric(input_dataframes=input_dataframes,
                              segment_dataframe=segment_dataframe,
                              result_dataframe=result_dataframe)
        result_dataframe = result_dataframe.join(query_result,
                                                 on=SCENARIO_NAME,
                                                 how='left')

    output_dataframes[ENV_METRICS_RESULTS] = result_dataframe
    return output_dataframes
