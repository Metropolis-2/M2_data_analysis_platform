import geopy.distance
from pyspark.sql.functions import *
from pyspark.sql import Window
import pyspark.sql.functions as F

from parse.parser_constants import REG_LOG_PREFIX
from schemas.tables_attributes import SCENARIO_NAME, ACID, SIMULATION_TIME, LATITUDE, LONGITUDE, ALTITUDE, ENV2, ENV4
from utils.parser_utils import get_coordinates_distance, great_circle_udf
from config import settings

def compute_ENV1_metric(input_df, output_df): #TODO: PENDING
    '''
    ENV-1: Work done
    (Representing total energy needed to perform all flight intentions, computed by integrating the thrust (force) over the route displacement.
    The indicator is directly computed in the Bluesky simulator)
    '''
    # result = self.flst_log_dataframe.agg({'Work_done': 'sum'}).show()

    df = output_df["OUTPUT"]
    return df

def compute_ENV2_metric(input_df, output_df):
    '''
    ENV-2: Weighted average altitude
    (Average flight level weighed by the length flown at each flight level)
    '''
    df = output_df["OUTPUT"]
    dataframe = input_df[REG_LOG_PREFIX]
    # compute next latitude and longitude foreach row
    window = Window.partitionBy(SCENARIO_NAME).orderBy(SIMULATION_TIME)
    dataframe = dataframe.withColumn("NEXT_LATITUDE", lag(LATITUDE, -1).over(window)).withColumn("NEXT_LONGITUDE",lag(LONGITUDE,-1).over(window))
    # Remove rows with NEXT_LATITUDE and NEXT_LONGITUDE null (they are the rows of separation between scenarios)
    dataframe = dataframe.na.drop(subset=["NEXT_LATITUDE", "NEXT_LONGITUDE"])
    # # Check it
    # dataframe.filter(col("NEXT_LATITUDE").isNull() | col("NEXT_LATITUDE").isNull()).show()
    dataframe = dataframe.withColumn("DIST_NEXT_POINT", get_coordinates_distance(LATITUDE, LONGITUDE, "NEXT_LATITUDE", "NEXT_LONGITUDE"))
    dataframe = dataframe.withColumn("WEIGHT_SEGMENT", col(ALTITUDE) * col("DIST_NEXT_POINT"))
    dataframe = dataframe.groupby(SCENARIO_NAME, ACID).agg(F.sum(col("WEIGHT_SEGMENT")).alias("FP_ENV2"))
    # Calculate average per scenario
    dataframe = dataframe.groupBy(SCENARIO_NAME).agg(mean("FP_ENV2").alias(ENV2))
    df = df.join(dataframe, on=[SCENARIO_NAME], how='outer')
    return df

def compute_ENV3_metric(input_df, output_df, ENV3=None): #TODO: PENDING
    '''
    ENV-3: Equivalent Noise Level
    (Represent total sound exposure at the given point on city area surface.
    It is computed by aggregating the total sound intensity (of all sound sources) at that given point over the time)
    '''
    df = output_df["OUTPUT"]
    dataframe = input_df[REG_LOG_PREFIX].filter(col(SIMULATION_TIME) == settings.time_roi)
    point = struct(lit(settings.x_center), lit(settings.y_center))
    dataframe = dataframe.withColumn("distance", great_circle_udf(point, struct(col(LATITUDE), col(LONGITUDE)))).filter(
        (col("distance") <= settings.radius_roi) &
        (col(ALTITUDE) <= settings.altitude_roi))
    dataframe = dataframe.withColumn("noise_level", log10(1 / pow(col("distance"), 2)))

    dataframe = dataframe.groupBy(SCENARIO_NAME).agg(sum("noise_level").alias(ENV3))
    df = df.join(dataframe, on=[SCENARIO_NAME], how='outer')
    return df

def compute_ENV4_metric(input_df, output_df): #TODO: PENDING
    '''
    ENV-4: Altitude dispersion
    (The ratio between the difference of maximum and minimum length flown at a flight level and average length flown at level)
    '''
    df = output_df["OUTPUT"]
    dataframe = input_df[REG_LOG_PREFIX]
    dataframe = dataframe.groupby(SCENARIO_NAME, ACID).agg(F.max(ALTITUDE).alias("MAX_ALTITUDE"),F.min(ALTITUDE).alias("MIN_ALTITUDE"))
    dataframe = dataframe.withColumn("DIFF_ALTITUDE", col("MAX_ALTITUDE") - col("MIN_ALTITUDE"))
    avg_alt = dataframe.select(SCENARIO_NAME, "DIFF_ALTITUDE") \
        .groupby(SCENARIO_NAME) \
        .agg(mean("DIFF_ALTITUDE").alias("MEAN_DIFF_ALTITUDE"))
    dataframe = dataframe.join(avg_alt, how='outer')
    dataframe = dataframe.withColumn(ENV4, col("DIFF_ALTITUDE") / col("MEAN_DIFF_ALTITUDE"))
    dataframe = dataframe.select(SCENARIO_NAME, ACID, ENV4)
    df = df.join(dataframe, on=[SCENARIO_NAME], how='outer')
    return df