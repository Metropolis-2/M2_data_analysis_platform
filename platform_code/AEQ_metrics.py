# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""
from parse.parser_constants import FLST_LOG_PREFIX
from schemas.tables_attributes import SCENARIO_NAME, PRIORITY, LOITERING, BASELINE_ARRIVAL_TIME, DEL_TIME, AEQ1, AEQ2, \
    ACID, FLIGHT_TIME, VEHICLE, AEQ2_1, AEQ1_1, AEQ3, AEQ4, AEQ5, AEQ5_1
from pyspark.sql.functions import udf, col, when
import pyspark.sql.functions as F
from config import settings
from pyspark.sql.functions import stddev
from pyspark.sql.functions import mean
from pyspark.sql.functions import abs

def compute_AEQ1_metric(input_df, output_df):
    '''
    AEQ-1: Number of cancelled demands
    (Number of situations when realized arrival time of a given flight intention is greater than ideal expected arrival time
    by more or equal than some given cancellation delay limit that depends on mission type.
    Ideal expected arrival time is computed as arrival time of the fastest trajectory from origin to destination departing
    at the requested time as if a user were alone in the system, respecting all concept airspace rules.
    Realized arrival time comes directly from the simulations)
    '''
    df = output_df["OUTPUT"]
    dataframe = input_df[FLST_LOG_PREFIX].select(SCENARIO_NAME, PRIORITY, LOITERING, BASELINE_ARRIVAL_TIME, DEL_TIME)

    dataframe = dataframe\
        .withColumn("delay", col(DEL_TIME) - col(BASELINE_ARRIVAL_TIME))

    dataframe = dataframe\
        .withColumn("cancelation_limit",
                    when(col(PRIORITY) == 4, settings.thresholds.emergency_mission_delay)
                    .otherwise(when(col(LOITERING),settings.thresholds.loitering_mission_delay)
                               .otherwise(settings.thresholds.delivery_mission_delay)))

    dataframe = dataframe\
        .select(SCENARIO_NAME,"delay","cancelation_limit")\
        .withColumn(AEQ1, col("delay") >= col("cancelation_limit"))

    dataframe = dataframe.select(SCENARIO_NAME, AEQ1).where(
        col(AEQ1)).groupby(SCENARIO_NAME).count().withColumnRenamed("count", AEQ1)

    df = df.join(dataframe, on=[SCENARIO_NAME], how='outer')
    return df

def compute_AEQ1_1_metric(input_df, output_df):
    '''
    AEQ-1.1 Percentage of cancelled demands
    (Calculated as the ratio of AEQ-1 and the total number of flight intentions in the given scenario)
    '''
    df = output_df["OUTPUT"]
    dataframe = input_df[FLST_LOG_PREFIX].select(SCENARIO_NAME, ACID)
    dataframe = dataframe.groupby(SCENARIO_NAME).count().withColumnRenamed("count", "Num_Acids")
    dataframe = dataframe.join(df, on=[SCENARIO_NAME], how='outer')
    dataframe = dataframe.withColumn(AEQ1_1, (col(AEQ1) / col("Num_Acids")) * 100).select(SCENARIO_NAME, AEQ1_1)
    df = df.join(dataframe, on=[SCENARIO_NAME], how='outer')
    return df

def compute_AEQ2_metric(input_df, output_df):
    '''
    AEQ-2: Number of inoperative trajectories
    (Number of situations when realized total mission duration is greater than specific drone autonomy.
    Realized trajectories and hence realized total mission duration comes directly from a simulation)
    '''
    df = output_df["OUTPUT"]
    dataframe = input_df[FLST_LOG_PREFIX].select(SCENARIO_NAME, ACID, FLIGHT_TIME, VEHICLE)
    dataframe = dataframe.withColumn("autonomy", when(col(VEHICLE) == "MP20", settings.MP20.autonomy)
                                                         .otherwise(settings.MP30.autonomy))

    dataframe = dataframe.withColumn("inoperative", when(col(FLIGHT_TIME) >= col("autonomy"), True).otherwise(False))
    dataframe = dataframe.select(SCENARIO_NAME, col("inoperative")).where(col("inoperative") == True).groupby(
        SCENARIO_NAME).count().withColumnRenamed("count", AEQ2)
    df = df.join(dataframe, on=[SCENARIO_NAME], how='outer')
    return df

def compute_AEQ2_1_metric(input_df, output_df):
    '''
    AEQ-2.1: Percentage of inoperative trajectories
    (Calculated as the ratio of AEQ-2 and the total number of flight intentions in the given scenario)
    '''
    df = output_df["OUTPUT"]
    dataframe = input_df[FLST_LOG_PREFIX].select(SCENARIO_NAME, ACID)
    dataframe = dataframe.groupby(SCENARIO_NAME).count().withColumnRenamed("count", "Num_Acids")
    dataframe = dataframe.join(df, on=[SCENARIO_NAME], how='outer')
    dataframe = dataframe.withColumn(AEQ2_1, (col(AEQ2) / col("Num_Acids")) * 100).select(SCENARIO_NAME, AEQ2_1)
    df = df.join(dataframe, on=[SCENARIO_NAME], how='outer')
    return df

def compute_AEQ3_metric(input_df, output_df):
    '''
    AEQ-3: The demand delay dispersion
    (Measured as standard deviation of delay of all flight intentions, where delay for each flight intention is calculated
    as a difference between realized arrival time and ideal expected arrival time.
    Ideal expected arrival time is computed as arrival time of the fastest trajectory from origin to destination departing
    at the requested time as if a user were alone in the system, respecting all concept airspace rules.
    Realized arrival time comes directly from the simulations)
    '''
    df = output_df["OUTPUT"]
    dataframe = input_df[FLST_LOG_PREFIX].select(SCENARIO_NAME, ACID, BASELINE_ARRIVAL_TIME, DEL_TIME)
    dataframe = dataframe.groupby(SCENARIO_NAME).agg(stddev(col(DEL_TIME) - col(BASELINE_ARRIVAL_TIME)).alias(AEQ3))
    df = df.join(dataframe, on=[SCENARIO_NAME], how='outer')
    return df

def compute_AEQ4_metric(input_df, output_df):
    '''
    AEQ-4: The worst demand delay
    (Computed as the maximal difference between any individual flight intention delay and the average delay;
    where delay for each flight intention is calculated as the difference between realized arrival time and
    ideal expected arrival time)
    '''
    df = output_df["OUTPUT"]
    dataframe = input_df[FLST_LOG_PREFIX].select(SCENARIO_NAME, BASELINE_ARRIVAL_TIME, DEL_TIME)
    dataframe = dataframe.withColumn("delay", (col(DEL_TIME) - col(BASELINE_ARRIVAL_TIME)))
    avg_delay= dataframe.select(mean("delay").alias("avg_delay"))
    dataframe = dataframe.join(avg_delay, how='outer')
    dataframe = dataframe.withColumn("delay_increment", abs(col("delay")-col("avg_delay")))
    dataframe = dataframe.groupby(SCENARIO_NAME).agg(F.max("delay_increment").alias(AEQ4))
    df = df.join(dataframe, on=[SCENARIO_NAME], how='outer')
    return df

def compute_AEQ5_metric(input_df, output_df):
    '''
    AEQ-5: Number of inequitable delayed demands
    (Number of flight intentions whose delay is greater than a given threshold from the average delay in absolute sense,
    where delay for each flight intention is calculated as the difference between
    realized arrival time and ideal expected arrival time)
    '''
    df = output_df["OUTPUT"]
    df.show()
    dataframe = input_df[FLST_LOG_PREFIX].select(SCENARIO_NAME, ACID, BASELINE_ARRIVAL_TIME, DEL_TIME)
    dataframe = dataframe.withColumn("delay", (col(DEL_TIME) - col(BASELINE_ARRIVAL_TIME)))
    avg_delay= dataframe.select(mean("delay").alias("avg_delay"))
    dataframe = dataframe.join(avg_delay, how='outer')
    dataframe = dataframe.select(SCENARIO_NAME, ACID).where((col("delay") > col("avg_delay")+settings.threshold.AEQ5) | (col("delay") < col("avg_delay")-settings.threshold.AEQ5)).groupby(SCENARIO_NAME).count().withColumnRenamed("count", AEQ5)
    dataframe.show()
    df = df.join(dataframe, on=[SCENARIO_NAME], how='outer')
    return df

def compute_AEQ5_1_metric(input_df, output_df):
    '''
    AEQ-5-1: Percentage of inequitable delayed demands
    (Calculated as the ratio of AEQ-5 and the total number of flight intentions in the given scenario)
    '''
    df = output_df["OUTPUT"]
    dataframe = input_df[FLST_LOG_PREFIX].select(SCENARIO_NAME, ACID)
    dataframe = dataframe.groupby(SCENARIO_NAME).count().withColumnRenamed("count", "Num_Acids")
    dataframe = dataframe.join(df, on=[SCENARIO_NAME], how='outer')
    dataframe = dataframe.withColumn(AEQ5_1, ((col(AEQ5) / col("Num_Acids")) * 100)).select(SCENARIO_NAME, AEQ5_1)
    df = df.join(dataframe, on=[SCENARIO_NAME], how='outer')
    return df