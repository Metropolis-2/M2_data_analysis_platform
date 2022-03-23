from parse.parser_constants import FLST_LOG_PREFIX
from schemas.tables_attributes import FLIGHT_TIME, SCENARIO_NAME, PRIORITY, PRI1, BASELINE_3D_DISTANCE, PRI2, \
    DISTANCE_3D, ACID, SPAWN_TIME, BASELINE_DEPARTURE_TIME
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, col

def compute_PRI1_metric(spark, input_df, output_df):
    '''
    PRI-1: Weighted mission duration
    (Total duration of missions weighted in function of priority level)
    '''
    df = output_df["OUTPUT"]
    query_rows = input_df[FLST_LOG_PREFIX].groupby(SCENARIO_NAME, PRIORITY).agg((F.sum(FLIGHT_TIME) * col(PRIORITY)).alias(PRI1)).groupby(SCENARIO_NAME).agg(F.sum(PRI1))
    df = df.join(query_rows, on=[SCENARIO_NAME], how='outer')
    return df

def compute_PRI2_metric(spark, input_df, output_df):
    '''
    PRI-2: Weighted mission track length
    (Total distance travelled weighted in function of priority level)
    '''
    df = output_df["OUTPUT"]
    query_rows = input_df[FLST_LOG_PREFIX].groupby(SCENARIO_NAME, PRIORITY).agg((F.sum(DISTANCE_3D) * col(PRIORITY)).alias(PRI2)).groupby(SCENARIO_NAME).agg(F.sum(PRI2))
    df = df.join(query_rows, on=[SCENARIO_NAME], how='outer')
    return df

def compute_PRI3_metric(spark, input_df, output_df):
    '''
    PRI-3: Average mission duration per priority level
    (The average mission duration for each priority level per aircraft)
    '''
    df = output_df["OUTPUT"]
    df1 = input_df[FLST_LOG_PREFIX].groupby(SCENARIO_NAME, PRIORITY).agg(F.sum(FLIGHT_TIME).alias("Tprio"))
    df2 = input_df[FLST_LOG_PREFIX].groupby(SCENARIO_NAME, PRIORITY).count().withColumnRenamed("count", "Nprio")
    df3 = df1.join(df2, on=[SCENARIO_NAME, PRIORITY], how='outer')
    df3 = df3.withColumn("PRIO3", df3.Tprio / df3.Nprio)
    #TODO: drop Tprio and Nprio in df3
    df = df.join(df3, on=[SCENARIO_NAME], how='outer')
    return df

def compute_PRI4_metric(spark, input_df, output_df):
    '''
    PRI-4: Average mission track length per priority level
    (The average distance travelled for each priority level per aircraft)
    '''
    df = output_df["OUTPUT"]
    df1 = input_df[FLST_LOG_PREFIX].groupby(SCENARIO_NAME, PRIORITY).agg(F.sum(DISTANCE_3D).alias("Dprio"))
    df2 = input_df[FLST_LOG_PREFIX].groupby(SCENARIO_NAME, PRIORITY).count().withColumnRenamed("count", "Nprio")
    df3 = df1.join(df2, on=[SCENARIO_NAME, PRIORITY], how='outer')
    df3 = df3.withColumn("PRIO4", df3.Dprio / df3.Nprio)
    #TODO: drop Dprio and Nprio in df3
    df = df.join(df3, on=[SCENARIO_NAME], how='outer')
    return df

def compute_PRI5_metric(spark, input_df, output_df):
    '''
    PRI-5: Total delay per priority level
    (The total delay experienced by aircraft in a certain priority category relative to ideal conditions)
    '''
    df = output_df["OUTPUT"]
    df1 = input_df[FLST_LOG_PREFIX].withColumn("out", input_df[FLST_LOG_PREFIX][SPAWN_TIME] - input_df[FLST_LOG_PREFIX][BASELINE_DEPARTURE_TIME]).groupby(SCENARIO_NAME, ACID).agg(F.sum("PRI5"))
    df = df.join(df1, on=[SCENARIO_NAME, ACID], how='outer')
    return df
