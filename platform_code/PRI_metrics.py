from parse.parser_constants import FLST_LOG_PREFIX
from schemas.tables_attributes import FLIGHT_TIME, SCENARIO_NAME, PRIORITY, PRI1, BASELINE_3D_DISTANCE, PRI2, \
    DISTANCE_3D, ACID, SPAWN_TIME, BASELINE_DEPARTURE_TIME, BASELINE_FLIGHT_TIME, PRI5, PRI3, PRI4
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, col

def compute_PRI1_metric(input_df, output_df):
    '''
    PRI-1: Weighted mission duration
    (Total duration of missions weighted in function of priority level)
    '''
    df = output_df["OUTPUT"]
    query_rows = input_df[FLST_LOG_PREFIX].groupby(SCENARIO_NAME, PRIORITY).agg((F.sum(FLIGHT_TIME) * col(PRIORITY)).alias(PRI1)).groupby(SCENARIO_NAME).agg(F.sum(PRI1).alias(PRI1))
    df = df.join(query_rows, on=[SCENARIO_NAME], how='outer')
    return df

def compute_PRI2_metric(input_df, output_df):
    '''
    PRI-2: Weighted mission track length
    (Total distance travelled weighted in function of priority level)
    '''
    df = output_df["OUTPUT"]
    query_rows = input_df[FLST_LOG_PREFIX].groupby(SCENARIO_NAME, PRIORITY).agg((F.sum(DISTANCE_3D) * col(PRIORITY)).alias(PRI2)).groupby(SCENARIO_NAME).agg(F.sum(PRI2).alias(PRI2))
    df = df.join(query_rows, on=[SCENARIO_NAME], how='outer')
    return df

def compute_PRI3_metric(input_df, output_df):
    '''
    PRI-3: Average mission duration per priority level
    (The average mission duration for each priority level per aircraft)
    '''
    df = output_df["OUTPUT"]
    df1 = input_df[FLST_LOG_PREFIX].groupby(SCENARIO_NAME, PRIORITY).agg(F.sum(FLIGHT_TIME).alias("Tprio"))
    df2 = input_df[FLST_LOG_PREFIX].groupby(SCENARIO_NAME, PRIORITY).count().withColumnRenamed("count", "Nprio")
    df3 = df1.join(df2, on=[SCENARIO_NAME, PRIORITY], how='outer')
    df3 = df3.withColumn(PRI3, col("Tprio") / col("Nprio")).select(SCENARIO_NAME, PRIORITY, PRI3)
    df3.show()
    #TODO: Ask Niki: df = df.join(df3, on=[SCENARIO_NAME], how='outer')
    return df

def compute_PRI4_metric(input_df, output_df):
    '''
    PRI-4: Average mission track length per priority level
    (The average distance travelled for each priority level per aircraft)
    '''
    df = output_df["OUTPUT"]
    df1 = input_df[FLST_LOG_PREFIX].groupby(SCENARIO_NAME, PRIORITY).agg(F.sum(DISTANCE_3D).alias("Dprio"))
    df2 = input_df[FLST_LOG_PREFIX].groupby(SCENARIO_NAME, PRIORITY).count().withColumnRenamed("count", "Nprio")
    df3 = df1.join(df2, on=[SCENARIO_NAME, PRIORITY], how='outer')
    df3 = df3.withColumn(PRI4, col("Dprio") / col("Nprio")).select(SCENARIO_NAME, PRIORITY, PRI4)
    df3.show()
    #TODO: Ask Niki: df = df.join(df3, on=[SCENARIO_NAME], how='outer')
    return df

def compute_PRI5_metric(input_df, output_df):
    '''
    PRI-5: Total delay per priority level
    (The total delay experienced by aircraft in a certain priority category relative to ideal conditions)
    '''
    df = output_df["OUTPUT"]
    df1 = input_df[FLST_LOG_PREFIX].withColumn("sum1", col(SPAWN_TIME) - col(BASELINE_DEPARTURE_TIME)).groupby(
        SCENARIO_NAME, PRIORITY).agg(F.sum("sum1").alias("sum1"))
    df2 = input_df[FLST_LOG_PREFIX].withColumn("sum2", col(FLIGHT_TIME) - col(BASELINE_FLIGHT_TIME)).groupby(
        SCENARIO_NAME, PRIORITY).agg(F.sum("sum2").alias("sum2"))

    df3 = df1.join(df2, on=[SCENARIO_NAME, PRIORITY], how='outer')
    df_tmp = df3.withColumn(PRI5, col("sum1") + col("sum2")).select(SCENARIO_NAME, PRIORITY, PRI5)
    df_tmp.show()
    #TODO: Ask Niki: df = df.join(df3, on=[SCENARIO_NAME, ACID], how='outer')
    return df
