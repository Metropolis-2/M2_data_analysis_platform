from parse.parser_constants import CONF_LOG_PREFIX, LOS_LOG_PREFIX, GEO_LOG_PREFIX
from schemas.tables_attributes import SCENARIO_NAME, SAF1, SAF2, SAF3, DISTANCE, SAF4, SAF6, SAF5, LOS_DURATION_TIME
from pyspark.sql.functions import lit, col
import pyspark.sql.functions as F


def compute_SAF1_metric(spark, input_df, output_df):
    '''
    SAF-1: Number of conflicts
    (Number of aircraft pairs that will experience a loss of separation within the look-ahead time)
    '''
    df = output_df["OUTPUT"]
    query_rows = input_df[CONF_LOG_PREFIX].groupBy(SCENARIO_NAME).count().select([SCENARIO_NAME, col('count').alias(SAF1)])
    df = df.join(query_rows, on=[SCENARIO_NAME], how='outer')
    return df

def compute_SAF2_metric(spark, input_df, output_df):
    '''
    SAF-2: Number of intrusions
    (Number of aircraft pairs that experience loss of separation)
    '''
    df = output_df["OUTPUT"]
    query_rows = input_df[LOS_LOG_PREFIX].groupBy(SCENARIO_NAME).count().select([SCENARIO_NAME, col('count').alias(SAF2)])
    df = df.join(query_rows, on=[SCENARIO_NAME], how='outer')
    return df

def compute_SAF3_metric(spark, input_df, output_df):
    '''
    SAF-3: Intrusion prevention rate
    (Ratio representing the proportion of conflicts that did not result in a loss of separation)
    '''
    df = output_df["OUTPUT"]
    df = df.withColumn(SAF3, df.SAF1/df.SAF2)
    return df

def compute_SAF4_metric(spark, input_df, output_df):
    '''
    SAF-4: Minimum separation
    (The minimum separation between aircraft during conflicts)
    '''
    df = output_df["OUTPUT"]
    tmp_df = input_df[LOS_LOG_PREFIX].groupby(SCENARIO_NAME).agg(F.min(DISTANCE).alias(SAF4))
    df = df.join(tmp_df, on=[SCENARIO_NAME], how='outer')
    return df

def compute_SAF5_metric(spark, input_df, output_df):
    '''
    SAF-5: Time spent in LOS
    (Total time spent in a state of intrusion)
    '''
    df = output_df["OUTPUT"]
    query_rows = input_df[LOS_LOG_PREFIX].select(SCENARIO_NAME, col(LOS_DURATION_TIME).alias(SAF5))
    df = df.join(query_rows, on=[SCENARIO_NAME], how='outer')
    return df

def compute_SAF6_metric(spark, input_df, output_df):
    '''
    SAF-6: Geofence violations
    (The number of geofence/building area violations)
    '''
    df = output_df["OUTPUT"]
    query_rows = input_df[GEO_LOG_PREFIX].groupBy(SCENARIO_NAME).count().select([SCENARIO_NAME, col('count').alias(SAF6)])
    df = df.join(query_rows, on=[SCENARIO_NAME], how='outer')
    return df