# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 15:29:27 2022

@author: labpc2
"""

import dill
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, when
from pyspark.sql import SparkSession
import pandas as pd





spark = SparkSession.builder.appName('Platform Analysis.com').getOrCreate()
sparkContext = spark.sparkContext



input_file=open("parquets/loslog_dataframe.dill", 'rb')
df=dill.load(input_file)

#df[df["LAT1"]<49]

df.drop([0])

d = sparkContext.parallelize(df)

dd=d.filter(lambda x: x[5] <49).collect()


#dd.collect()
#print(d.count())