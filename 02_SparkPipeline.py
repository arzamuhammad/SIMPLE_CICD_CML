import pandas as pd
import numpy as np
import os

from pyspark.sql import SparkSession
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, CrossValidator
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-1")\
    .config("spark.yarn.access.hadoopFileSystems","s3a://demo-aws-1/")\
    .config("spark.hadoop.yarn.resourcemanager.principal",os.environ["HADOOP_USER_NAME"])\
    .config("spark.yarn.access.hadoopFileSystems", "s3a://demo-aws-1")\
    .config("spark.executor.instances", 2)\
    .config("spark.executor.cores", 2)\
    .getOrCreate()
    
### Reading latest data and recreating pipeline from model development notebook

latest_interactions_DF = spark.sql("SELECT * FROM DEFAULT.CUSTOMER_INTERACTIONS_CICD")

df = hist_DF.select("RECENCY", "HISTORY", "USED_DISCOUNT", "USED_BOGO", "ZIP_CODE", "IS_REFERRAL", "CHANNEL", "OFFER", "SCORE", "CONVERSION")

#Renaming target feature as "LABEL":
df = df.withColumnRenamed("CONVERSION","label")

cat_cols = [item[0] for item in df.dtypes if item[1].startswith('string')]
num_cols = [item[0] for item in df.dtypes if item[1].startswith('in')]

!hdfs dfs -ls "s3a://demo-aws-1/datalake/pdefusco/pipeline"