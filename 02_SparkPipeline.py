import pandas as pd
import numpy as np
import os

from pyspark.sql import SparkSession
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, CrossValidator
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

from utils.get_latest_models import get_models, find_latest, load_latest_pipeline

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

#Notice we are only picking the most recent 10000 rows reflecting latest customer interactions
latest_interactions_DF = spark.sql("SELECT * FROM DEFAULT.CUSTOMER_INTERACTIONS_CICD LIMIT 10000")

df = latest_interactions_DF.select("RECENCY", "HISTORY", "USED_DISCOUNT", "USED_BOGO", "ZIP_CODE", "IS_REFERRAL", "CHANNEL", "OFFER", "SCORE", "CONVERSION")

#Renaming target feature as "LABEL":
df = df.withColumnRenamed("CONVERSION","label")

cols = df.columns

## Using Helper methods from utils.get_latest_models to load latest pipeline
models = get_models()
filtered = find_latest(models)
latest_pipeline_url = load_latest_pipeline(filtered)

pipeline = Pipeline.load(latest_pipeline_url)

## Fitting the pipeline and transforming data
pipelineModel = pipeline.fit(df)
out_df = pipelineModel.transform(df)

# 
temp = scaledData.rdd.map(lambda x:[float(y) for y in x['features']]).toDF(cols)

spark.sql("""CREATE TABLE IF NOT EXISTS default.transformed_interactions_CICD (
          RECENCY INT,
          HISTORY INT, 
          USED_DISCOUNT INT, 
          USED_BOGO INT, 
          ZIP_CODE INT, 
          IS_REFERRAL INT, 
          CHANNEL INT, 
          OFFER INT,
          SCORE FLOAT, 
          CONVERSION INT)""")

temp.write.insertInto("default.transformed_interactions_CICD", overwrite = False) 