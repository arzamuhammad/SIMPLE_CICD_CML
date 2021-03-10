import pandas as pd
import numpy as np
import os
import sqlite3
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import PipelineModel
from pyspark.ml.linalg import DenseVector
from utils.get_latest_models import *

spark = SparkSession\
    .builder\
    .appName("Database Scoring")\
    .config("spark.authenticate", "true")\
    .config("spark.yarn.access.hadoopFileSystems", os.environ["STORAGE"])\
    .config("spark.hadoop.yarn.resourcemanager.principal",os.environ["HADOOP_USER_NAME"])\
    .config("spark.executor.instances", 2)\
    .config("spark.executor.cores", 2)\
    .getOrCreate()
    
#spark.sql("DROP TABLE default.transformed_interactions_CICD")
    
### Reading latest data and recreating pipeline from model development notebook

#Notice we are only picking the most recent 1000 rows reflecting latest customer interactions
df = spark.sql("SELECT RECENCY, HISTORY, USED_DISCOUNT, USED_BOGO, ZIP_CODE, IS_REFERRAL, CHANNEL, OFFER, SCORE, CONVERSION FROM DEFAULT.CUSTOMER_INTERACTIONS_CICD ORDER BY BATCH_TMS DESC LIMIT 1000")

#Renaming target feature as "LABEL":
df = df.withColumnRenamed("CONVERSION","label")


latest_url_formatted = find_latest("model")

pm = PipelineModel.load(latest_url_formatted)

predictions = pm.transform(df)

## Writing predictions to a new Spark table ##

cols = ['RECENCY',
 'HISTORY',
 'USED_DISCOUNT',
 'USED_BOGO',
 'ZIP_CODE',
 'IS_REFERRAL',
 'CHANNEL',
 'OFFER',
 'SCORE',
 'LABEL',
 'ZIP_CODE_StringIndex',
 'CHANNEL_StringIndex',
 'OFFER_StringIndex',
 'prediction']

preds = predictions.select(*cols)

spark.sql("""CREATE TABLE IF NOT EXISTS default.transformed_interactions_CICD (
          RECENCY FLOAT,
          HISTORY FLOAT, 
          USED_DISCOUNT FLOAT, 
          USED_BOGO FLOAT, 
          ZIP_CODE FLOAT, 
          IS_REFERRAL FLOAT, 
          CHANNEL FLOAT, 
          OFFER FLOAT,
          SCORE FLOAT,
          LABEL FLOAT,
          ZIP_CODE_StringIndex FLOAT, 
          CHANNEL_StringIndex FLOAT,
          OFFER_StringIndex FLOAT, 
          PREDICTION FLOAT
          )""")

preds.write.insertInto("default.transformed_interactions_CICD", overwrite = False) 