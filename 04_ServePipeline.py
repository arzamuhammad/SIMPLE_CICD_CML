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
from pyspark.sql.types import *

spark = SparkSession\
    .builder\
    .appName("Database Scoring")\
    .config("spark.authenticate", "true")\
    .config("spark.yarn.access.hadoopFileSystems", os.environ["STORAGE"])\
    .config("spark.hadoop.yarn.resourcemanager.principal",os.environ["HADOOP_USER_NAME"])\
    .config("spark.executor.instances", 2)\
    .config("spark.executor.cores", 2)\
    .getOrCreate()
    
#Reading the most up to date model
latest_url_formatted = find_latest("model")
    
feature_schema = StructType([StructField("RECENCY", FloatType(), True),
StructField("HISTORY", FloatType(), True),
StructField("USED_DISCOUNT", FloatType(), True),
StructField("USED_BOGO", FloatType(), True),
StructField("IS_REFERRAL", FloatType(), True)])


#cols: ['RECENCY', 'HISTORY', 'USED_DISCOUNT', 'USED_BOGO', 'IS_REFERRAL']

#data = {"feature":"11,204,0,1,1"}
#{"result": 1}

def predict(data, feature_schema, latest_url_formatted):
  
  request_data = data["feature"].split(",")
  
  df = spark.createDataFrame([
    (
    float(request_data[0]), 
    float(request_data[1]),
    float(request_data[2]),
    float(request_data[3]),
    float(request_data[4])
    )
  ], schema=feature_schema)
  
  pm = PipelineModel.load(latest_url_formatted)
  predictions = pm.transform(df).collect()[0].prediction
  
  return {"result": predictions}

