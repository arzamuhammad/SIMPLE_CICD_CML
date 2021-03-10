import pandas as pd
import numpy as np
import datetime
import os, time
import sqlite3
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import PipelineModel, Pipeline
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
    
### Reading latest data and recreating pipeline from model development notebook

#Notice we are only picking the most recent 1000 rows reflecting latest customer interactions
df = spark.sql("SELECT RECENCY, HISTORY, USED_DISCOUNT, USED_BOGO, ZIP_CODE, IS_REFERRAL, CHANNEL, OFFER, SCORE, CONVERSION FROM DEFAULT.CUSTOMER_INTERACTIONS_CICD ORDER BY BATCH_TMS DESC LIMIT 20000")

#Renaming target feature as "LABEL":
df = df.withColumnRenamed("CONVERSION","label")

latest_url_formatted = find_latest("pipeline")

pm = Pipeline.load(latest_url_formatted)

pm = pm.fit(df)

## Saving the newly trained model

run_time_suffix = datetime.now()
run_time_suffix_string = run_time_suffix.strftime("%d%m%Y%H%M%S")

pm.write().overwrite().save(os.environ["STORAGE"]+"/testpysparkmodels/"+"{}".format(run_time_suffix_string))

## Saving the newly trained model metadata
conn = sqlite3.connect('models.db')
c = conn.cursor()

import re
models_insert = [(str(pm).split("_")[0], 
                 str(pm).split("_")[1], 
                 str(run_time_suffix), 
                 os.environ["STORAGE"]+"/testpysparkmodels/"+"{}".format(run_time_suffix))
                ]

c.executemany('INSERT INTO models VALUES (?,?,?,?)', models_insert)

conn.commit()
conn.close()