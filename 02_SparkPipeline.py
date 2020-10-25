import pandas as pd
import numpy as np
import os
import sqlite3
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.linalg import DenseVector

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
latest_interactions_DF = spark.sql("SELECT * FROM DEFAULT.CUSTOMER_INTERACTIONS_CICD ORDER BY BATCH_TMS DESC LIMIT 1000")

df = latest_interactions_DF.select("RECENCY", "HISTORY", "USED_DISCOUNT", "USED_BOGO", "ZIP_CODE", "IS_REFERRAL", "CHANNEL", "OFFER", "SCORE", "CONVERSION")

#Renaming target feature as "LABEL":
df = df.withColumnRenamed("CONVERSION","LABEL")

## Loading latest pipeline object from storage

conn = sqlite3.connect('Simple_CICD_CML/models.db')
c = conn.cursor()
p = c.execute('''SELECT pipeline_storage_location FROM pipelines ORDER BY training_time DESC''')

latest_pipeline_url = p.fetchall()[0][0]

int_time_string = str(datetime.strptime(latest_pipeline_url.split("/")[-1], '%Y-%m-%d %H:%M:%S.%f').strftime("%d%m%Y%H%M%S"))
latest_pipeline_url_formatted = latest_pipeline_url.replace(latest_pipeline_url.split('/')[-1], int_time_string)

pipeline = Pipeline.load(latest_pipeline_url_formatted)

cat_cols = [item[0] for item in df.dtypes if item[1].startswith('string')]
num_cols = [item[0] for item in df.dtypes if item[1].startswith('in') or item[1].startswith('dou')]

num_cols.remove('LABEL')

def make_pipeline(df):        
    stages= []

    for col in cat_cols:

        stringIndexer = StringIndexer(inputCol = col , outputCol = col + '_StringIndex')
        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[col + '_ClassVect'])
        stages += [stringIndexer, encoder]

    #Assembling mixed data type transformations:
    assemblerInputs = [c + "_ClassVect" for c in cat_cols] + num_cols
    #assemblerInputs = num_cols
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

    stages += [assembler]
    
    #Scaling features
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    stages += [scaler]

    #Creating and running the pipeline:
    pipeline = Pipeline(stages=stages)
    pipelineModel = pipeline.fit(df)
    out_df = pipelineModel.transform(df)
    
    return out_df, pipeline

out_df, pipeline = make_pipeline(df)

# Writing transformed data into a table
cols = [item[0] for item in out_df.dtypes if item[1].startswith('in') or item[1].startswith('dou') or item[1].startswith('str')]
temp = out_df.rdd.map(lambda x: (x['LABEL'], DenseVector(x['scaledFeatures'])).toDF(cat_cols+num_cols)

                      #out_df.rdd.map(lambda x: (x['LABEL'], DenseVector(x['scaledFeatures']))).toDF().show()
                      
spark.sql("""CREATE TABLE IF NOT EXISTS default.transformed_interactions_CICD (
          RECENCY FLOAT,
          HISTORY FLOAT, 
          USED_DISCOUNT FLOAT, 
          USED_BOGO FLOAT, 
          ZIP_CODE FLOAT, 
          IS_REFERRAL FLOAT, 
          CHANNEL FLOAT, 
          OFFER FLOAT,
          LABEL FLOAT,
          ZIP_CODE_StringIndex FLOAT, 
          CHANNEL_StringIndex FLOAT,
          OFFER_StringIndex FLOAT
          )""")

temp.write.insertInto("default.transformed_interactions_CICD", overwrite = False) 