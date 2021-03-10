from pyspark.sql import SparkSession
from datetime import datetime
import random, string
import pandas as pd
import numpy as np
import uuid
import os

## Creasting some random instances from the original file ##

new_interactions_df = pd.read_csv("data/historical.csv")

df_sample = new_interactions_df.sample(1000)

#conversion_values = [0,1]
now = datetime.now()

df_sample["conversion"] = np.nan #np.random.choice(conversion_values, size=len(df_sample), p=[0.3,0.7])
df_sample['batch_id'] = uuid.uuid1()
df_sample['batch_tms'] = datetime.now() 

## Writing the new interactions to the database ##

spark = SparkSession\
    .builder\
    .appName("Generate Data")\
    .config("spark.authenticate", "true")\
    .config("spark.yarn.access.hadoopFileSystems", os.environ["STORAGE"])\
    .config("spark.hadoop.yarn.resourcemanager.principal",os.environ["HADOOP_USER_NAME"])\
    .getOrCreate()
    
new_interactions_spark_df = spark.createDataFrame(df_sample)
new_interactions_spark_df.write.insertInto("default.customer_interactions_CICD", overwrite = False) 

spark.stop()