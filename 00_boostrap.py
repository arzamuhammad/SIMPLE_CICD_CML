## NB: Only run this once ##

#!rm /home/cdsw/Simple_CICD_CML/models.db
!pip3 install -r requirements.txt

import os
import time
import uuid
import json
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from pyspark.sql import SparkSession

#Extracting the correct URL from hive-site.xml
tree = ET.parse('/etc/hadoop/conf/hive-site.xml')
root = tree.getroot()

for prop in root.findall('property'):
    if prop.find('name').text == "hive.metastore.warehouse.dir":
        storage = prop.find('value').text.split("/")[0] + "//" + prop.find('value').text.split("/")[2]

print("The correct Coud Storage URL is:{}".format(storage))

os.environ['STORAGE'] = storage

## Apply Batch ID and Current time to data ##

now = datetime.now()
df = pd.read_csv("data/historical.csv")

df['batch_id'] = uuid.uuid1()
df['batch_tms'] = datetime.now() 

df.to_csv("data/historical.csv", index=False)

### Load Historical Data

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.yarn.access.hadoopFileSystems",os.environ['STORAGE'])\
    .config("spark.hadoop.yarn.resourcemanager.principal",os.environ["HADOOP_USER_NAME"])\
    .getOrCreate()

spark.sql("DROP TABLE IF EXISTS default.customer_interactions_cicd")

spark.sql("""CREATE TABLE IF NOT EXISTS default.customer_interactions_CICD (NAME STRING, 
          STREET_ADDRESS STRING,
          CITY STRING,
          POSTCODE INT, 
          PHONE_NUMBER INTEGER,
          JOB STRING,
          RECENCY INT,
          HISTORY INT, 
          USED_DISCOUNT INT, 
          USED_BOGO INT, 
          ZIP_CODE STRING, 
          IS_REFERRAL INT, 
          CHANNEL STRING, 
          OFFER STRING,
          CONVERSION INT, 
          SCORE FLOAT, 
          BATCH_ID STRING,
          BATCH_TMS TIMESTAMP
          )""")
    
historical_spark_df = spark.read.csv("data/historical.csv", header=True, sep=',')

historical_spark_df.write.insertInto("default.customer_interactions_CICD", overwrite = False) 

## Create sqlite table to track models metadata

import sqlite3
conn = sqlite3.connect('models.db')
c = conn.cursor()
c.execute(""" CREATE TABLE spark_models (model_name text, model_id text, training_time timestamp, model_storage_location text) """)
c.execute(""" CREATE TABLE spark_pipelines (pipeline_name text, pipeline_id text, training_time timestamp, pipeline_storage_location text) """)
c.execute(""" CREATE TABLE sklearn_models (model_name text, model_id text, training_time timestamp, model_storage_location text) """)
conn.commit()
conn.close()
