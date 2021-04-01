import pandas as pd
import numpy as np
from datetime import datetime
import os, time
import sqlite3
from datetime import datetime
from pyspark.sql import SparkSession
from utils.get_latest_models import *
import joblib
from cmlbootstrap import CMLBootstrap
from sklearn.ensemble import RandomForestClassifier
import yaml

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

spark_table = "DEFAULT.CUSTOMER_INTERACTIONS_CICD"

#Notice we are only picking the most recent 1000 rows reflecting latest customer interactions
df = spark.sql("SELECT RECENCY, HISTORY, USED_DISCOUNT, USED_BOGO, ZIP_CODE, IS_REFERRAL, CHANNEL, OFFER, SCORE, CONVERSION FROM {} ORDER BY BATCH_TMS DESC LIMIT 20000".format(spark_table))

#Renaming target feature as "LABEL":
df = df.withColumnRenamed("CONVERSION","label")

#Converting the Spark df to Pandas df
pandas_df = df.toPandas()

y = pandas_df["label"]
X = pandas_df[["RECENCY", "HISTORY", "USED_DISCOUNT", "USED_BOGO", "IS_REFERRAL", "SCORE"]]

#Training Random Forest Classifier
#Disclaimer: in order to avoid redundancy in the demo, we are not going to train this model the correct way as we did with the PySpark classifier 
#Do not train your models this way at home

clf = RandomForestClassifier(max_depth=2, random_state=0)
clf.fit(X, y)

## Saving the newly trained model

run_time_suffix = datetime.now()
run_time_suffix_string = run_time_suffix.strftime("%d%m%Y%H%M%S")

model_dir = "sklearn_models"
model_name = clf.__class__.__name__+"{}".format(run_time_suffix_string)

joblib.dump(clf, model_dir+"/"+model_name)

## Saving the newly trained model metadata
conn = sqlite3.connect('models.db')
c = conn.cursor()

import re
models_insert = [(model_name, 
                 model_name, 
                 run_time_suffix_string, 
                 model_dir+"/"+model_name)
                ]

c.executemany('INSERT INTO sklearn_models VALUES (?,?,?,?)', models_insert)

conn.commit()
conn.close()


#USING CML BOOTSTRAP TO PUSH NEW MODEL AUTOMATICALLY

HOST = os.getenv("CDSW_API_URL").split(
    ":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split(
    "/")[6]
API_KEY = os.getenv("MY_API_KEY")
PROJECT_NAME = os.getenv("CDSW_PROJECT")

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

# Get User Details
user_details = cml.get_user({})
user_obj = {"id": user_details["id"], "username": USERNAME,
            "name": user_details["name"],
            "type": user_details["type"],
            "html_url": user_details["html_url"],
            "url": user_details["url"]
            }

# Get Project Details
project_details = cml.get_project({})
project_id = project_details["id"]


# Get Default Engine Details
default_engine_details = cml.get_default_engine({})
default_engine_image_id = default_engine_details["id"]

## CREATE MODEL PROGRAMMATICALLY ##

model_name = "Sklearn Classifier " + run_time_suffix_string

# Create the YAML file for Model Lineage

dict_yaml = [{model_name : {"hive_table_qualified_names": [spark_table], "metadata":{"custom metadata key" : "custom metadata value"}}}]

with open(r'lineage.yml', 'w') as file:
  documents = yaml.dump(dict_yaml, file)

# Create Model
example_model_input = {"RECENCY": 23, "HISTORY": 29, "USED_DISCOUNT": 0, "USED_BOGO": 0, "IS_REFERRAL": 0, "SCORE":0.766}

create_model_params = {
    "projectId": project_id,
    "name": model_name,
    "description": "Marketing Campaign Predictions",
    "visibility": "private",
    "enableAuth": False,
    "targetFilePath": "04_ServePredictions.py",
    "targetFunctionName": "predict",
    "engineImageId": default_engine_image_id,
    "kernel": "python3",
    "examples": [
        {
            "request": example_model_input,
            "response": {}
        }],
    "cpuMillicores": 1000,
    "memoryMb": 2048,
    "nvidiaGPUs": 0,
    "replicationPolicy": {"type": "fixed", "numReplicas": 1},
    "environment": {}}

new_model_details = cml.create_model(create_model_params)
access_key = new_model_details["accessKey"]  # todo check for bad response
model_id = new_model_details["id"]

print("New model created with access key", access_key)

# Disable model_authentication
cml.set_model_auth({"id": model_id, "enableAuth": False})

#Wait for the model to deploy. Tear down previous model. 
is_deployed = False

while is_deployed == False:
  model = cml.get_model({"id": str(new_model_details["id"]), "latestModelDeployment": True, "latestModelBuild": True})
  if model["latestModelDeployment"]["status"] == 'deployed':
    print("Model is deployed")
    break
  else:
    print ("Deploying Model.....")
    time.sleep(10)



