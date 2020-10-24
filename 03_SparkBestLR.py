import pandas as pd
import numpy as np
import os

from pyspark.sql import SparkSession
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, CrossValidator
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression

from utils.get_latest_models import get_models, find_latest, load_latest_lr

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
    
latest_transformed_interactions_DF = spark.sql("SELECT * FROM DEFAULT.TRANSFORMED_INTERACTIONS_CICD LIMIT 10000")

## Using Helper methods from utils.get_latest_models to load latest pipeline
models = get_models()
filtered = find_latest(models)
latest_bestLR_url = load_latest_lr(filtered)

lr = LogisticRegression.load(latest_bestLR_url)

## Fitting the pipeline and transforming data
lrModel = lr.fit(df)
out_df = pipelineModel.transform(df)
