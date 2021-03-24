## 01_ModelDevelopment.py ##

import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd

import os
import sys
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("ModelDevelopment")\
    .config("spark.authenticate", "true")\
    .config("spark.yarn.access.hadoopFileSystems", os.environ["STORAGE"])\
    .config("spark.hadoop.yarn.resourcemanager.principal",os.environ["HADOOP_USER_NAME"])\
    .config("spark.executor.memory","6g")\
    .config("spark.executor.cores","3")\
    .getOrCreate()

hist_DF = spark.sql("SELECT * FROM DEFAULT.CUSTOMER_INTERACTIONS_CICD")

df = hist_DF.select("RECENCY", "HISTORY", "USED_DISCOUNT", "USED_BOGO", "ZIP_CODE", "IS_REFERRAL", "CHANNEL", "OFFER", "SCORE", "CONVERSION")

#Renaming target feature as "LABEL":
df = df.withColumnRenamed("CONVERSION","label")

cat_cols = [item[0] for item in df.dtypes if item[1].startswith('string')]
num_cols = [item[0] for item in df.dtypes if item[1].startswith('in')]

num_cols.remove('label')

train, test = df.randomSplit([0.8, 0.2], seed=1)

from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, CrossValidator
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier

def make_pipeline():

    stages= []

    for col in cat_cols:

        stringIndexer = StringIndexer(inputCol = col , outputCol = col + '_StringIndex')
        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[col + '_ClassVect'])
        stages += [stringIndexer, encoder]

    #Assembling mixed data type transformations:
    assemblerInputs = [c + "_ClassVect" for c in cat_cols] + num_cols
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

    stages += [assembler]

    rf = RandomForestClassifier(labelCol = "label")

    stages += [rf]

    #Creating and running the pipeline:
    pipeline = Pipeline(stages=stages)

    return pipeline

pipeline = make_pipeline()

paramGrid = ParamGridBuilder() \
    .addGrid(RandomForestClassifier.numTrees, [10, 20, 30]) \
    .addGrid(RandomForestClassifier.maxDepth, [5, 10]) \
    .build()

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(metricName="areaUnderROC"),
                          numFolds=5)

# Run cross-validation, and choose the best set of parameters.
cvModel = crossval.fit(train)

bestModel = cvModel.bestModel

#Evaluating model with the held out test set:
prediction = cvModel.transform(test)

predictionAndTarget = prediction.select("label", "prediction")

# Create both evaluators
evaluatorMulti = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="prediction", metricName='areaUnderROC')

# Get metrics
acc = evaluatorMulti.evaluate(predictionAndTarget, {evaluatorMulti.metricName: "accuracy"})
f1 = evaluatorMulti.evaluate(predictionAndTarget, {evaluatorMulti.metricName: "f1"})
weightedPrecision = evaluatorMulti.evaluate(predictionAndTarget, {evaluatorMulti.metricName: "weightedPrecision"})
weightedRecall = evaluatorMulti.evaluate(predictionAndTarget, {evaluatorMulti.metricName: "weightedRecall"})
auc = evaluator.evaluate(predictionAndTarget)

y_true = predictionAndTarget.select(['label']).collect()
y_pred = predictionAndTarget.select(['prediction']).collect()

from sklearn.metrics import classification_report, confusion_matrix, plot_confusion_matrix, roc_curve
cm = confusion_matrix(y_true, y_pred)
print(classification_report(y_true, y_pred))

run_time_suffix = datetime.now()
run_time_suffix_string = run_time_suffix.strftime("%d%m%Y%H%M%S")

bestModel.write().overwrite().save(os.environ["STORAGE"]+"/testpysparkmodels/"+"{}".format(run_time_suffix_string))
pipeline.write().overwrite().save(os.environ["STORAGE"]+"/testpysparkpipelines/"+"{}".format(run_time_suffix_string))

spark.stop()

import sqlite3
conn = sqlite3.connect('models.db')
c = conn.cursor()

import re
models_insert = [(str(bestModel).split("_")[0],
                 str(bestModel).split("_")[1],
                 str(run_time_suffix),
                 os.environ["STORAGE"]+"/testpysparkmodels/"+"{}".format(run_time_suffix))
                ]

pipelines_insert = [(str(pipeline).split("_")[0],
                 str(pipeline).split("_")[1],
                 str(run_time_suffix),
                 os.environ["STORAGE"]+"/testpysparkpipelines/"+"{}".format(run_time_suffix))
                ]

c.executemany('INSERT INTO pipelines VALUES (?,?,?,?)', pipelines_insert)
c.executemany('INSERT INTO models VALUES (?,?,?,?)', models_insert)
c.executemany('INSERT INTO models VALUES (?,?,?,?)', models_insert)

conn.commit()
conn.close()

#Check pipeline was written to sqlite3 successfully
conn = sqlite3.connect('models.db')
c = conn.cursor()
for i in c.execute("select * from pipelines"): print(i)
for i in c.execute("select * from models"): print(i)

conn.commit()
conn.close()
