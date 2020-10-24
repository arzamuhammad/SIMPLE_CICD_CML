import pandas as pd
import numpy as np
import os
import random, string

from faker import Faker

from pyspark.sql import SparkSession



mu, sigma = 1, .4 # mean and standard deviation
s = np.random.normal(mu, sigma, rg)
data['score'] = data['conversion']+s

new_interactions_df = pd.DataFrame(data)

#new_interactions_df.to_csv('data/new_interactions_df.csv', index=False)

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-1")\
    .config("spark.yarn.access.hadoopFileSystems","s3a://demo-aws-1/")\
    .config("spark.hadoop.yarn.resourcemanager.principal",os.getenv("HADOOP_USER_NAME"))\
    .getOrCreate()
    

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
          CONVERSION INT)""")
    
new_interactions_spark_df = spark.createDataFrame(new_interactions_df)
  
new_interactions_spark_df.write.insertInto("default.customer_interactions_CICD", overwrite = False) 


"""fake = Faker('en_US')

zip_codes = ['Suburban', 'Rural', 'Urban']
channels = ['Phone', 'Web', 'Multichannel']
offers = ['Buy One Get One', 'No Offer', 'Discount']

data = {} 
#SDNSLOGS_AGENT_V3
rg = 20000

data['name'] = [fake.name() for i in range(rg)]
data['street_address'] = [fake.street_address() for i in range(rg)]
data['city'] = [fake.city() for i in range(rg)]
data['postcode'] = [fake.postcode() for i in range(rg)]
data['phone_number'] = [fake.phone_number() for i in range(rg)]
data['job'] = [fake.job() for i in range(rg)]
data['recency'] = [random.randint(1,10) for i in range(rg)]
data['history'] = [random.randint(1,700) for i in range(rg)]
data['used_discount'] = [random.randint(0,1) for i in range(rg)]
data['used_bogo'] = [random.randint(0,1) for i in range(rg)]
data['zip_code'] = [random.choice(zip_codes) for i in range(rg)]
data['is_referral'] = [random.randint(0,1) for i in range(rg)]
data['channel'] = [random.choice(channels) for i in range(rg)]
data['offer'] = [random.choice(offers) for i in range(rg)]
data['conversion'] = [random.randint(0,1) for i in range(rg)]"""
