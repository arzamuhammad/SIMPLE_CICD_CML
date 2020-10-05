import pandas as pd

#pd.read_csv('data/historical.csv')['offer'].unique()


import boto3

s3 = boto3.client('s3')  

### ADD HERE

"""from boto.s3.connection import S3Connection
aws_connection = S3Connection()

bucket = aws_connection.get_bucket('demo-aws-1')
#key = bucket.get_key('faithful.csv')

for key in bucket.list(prefix='datalake/pdefusco/'):
  print(key)"""

import boto3

s3 = boto3.resource('s3')
my_bucket = s3.Bucket('demo-aws-1')

models = []
for object_summary in my_bucket.objects.filter(Prefix="datalake/pdefusco/"):
    models.append(object_summary.key.split('/')[3])
    
models.