import pandas as pd
import numpy as np
import joblib
import sklearn
import os
import cdsw
from utils.get_latest_models import *

model_path = find_latest("sklearn_model")

clf = joblib.load(model_path)

##### Model Tracking Decorator - use to track model with PostgresDB
@cdsw.model_metrics
def predict(data):
  
# Sample input
# {"RECENCY":11, 
#         "HISTORY":204,
#         "USED_DISCOUNT":0,
#         "USED_BOGO":1,
#         "IS_REFERRAL":1, 
#         "SCORE":0.766}
#
# Sample Output
# {"result": 1}

    df = pd.DataFrame(data, index=[0])
    
    df.columns = ['recency', 'history', 'used_discount', 'used_bogo', 'is_referral', 'score']

    df = df.astype('float')
    
    prediction = float(clf.predict(df)[0])
    probability = float(clf.predict_proba(df)[0][1])
    
    # Track model metrics
    cdsw.track_metric('prediction', prediction)
    cdsw.track_metric('conversion_probability', probability)
  
    
    return {'prediction': prediction, 
            'conversion_probability': probability}


