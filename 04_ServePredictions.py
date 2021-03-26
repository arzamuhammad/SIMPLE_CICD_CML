import pandas as pd
import numpy as np
import joblib
import sklearn
import os
from utils.get_latest_models import *


model_path = find_latest("sklearn_model")

clf = joblib.load(model_path)

def predict(data):
  
#Sample input
#{"RECENCY":11, 
#         "HISTORY":204,
#         "USED_DISCOUNT":0,
#         "USED_BOGO":1,
#         "IS_REFERRAL":1, 
#         "SCORE":0.766}
#Sample Output
#{"result": 1}

    df = pd.DataFrame(data, index=[0])
    
    df.columns = ['recency', 'history', 'used_discount', 'used_bogo', 'is_referral', 'channel', 'offer']
    
    df['recency'] = df['recency'].astype(float)
    df['history'] = df['history'].astype(float)
    df['used_discount'] = df['used_discount'].astype(float)
    df['used_bogo'] = df['used_bogo'].astype(float)
    df['is_referral'] = df['is_referral'].astype(float)
    
    return {'result': clf.predict(df)[0]}


