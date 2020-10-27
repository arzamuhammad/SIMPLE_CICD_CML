# CI / CD for Database Scoring in Cloudera Machine Learning with PySpark Pipelines


![alt text](https://github.com/pdefusco/myimages_repo/blob/main/Simple%20CI_CD%20in%20CML.jpeg)


## INSTRUCTIONS FOR USE

##### 0. Clone this github repository

- Create a new CML blank project (do not create from github)
- In the new project, start a Workbench editor session, open the terminal, and execute the following command:
"git clone https://github.com/pdefusco/Simple_CICD_CML.git"


##### 1. Open a Workbench editor session and run 00_bootstrap.py

- This step will create and prepopulate a Spark Table with historical customer interactions.
- The data contains past customer interactions. The conversion attribute is the target for the Binary Classifier.
- We want to predict wether if a customer will buy or not based on a marketing offer.


##### 2. Open a Jupyter Notebooks session and run 01_ModelDevelopment.ipynb

- This notebook is the development area for the Data Scientist. It is where he/she research and tune a model.
- Notice the classifier is not tuned nor evaluated for bias/overfitting. This is something you can optionally do with another session or experiments.
- This notebook outputs a PySpark pipeline and model object to Cloud Storage. Metadata related to each is saved in a sqlite3 table.


##### 3. Create a CML Job and run 01_DataIngest.py. Schedule this to run every hour

- This script will generate some new customer interactions and load them into our Spark customer table in batch.
- Notice the target attribute is null. This is wanted as it will be our job to predict customer reactions to offers.


##### 4. Create a CML Job with the 02_PredictPipeline.py script

- Set execution on an hourly basis but at a time later than the scheduled time for 01_DataIngest.py.
- This script imports the PySpark model you tuned in step 2.
- It then makes predictions on the new customer data.


##### 5. Create a CML Job with the 03_RetrainPipeline.py script

- Set execution as dependent upon the completion of the job in the prior step.
- This script imports the PySpark pipeline you created in step 2.
- It then retrains this pipeline on the new customer data. 
- NB: in a real world scenario, you'd have to schedule a waiting time to allow for customer responses to come into the customer table.


10/27/20 WIP items
1. There is no dashboard yet
2. The model in 01_ModelDevelopment.ipynb is not tuned. I will be working on a custom oversampling stage to improve that model.
