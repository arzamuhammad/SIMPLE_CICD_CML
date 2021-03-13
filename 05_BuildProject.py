### Step 1: Install Requirements

!pip3 install -r requirements.txt


from cmlbootstrap import CMLBootstrap
import datetime
import os, time

HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY") 
PROJECT_NAME = os.getenv("CDSW_PROJECT") 

# Instantiate API Wrapper
# Passing API key directly is better
cml = CMLBootstrap(HOST, USERNAME, os.environ["MY_API_KEY"], PROJECT_NAME)


# Get Project Details
project_details = cml.get_project({})
project_id = project_details["id"]

run_time_suffix = datetime.datetime.now()
run_time_suffix = run_time_suffix.strftime("%d%m%Y%H%M%S")

### Step 2: Run 00_bootstrap.py to create Spark table





### Step 3: Run 01_ModelDevelopment.ipynb to develop a first baseline model

### Step 4: Create a CML Job for 01_A_DataIngest.py

### Step 5: Create and Schedule two CML Jobs for 02_PredictPipeline.py and 03_RetrainPipeline.py

### Optional: Push 04_Dashboard.py to a CML Application