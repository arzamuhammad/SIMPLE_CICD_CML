### Step 1: Install Requirements
!bash cdsw-build.sh

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

exec(open("00_boostrap.py").read())

### Step 3: Run 01_ModelDevelopment.ipynb to develop a first baseline model

exec(open("01_A_ModelDevelopment.py").read())

# Get User Details
user_details = cml.get_user({})
user_obj = {"id": user_details["id"], 
            "username": user_details["username"],
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

# Step 5: Create Jobs for CI / CD Pipeline

create_jobs_params = {"name": "Data Ingest " + run_time_suffix,
                      "type": "cron",
                      "script": "01_B_DataIngest.py",
                      "timezone": "America/Los_Angeles",
                      "environment": {},
                      "kernel": "python3",
                      "cpu": 2,
                      "memory": 4,
                      "nvidia_gpu": 0,
                      "include_logs": True,
                      "schedule": "*/30 * * * *",
                      "notifications": [
                          {"user_id": user_obj["id"],
                           "user":  user_obj,
                           "success": False, "failure": False, "timeout": False, "stopped": False
                           }
                      ],
                      "recipients": {},
                      "attachments": [],
                      "include_logs": True,
                      "report_attachments": [],
                      "success_recipients": [],
                      "failure_recipients": [],
                      "timeout_recipients": [],
                      "stopped_recipients": []
                      }

new_job = cml.create_job(create_jobs_params)
new_job_id = new_job["id"]
print("Created new job with jobid", new_job_id)

### Step 5: Create and Schedule two CML Jobs for 
# 02_PredictPipeline.py, 03_A_RetrainPipeline_PySpark.py and 03_B_RetrainPipeline_Sklearn.py

# Create Job
create_jobs_params = {"name": "Customer Scoring Job",
                      "type": "cron",
                      "script": "02_PredictPipeline.py",
                      "timezone": "America/Los_Angeles",
                      "environment": {},
                      "kernel": "python3",
                      "cpu": 2,
                      "memory": 4,
                      "nvidia_gpu": 0,
                      "include_logs": True,
                      "schedule": "*/45 * * * *",
                      "notifications": [
                          {"user_id": user_obj["id"],
                           "user":  user_obj,
                           "success": False, "failure": False, "timeout": False, "stopped": False
                           }
                      ],
                      "recipients": {},
                      "attachments": [],
                      "include_logs": True,
                      "report_attachments": [],
                      "success_recipients": [],
                      "failure_recipients": [],
                      "timeout_recipients": [],
                      "stopped_recipients": []
                      }

parent_job = cml.create_job(create_jobs_params)
parent_job_id = parent_job["id"]
print("Created new job with jobid", parent_job_id)

# Create Job
create_jobs_params = {"name": "Retrain PySPark Model Job - Model Scoring via Database",
                      "script": "03_A_RetrainPipeline_PySpark.py",
                      "timezone": "America/Los_Angeles",
                      "environment": {},
                      "kernel": "python3",
                      "cpu": 2,
                      "memory": 4,
                      "nvidia_gpu": 0,
                      "include_logs": True,
                      "notifications": [
                          {"user_id": user_obj["id"],
                           "user":  user_obj,
                           "success": False, "failure": False, "timeout": False, "stopped": False
                           }
                      ],
                      "type": "cron",
                      "schedule": "*/50 * * * *",
                      "recipients": {},
                      "attachments": [],
                      "include_logs": True,
                      "report_attachments": [],
                      "success_recipients": [],
                      "failure_recipients": [],
                      "timeout_recipients": [],
                      "stopped_recipients": []
                      }

new_job = cml.create_job(new_params)
new_job_id = new_job["id"]
print("Created new job with jobid", new_job_id)


# Create Job
create_jobs_params = {"name": "Retrain Sklearn Model Job - Model Serving via REST API",
                      "script": "03_B_RetrainPipeline_Sklearn.py",
                      "timezone": "America/Los_Angeles",
                      "environment": {},
                      "kernel": "python3",
                      "cpu": 2,
                      "memory": 4,
                      "nvidia_gpu": 0,
                      "include_logs": True,
                      "notifications": [
                          {"user_id": user_obj["id"],
                           "user":  user_obj,
                           "success": False, "failure": False, "timeout": False, "stopped": False
                           }
                      ],
                      "type": "cron",
                      "schedule": "*/50 * * * *",
                      "recipients": {},
                      "attachments": [],
                      "include_logs": True,
                      "report_attachments": [],
                      "success_recipients": [],
                      "failure_recipients": [],
                      "timeout_recipients": [],
                      "stopped_recipients": []
                      }

new_job = cml.create_job(new_params)
new_job_id = new_job["id"]
print("Created new job with jobid", new_job_id)

### Step 6: Run model tracking scripts

time.sleep(60)

exec(open("05_A_WarmUpModel.py").read())

exec(open("05_B_ModelReporting.py").read())
