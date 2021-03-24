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
  
# This will run the data ingest file. You need this to create the hive table from the 
# csv file.
#exec(open("00_boostrap.py").read())

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

## Exerc

