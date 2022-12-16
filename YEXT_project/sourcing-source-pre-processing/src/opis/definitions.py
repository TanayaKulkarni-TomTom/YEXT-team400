import os
from dotenv import load_dotenv
from os import cpu_count, environ as env

load_dotenv()

# This is your Project Root
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# This are env variables from .env file
# See .env_template for more info
URL_FOR_INDEX_FILE = env["URL_FOR_INDEX_FILE"]
TomTom_PW_FOR_DATA = env["TomTom_PW_FOR_DATA"]
TomTom_USERNAME_FOR_DATA = env["TomTom_USERNAME_FOR_DATA"]

DESTINATION_DIR = env["DESTINATION_DIR"]
PRE_PROCESSED_DIR = env["PRE_PROCESSED_DIR"]
PRE_PROCESSED_FILE_EXTENSION = env["PRE_PROCESSED_FILE_EXTENSION"]

SDP_SOURCE = env["SDP_SOURCE"]
JIRA_TICKET = env["JIRA_TICKET"]

POI_DL_SSH_HOST = env["POI_DL_SSH_HOST"]
POI_DL_SSH_USER = env["POI_DL_SSH_USER"]
POI_DL_SSH_PORT = env["POI_DL_SSH_PORT"]
POI_DL_SSH_KEY = env["POI_DL_SSH_KEY"]
POI_DL_SSH_UPLOADS_FOLDER = env["POI_DL_SSH_UPLOADS_FOLDER"]
POI_DL_API_URL = env["POI_DL_API_URL"]
SLACK_WEBHOOK_URL = env["SLACK_WEBHOOK_URL"]

CATEGORY_MAPPING_CHOICE = env["CATEGORY_MAPPING_CHOICE"]
SINGLE_GDF_MAIN_CODE = env["SINGLE_GDF_MAIN_CODE"]
SINGLE_GDF_CODE = env["SINGLE_GDF_CODE"]

DELIMITER = env["DELIMITER"]
QUOTE_CHAR = env["QUOTE_CHAR"]

SCHEDULER_DAY = env["SCHEDULER_DAY"]
SCHEDULER_HOUR = env["SCHEDULER_HOUR"]
SCHEDULER_MINUTE = env["SCHEDULER_MINUTE"]

PIPELINE_NAME = env["PIPELINE_NAME"]

# Number of cpu
NUMBER_OF_CPU = 1
if cpu_count() != 1:
    NUMBER_OF_CPU = cpu_count() - 1
else:
    NUMBER_OF_CPU = cpu_count()
