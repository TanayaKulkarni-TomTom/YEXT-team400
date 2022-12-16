import os
from dotenv import load_dotenv
from os import cpu_count, environ as env


try:
    load_dotenv()

    # This is your Project Root
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

    # This are env variables from .env file
    # See .env_template for more info
    HOST = env["FTP_HOST"]
    USERNAME = env["FTP_USERNAME"]
    PASSWORD = env["FTP_PASSWORD"]
    FTP_DIR = env["FTP_DIR"]

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
except Exception as error:
    raise error
