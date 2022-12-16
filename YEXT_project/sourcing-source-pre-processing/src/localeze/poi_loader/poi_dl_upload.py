""" -*- coding: utf-8 -*-
! Python3.8"""

from datetime import datetime
from pathlib import Path, PurePosixPath
from time import sleep
import requests as r
from paramiko import SSHClient
import paramiko
from definitions import (
    POI_DL_SSH_HOST,
    POI_DL_SSH_USER,
    POI_DL_SSH_PORT,
    POI_DL_SSH_KEY,
    POI_DL_SSH_UPLOADS_FOLDER,
    POI_DL_API_URL,
    SDP_SOURCE_ID_USA,
    SDP_SOURCE_ID_CAN,
    JIRA_TICKET,
    CATEGORY_MAPPING_CHOICE
)
from helper_data.usa_field_mapping import field_mapping as usa_field_mapping
from helper_data.can_field_mapping import field_mapping as can_field_mapping


def ftp_send_file(file, remote_file_path):
    """
    Send file to FTP of POI Loader.
    Returns true if file is successfully send, false if file was not sent.
    :param file: path of source file (Path object)
    :param remote_file_path: path of remote file (Path object)
    """

    print(file, remote_file_path)

    try:
        if not Path(file).exists():
            raise FileExistsError(f"File {file} does not exists")
        print(f"Uploading file to FTP: {POI_DL_SSH_HOST}, file: {file}")
        client = SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=POI_DL_SSH_HOST,
            username=POI_DL_SSH_USER,
            key_filename=POI_DL_SSH_KEY,
            port=int(POI_DL_SSH_PORT),
        )
        sftp = client.open_sftp()
        try:
            # Test if remote_path exists
            sftp.chdir(str(remote_file_path.parent))
        except IOError:
            sftp.mkdir(str(remote_file_path.parent))  # Create remote_path
        sftp.put(localpath=str(file), remotepath=str(remote_file_path))
        sftp.close()
        print(f"The file is successfully sent to FTP: {POI_DL_SSH_HOST}, file: {file}")
        return {"error": "", "success": True}
    except Exception as err:
        return {"error": err, "success": False}


def make_api_request(filename, country, api_url, sdp_source_id, field_mapping, delimiter, quote_char):
    """
    Hit poi loader's api for automatic uploads with a post request
    :param filename: names of delivery file
    :paran api_url: url of poi loader automatic api
    """

    print(f"Hitting tool api: {api_url}")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    jira_ticket = str(JIRA_TICKET)

    body = {
        "user": "piperc",
        "sdp_source_id": sdp_source_id,
        "jira_ticket": jira_ticket,
        "currency": today,
        "country_code": country,
        "category_mapping_choice": CATEGORY_MAPPING_CHOICE,
        "single_gdf_main_code": "",
        "single_gdf_code": "",
        "delimiter": delimiter,
        "quote_char": quote_char,
        "filename": filename,
        "is_prod": True,
        "skip_qc_normalization": True,
        "field_mapping": field_mapping
    }

    result = r.post(api_url, json=body)
    return result.json()


def poi_dl_upload(files):
    """
    Upload files to POI loader.
    Process:
        1) extract country codes from file names
        2) send a file to ftp
        3) hit poi loader api

    :param files: list of file paths to send to poi loader
    """

    # load envs and setup constants
    ssh_folder = POI_DL_SSH_UPLOADS_FOLDER
    api_url = POI_DL_API_URL
    today = datetime.utcnow().strftime("%Y-%m-%d")

    try:
        if len(files) == 0:
            raise FileNotFoundError("No files in uploads folder")
        else:
            for file in files:
                remote_file_path = PurePosixPath(ssh_folder).joinpath(today, file.name)
                ftp_result = ftp_send_file(file, remote_file_path)

                if not ftp_result["success"]:
                    raise Exception(
                        f'Error when sending to FTP for file: {file}, error: {ftp_result["error"]}'
                    )
                else:
                    # hit poi loader api
                    if file.name == "listings.json":
                        country = "USA"
                        quote_char = ""
                        delimiter = ""
                        field_mapping = usa_field_mapping
                        sdp_source_id = str(SDP_SOURCE_ID_USA)
                    else:
                        country = "CAN"
                        quote_char = "double_quote"
                        delimiter = "pipe"
                        field_mapping = can_field_mapping
                        sdp_source_id = str(SDP_SOURCE_ID_CAN)
                    result = make_api_request(str(file.name), country, api_url, sdp_source_id, field_mapping, delimiter, quote_char)
                    if result["status"] == "success":
                        delivery_id = result["data"]["delivery_id"]
                        print(
                            f"Successfull response from tool API for file{file}, delivery_id: {delivery_id}"
                        )
                        sleep(1)
                        return delivery_id
                    else:
                        raise Exception(
                            f"Error response from tool API for file: {file}\nError message: {result}"
                        )

    except Exception as err:
        raise Exception(f"Error during export: {err}")
