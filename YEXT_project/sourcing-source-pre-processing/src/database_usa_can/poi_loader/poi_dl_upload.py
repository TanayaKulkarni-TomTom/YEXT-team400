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
    SDP_SOURCE,
    JIRA_TICKET,
    CATEGORY_MAPPING_CHOICE,
    DELIMITER,
    QUOTE_CHAR
)


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


def make_api_request(filename, country, api_ulr):
    """
    Hit poi loader's api for automatic uploads with a post request
    :param filename: names of delivery file
    :param country: country code of delivery
    :paran api_url: url of poi loader automatic api
    """

    print(f"Hitting tool api: {api_ulr}")
    today = datetime.utcnow().strftime("%Y-%m-%d")

    body = {
        "user": "piperc",
        "sdp_source_id": str(SDP_SOURCE),
        "jira_ticket": str(JIRA_TICKET),
        "currency": today,
        "country_code": country,
        "category_mapping_choice": CATEGORY_MAPPING_CHOICE,
        "single_gdf_main_code": "",
        "single_gdf_code": "",
        "delimiter": DELIMITER,
        "quote_char": QUOTE_CHAR,
        "filename": filename,
        "is_prod": True,
        "skip_qc_normalization": True,
        "field_mapping": {
            "poi_id": ["Business_ID"],
            "poi_name": ["Company_Name"],
            "translated_poi_name": [],
            "latitude": ["Latitude"],
            "longitude": ["Longitude"],
            "address": ["Address"],
            "streetname": ["Street_Name"],
            "house_number": ["Address_Number"],
            "city": ["City"],
            "country_code_char3": ["country_from_user"],
            "country_full_name": [],
            "state": ["State"],
            "category_code": ["SIC01_Code"],
            "category_description": ["SIC01_Description"],
            "subcategory_code": [],
            "subcategory_description": [],
            "poi_photos": [],
            "poi_score": [],
            "poi_score_raw": [],
            "phone": ["Phone"],
            "fax": ["Fax"],
            "contact_email": ["Company_Email"],
            "url": ["URL_Company_Full"],
            "ratings": [],
            "poi_out_of_business": [],
            "poi_reviews": [],
            "poi_price_range": [],
            "poi_name_lang_code": [],
            "poi_name2": [],
            "poi_name_lang_code2": [],
            "poi_name3": [],
            "poi_name_lang_code3": [],
            "poi_name4": [],
            "poi_name_lang_code4": [],
            "house_number_addition": [],
            "address_lang_code": [],
            "address2": [],
            "address_lang_code2": [],
            "address3": [],
            "address_lang_code3": [],
            "address4": [],
            "address_lang_code4": [],
            "postal_code": [],
            "postal_code_sub": [],
            "city_lang_code": [],
            "city2": [],
            "city_lang_code2": [],
            "city3": [],
            "citylangcode3": [],
            "city4": [],
            "city_lang_code4": [],
            "administrative_area": [],
            "brand1": [],
            "opening_hours": [],
            "parking_size": [],
            "parking_type": [],
            "lpg_6f": [],
            "diesel_6f": [],
            "petrol_6f": [],
            "cng_6f": [],
            "adblue_6f": [],
            "dieselcom_6f": [],
            "e85_6f": [],
            "biodiesel_6f": [],
            "hydrogen_6f": [],
            "propane_6f": [],
            "sf_hotel_motel": [],
            "sf_parking_facility": [],
            "sf_petrol_station": [],
            "sf_restaurant": [],
            "sf_wc": [],
            "sf_kiosk": [],
            "sf_wifi": [],
            "sf_men_and_women_showers": [],
            "sf_laundry_facilities": [],
            "sf_repair_facility": [],
            "sf_picnic_area": [],
            "sf_refreshments": [],
            "sf_drinking_water": [],
            "sf_fire_place": [],
            "sf_public_phone": [],
            "sf_emergency_phone": [],
            "sf_camping_area": [],
            "sf_caravan_park": [],
            "sf_dump_station": [],
            "sf_first_aid": [],
            "sf_wheelchair_wc": [],
            "sf_wheelchair_access": [],
            "sf_sum": [],
            "tf_unspecified": [],
            "tf_secured": [],
            "tf_reservable": [],
            "tf_hazmant_allowed": [],
            "tf_power_refrigerated_trucks": [],
            "tf_sum": [],
            "payment_method1": [],
            "payment_detail1": [],
            "payment_method2": [],
            "payment_detail2": [],
            "payment_method3": [],
            "payment_detail3": [],
            "payment_method4": [],
            "payment_detail4": [],
            "payment_method5": [],
            "payment_detail5": [],
            "vt_passenger_car": [],
            "vt_medium_truck": [],
            "vt_heavy_truck": [],
            "twenty4_hrs_er_room_or_service": [],
            "drive_through_service": [],
            "generic_attribute": [],
            "fid": ["fid"],
            "supplier_geocoding_level": [],
            "raw_category_code": [],
            "raw_category_description": [],
            "raw_subcategory_code": [],
            "raw_subcategory_description": [],
            "raw_category_column": []
        },
    }

    result = r.post(api_ulr, json=body)
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
                    if "USA" in str(file.name):
                        country = "USA"
                    else:
                        country = "CAN"
                    result = make_api_request(str(file.name), country, POI_DL_API_URL)
                    if result["status"] == "success":
                        delivery_id = result["data"]["delivery_id"]
                        print(
                            f"Successful response from tool API for file{file}, delivery_id: {delivery_id}"
                        )
                        sleep(1)
                        return delivery_id
                    else:
                        raise Exception(
                            f"Error response from tool API for file: {file}\nError message: {result}"
                        )

    except Exception as err:
        raise Exception(f"Error during export: {err}")
