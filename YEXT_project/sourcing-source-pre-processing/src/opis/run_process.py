from pathlib import Path
import os
import json
import traceback
from download.ftp_commands import (
    get_files_from_index,
    curl_download,
)
from time import time
from datetime import datetime
from pipeline import pipeline_runner
from concurrent.futures import ProcessPoolExecutor
from definitions import (
    URL_FOR_INDEX_FILE,
    TomTom_USERNAME_FOR_DATA,
    TomTom_PW_FOR_DATA,
    DESTINATION_DIR,
    PRE_PROCESSED_DIR,
    NUMBER_OF_CPU,
)
from poi_loader.poi_dl_upload import poi_dl_upload
from helpers.rm_dir_recursively import rm_dir_recursively
from slack.send_slack import send_slack_message
from helpers.helpers import get_countries_mapping


def main():
    """
    Run poi delivery process:
    1) download files from opis
    2) run pre-processing (country code mapping and category mapping)
    3) upload files to poi loader
    """
    ts = time()  # start time
    print("Process is started at: ", datetime.utcnow())

    send_slack_message(action="Pipeline is started", status="info")

    ps = time()  # start time of a module (e.g. downloading, pre-processing, etc.)

    try:
        # Download index file. (File with all files)
        downloaded_path = ""
        downloaded_path = curl_download(
            filename='index.xml',
            dest_dir="./",
            host=URL_FOR_INDEX_FILE,
            user=TomTom_USERNAME_FOR_DATA,
            password=TomTom_PW_FOR_DATA
        )
        if not downloaded_path:
            raise Exception("Index File is not downloaded")

        countries_mapping = get_countries_mapping()
        filenames_url_country = get_files_from_index(downloaded_path, countries_mapping)

        # Create destination dir and pre-processed dir
        if not Path(DESTINATION_DIR).exists():
            print("Destination dir not exists, creating...")
            Path(DESTINATION_DIR).mkdir()

        if not Path(PRE_PROCESSED_DIR).exists():
            print("Pre-processed dir not exists, creating...")
            Path(PRE_PROCESSED_DIR).mkdir()

        downloaded_paths = []
        countries = {}  # This will have downloaded_path as key and country as value. Used later in ProcessPool
        for filename, delivery_url, country in filenames_url_country:
            try:
                downloaded_path = curl_download(
                    filename=filename,
                    dest_dir=DESTINATION_DIR,
                    host=delivery_url,
                    user=TomTom_USERNAME_FOR_DATA,
                    password=TomTom_PW_FOR_DATA
                )
                if not downloaded_path:
                    raise Exception(f"File {filename} not downloaded")
                downloaded_paths.append(downloaded_path)
                countries[downloaded_path] = country
            except Exception as error:
                print("Failed to download: ", error, traceback.format_exc(limit=1))
                send_slack_message(
                    action="Downloading from supplier",
                    status="error",
                    msg="Failed to download file:\n"
                    + f"File --- {filename}\n"
                    + f"Error --- {error}\n"
                    + f"Traceback --- {traceback.format_exc()}",
                )

        print("Downloading is completed, it took {} s...".format(time() - ps))
        send_slack_message(
            action="Downloading from supplier",
            status="success",
            msg=f"Downloading is completed, it took {time() - ps} s...",
        )

        ps = time()

        print("Pre-processng started")

        statuses = []
        # run preprocessing pipeline for downloaded files
        with ProcessPoolExecutor(max_workers=NUMBER_OF_CPU) as executor:
            for file, pre_processed_file in zip(
                downloaded_paths, executor.map(pipeline_runner, downloaded_paths)
            ):
                try:
                    print(f"file: {file}, result: {pre_processed_file}")
                    # upload to poi loader
                    print(f"Uploading file to POI Loader -- {pre_processed_file}")
                    future = executor.submit(poi_dl_upload, [Path(pre_processed_file)], [countries[file]])
                    delivery_id = future.result()
                    print(
                        f"File successfully uploaded to POI Loader, delivery_id -- {delivery_id}"
                    )
                    send_slack_message(
                        action="Uploading to POI loader",
                        status="success",
                        msg=f"Delivery id -- {delivery_id}\n"
                        + f"File -- {pre_processed_file}",
                    )
                    statuses.append(
                        {
                            "file": pre_processed_file,
                            "delivery_id": delivery_id,
                            "status": "success",
                        }
                    )
                except Exception as error:
                    print(
                        f"Failed to upload to POI Loader -- {pre_processed_file}, {error}, {traceback.format_exc()}"
                    )
                    send_slack_message(
                        action="Uploading to POI loader",
                        status="error",
                        msg="Failed to upload file to POI Loader:\n"
                        + f"File -- {pre_processed_file}\n"
                        + f"Error traceback -- {json.dumps(traceback.format_exc())}",
                    )
                    statuses.append(
                        {
                            "file": pre_processed_file,
                            "delivery_id": "",
                            "status": "failed",
                        }
                    )
        print(
            "Pre-processing and uploading to poi loader is completed, it took {} s...".format(
                time() - ps
            )
        )
        send_slack_message(
            action="Pre-processing and uploading to poi loader is completed",
            status="info",
            msg=f"it took {time() - ps}",
        )

        # If all files got delivery id - remove all files
        remove_files_flag = True
        for index, item in enumerate(statuses):
            if item["status"] == "failed":
                remove_files_flag = False
                break

        if remove_files_flag:
            # clean upload dir
            if Path(DESTINATION_DIR).exists():
                rm_dir_recursively(DESTINATION_DIR)
            # clean pre-processed dir
            if Path(PRE_PROCESSED_DIR).exists():
                rm_dir_recursively(PRE_PROCESSED_DIR)

        # Calculate total stats
        totals = {
            "success_files": sum(s["status"] == "success" for s in statuses),
            "error_files": sum(s["status"] == "failed" for s in statuses),
        }

        send_slack_message(
            action="Pipeline is finished",
            status="success",
            msg=f"Total pipeline time: {time() - ts}\n" + json.dumps(totals),
        )
        print(
            "Pipeline is finished"
            + f"Total pipeline time: {time() - ts}\n"
            + json.dumps(totals)
        )

        # Shutdown the machine if all deliveries were successfully sent
        if remove_files_flag:
            os.system("sudo shutdown now -h")

    except Exception as error:
        send_slack_message(
            action="Error in pipeline",
            status="error",
            msg=f"{error}\n" + f"{traceback.format_exc()}",
        )
        print(error)


if __name__ == "__main__":
    main()
