import json
from pathlib import Path
import os
import traceback
from download.ftp_commands import (
    FtpDeliveryFilesNotFoundException,
    filter_yext_files,
    curl_ftp_download,
    get_list_of_files,
)
from download.sftp_download import(
    sftp_filepath
)
from time import time
from datetime import datetime
from pipeline import pipeline_runner
from concurrent.futures import ProcessPoolExecutor
from definitions import (
    HOST,
    USERNAME,
    PASSWORD,
    FTP_DIR,
    DESTINATION_DIR,
    PRE_PROCESSED_DIR,
    NUMBER_OF_CPU,
)
from poi_loader.upload_yext_ftp import poi_dl_upload
from helpers.rm_dir_recursively import rm_dir_recursively
from slack.send_slack import send_slack_message


def main():
    """
    Run poi delivery process:
    1) download files from yext ftp
    2) run pre-processing (country code mapping and category mapping)
    3) upload files to poi loader
    """
    ts = time()  # start time
    print("Process is started at: ", datetime.utcnow())

    send_slack_message(action="Pipeline is started", status="info")

    ps = time()  # start time of a module (e.g. downloading, pre-processing, etc.)

    try:
        # get list of files
        #all_files = get_list_of_files(HOST, USERNAME, PASSWORD, FTP_DIR)
        #yext_files = filter_yext_files(all_files)
        #if not yext_files:
         #   raise FtpDeliveryFilesNotFoundException()

        # Create destination dir and pre-processed dir
        if not Path(DESTINATION_DIR).exists():
            print("Destination dir not exists, creating...")
            Path(DESTINATION_DIR).mkdir()

        if not Path(PRE_PROCESSED_DIR).exists():
            print("Pre-processed dir not exists, creating...")
            Path(PRE_PROCESSED_DIR).mkdir()

        #Create a variable yext_file which will have output of curl command
        yext_file = "" 
        file=FTP_DIR

        downloaded_paths = []
        # downloading files from yext's ftp
        #for file in yext_files:
        try:
                
                downloaded_path = sftp_filepath() #calls method in the Download directory that returns the sftp filepath in
                #call it here
                if not downloaded_path:
                    raise Exception("File not downloaded")
                #downloaded_paths.append(downloaded_path)
                #yext_file= downloaded_path

        except Exception as error:
                print("Failed to download: ", error, traceback.format_exc(limit=1))
                send_slack_message(
                    action="Downloading from supplier",
                    status="error",
                    msg="Failed to download file:\n"
                    + f"File --- {file}\n"
                    + f"Error --- {error}\n"
                    + f"Traceback --- {traceback.format_exc()}",
                )

        print("Downloading is completed, it took {} s...".format(time() - ps))
        send_slack_message(
            action="Downloading from supplier",
            status="success",
            msg=f"Downloading is completed, it took {time() - ps} s...",
        )
        # downloaded_paths = [
        #     PosixPath('raw-deliveries/TomTom_Canada_Business_Database_November_2022.zip'),
        #     PosixPath('raw-deliveries/TomTom_USA_Business_Database_November_2022.zip')
        # ]

        ps = time()

        print("Pre-processing started")

        statuses = []
        # run pre-processing pipeline for downloaded files
        with ProcessPoolExecutor(max_workers=NUMBER_OF_CPU) as executor:

            for file, new_filename in zip(
                downloaded_path, executor.map(pipeline_runner, [downloaded_path])
            ):
                try:
                    print(f"file: {file}, result: {new_filename}")
                    if new_filename is None:
                        raise Exception(
                            f"Error in pre-processing -- {traceback.format_exc()}"
                        )
                    # upload to poi loader
                    print(f"Uploading file to POI Loader -- {new_filename}")
                    future = executor.submit(poi_dl_upload, [new_filename])
                    delivery_id = future.result()

                    print(
                        f"File successfully uploaded to POI Loader, delivery_id -- {delivery_id}"
                    )
                    send_slack_message(
                        action="Uploading to POI loader",
                        status="success",
                        msg=f"Delivery id -- {delivery_id}\n"
                        + f"File -- {new_filename}",
                    )
                    statuses.append(
                        {
                            "file": new_filename,
                            "delivery_id": delivery_id,
                            "status": "success",
                        }
                    )
                except Exception as error:
                    print(
                        f"Failed to upload to POI Loader -- {new_filename}, {error}, {traceback.format_exc()}"
                    )
                    send_slack_message(
                        action="Uploading to POI loader",
                        status="error",
                        msg="Failed to upload file to POI Loader:\n"
                        + f"File -- {new_filename}\n"
                        + f"Error traceback -- {json.dumps(traceback.format_exc())}",
                    )
                    statuses.append(
                        {
                            "file": new_filename,
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
