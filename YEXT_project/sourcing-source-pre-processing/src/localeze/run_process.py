from pathlib import Path
import os
import json
import zipfile
import traceback
from download.ftp_commands import (
    sftp_download,
    curl_download,
)
from time import time
from datetime import datetime
from pipeline import pipeline_runner
from concurrent.futures import ProcessPoolExecutor
from definitions import (
    API_URL,
    TomTom_USERNAME_FOR_API,
    TomTom_PW_FOR_API,
    SFTP_HOST,
    SFTP_USERNAME,
    SFTP_PASSWORD,
    SFTP_CAN_FILEPATH,
    SFTP_CAN_FILE,
    DESTINATION_DIR,
    PRE_PROCESSED_DIR,
    NUMBER_OF_CPU,
    CnOpts_known_hosts
)
from poi_loader.poi_dl_upload import poi_dl_upload
from helpers.rm_dir_recursively import rm_dir_recursively
from slack.send_slack import send_slack_message
from helper_data.fieldnames import fieldnames


def main():
    """
    Run poi delivery process:
    1) download files from localeze api and sftp
    2) run pre-processing (country code mapping and category mapping)
    3) upload files to poi loader
    """
    ts = time()  # start time
    print("Process is started at: ", datetime.utcnow())

    send_slack_message(action="Pipeline is started", status="info")

    ps = time()  # start time of a module (e.g. downloading, pre-processing, etc.)

    try:
        # Create destination dir and pre-processed dir
        if not Path(DESTINATION_DIR).exists():
            print("Destination dir not exists, creating...")
            Path(DESTINATION_DIR).mkdir()

        if not Path(PRE_PROCESSED_DIR).exists():
            print("Pre-processed dir not exists, creating...")
            Path(PRE_PROCESSED_DIR).mkdir()

        can_file = sftp_download(
            SFTP_CAN_FILEPATH,
            Path(DESTINATION_DIR).joinpath(SFTP_CAN_FILE),
            SFTP_HOST,
            SFTP_USERNAME,
            SFTP_PASSWORD,
            CnOpts_known_hosts
        )
        with zipfile.ZipFile(can_file) as zip_ref:
            zip_ref.extractall(DESTINATION_DIR)
        can_unzipped_file = Path(DESTINATION_DIR).joinpath("iyp.txt")
        can_pre_processed_file = Path(PRE_PROCESSED_DIR).joinpath("iyp.txt")
        with open(can_pre_processed_file, 'w') as wf:
            with open(can_unzipped_file, 'r', encoding="ISO-8859-1") as rf:
                chunk_size = 1024*64
                wf.write("|".join(fieldnames))
                wf.write('\n')
                while True:
                    data = rf.read(chunk_size)
                    if not data:
                        break
                    wf.write(data)
        poi_dl_upload([can_pre_processed_file])

        usa_file = curl_download(
            "listings.json",
            DESTINATION_DIR,
            API_URL,
            TomTom_USERNAME_FOR_API,
            TomTom_PW_FOR_API
        )
        # downloaded_paths = [str(Path(DESTINATION_DIR).joinpath('localeze_sample.json'))]
        # downloaded_paths = [str(Path(DESTINATION_DIR).joinpath('listings.json'))]
        downloaded_paths = [usa_file]
        print("Downloading is completed, it took {} s...".format(time() - ps))
        send_slack_message(
            action="Downloading from supplier",
            status="success",
            msg=f"Downloading is completed, it took {time() - ps} s...",
        )

        ps = time()

        print("Pre-processing started")

        statuses = []
        # run pre-processing pipeline for downloaded files
        with ProcessPoolExecutor(max_workers=NUMBER_OF_CPU) as executor:

            for file, new_filename in zip(
                downloaded_paths, executor.map(pipeline_runner, downloaded_paths)
            ):
                try:
                    print(f"file: {file}, result: {new_filename}")
                    if new_filename == 'listings.json':
                        with open(Path(PRE_PROCESSED_DIR).joinpath(new_filename), 'rb+') as filehandle:
                            filehandle.seek(-1, os.SEEK_END)
                            filehandle.truncate()
                        with open(Path(PRE_PROCESSED_DIR).joinpath(new_filename), 'a') as filehandle:
                            filehandle.write(']')
                    if new_filename is None:
                        raise Exception(
                            f"Error in pre-processing -- {traceback.format_exc()}"
                        )
                    pre_processed_file = Path(PRE_PROCESSED_DIR).joinpath(new_filename)
                    # upload to poi loader
                    print(f"Uploading file to POI Loader -- {pre_processed_file}")
                    future = executor.submit(poi_dl_upload, [pre_processed_file])
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
