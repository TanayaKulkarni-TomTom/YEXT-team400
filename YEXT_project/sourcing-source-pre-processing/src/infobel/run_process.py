import json
from pathlib import Path
import os
import sys
import traceback
from download.ftp_commands import (
    FtpDeliveryFilesNotFoundException,
    filter_infobel_files,
    curl_ftp_download,
    get_list_of_files,
)
from time import time
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
from poi_loader.upload_infobel_ftp import poi_dl_upload
from helpers.rm_dir_recursively import rm_dir_recursively
from slack.send_slack import send_slack_message
import helpers.app_logger as app_logger

logger = app_logger.get_logger(__name__)


def exception_handler(type, value, tb):
    """A function to route uncaught exceptions to logger

    Args:
        type ([type]): [error type]
        value ([type]): [error value]
        tb ([type]): [traceback object]
    """
    logger.exception(
        f"Uncaught exception: {type}, {value},\n {traceback.format_tb(tb)}"
    )


# Hook exception handler to log all uncaught exceptions
sys.excepthook = exception_handler


def main():
    """
    Run poi delivery process:
    1) download files from infobel ftp
    2) run pre-processing (country code mapping and category mapping)
    3) upload files to poi loader
    """
    ts = time()  # start time
    logger.info("Process is started")

    send_slack_message(action="Pipeline is started", status="info")

    ps = time()  # start time of a module (e.g. downloading, pre-processing, etc.)

    try:
        # get list of files
        all_files = get_list_of_files(HOST, USERNAME, PASSWORD, FTP_DIR)
        infobel_files = filter_infobel_files(all_files)
        if not infobel_files:
            raise FtpDeliveryFilesNotFoundException()

        # Create destination dir and pre-processed dir
        if not Path(DESTINATION_DIR).exists():
            logger.info("Destination dir not exists, creating...")
            Path(DESTINATION_DIR).mkdir()

        if not Path(PRE_PROCESSED_DIR).exists():
            logger.info("Pre-processed dir not exists, creating...")
            Path(PRE_PROCESSED_DIR).mkdir()

        downloaded_paths = []
        # downloading files from infobel's ftp
        for file in infobel_files:
            try:
                downloaded_path = ""
                downloaded_path = curl_ftp_download(
                    filepath=file,
                    dest_dir=DESTINATION_DIR,
                    host=HOST,
                    user=USERNAME,
                    password=PASSWORD,
                )
                if not downloaded_path:
                    raise Exception("File not downloaded")
                downloaded_paths.append(downloaded_path)

            except Exception as error:
                logger.exception(f"Failed to download: {error}")
                send_slack_message(
                    action="Downloading from supplier",
                    status="error",
                    msg="Failed to download file:\n"
                    + f"File --- {file}\n"
                    + f"Error --- {error}\n"
                    + f"Traceback --- {traceback.format_exc()}",
                )

        logger.info("Downloading is completed, it took {} s...".format(time() - ps))
        send_slack_message(
            action="Downloading from supplier",
            status="success",
            msg=f"Downloading is completed, it took {time() - ps} s...",
        )

        ps = time()

        logger.info("Pre-processing started")

        statuses = []
        # run pre-processing pipeline for downloaded files
        with ProcessPoolExecutor(max_workers=NUMBER_OF_CPU) as executor:

            for file, new_filename in zip(
                downloaded_paths, executor.map(pipeline_runner, downloaded_paths)
            ):
                try:
                    logger.info(f"file: {file}, result: {new_filename}")
                    if new_filename is None:
                        raise Exception(
                            f"Error in pre-processing -- {traceback.format_exc()}"
                        )
                    pre_processed_file = Path(PRE_PROCESSED_DIR).joinpath(new_filename)
                    # upload to poi loader
                    logger.info(f"Uploading file to POI Loader -- {pre_processed_file}")
                    future = executor.submit(poi_dl_upload, [pre_processed_file])
                    delivery_id = future.result()

                    logger.info(
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
                    logger.exception(
                        f"Failed to upload to POI Loader -- {new_filename}, {error}"
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

        logger.info(
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
        logger.info(
            "Pipeline is finished"
            + f"Total pipeline time: {time() - ts}\n"
            + json.dumps(totals)
        )

        # Shutdown the machine
        os.system("sudo shutdown now -h")

    except Exception as error:
        send_slack_message(
            action="Error in pipeline",
            status="error",
            msg=f"{error}\n" + f"{traceback.format_exc()}",
        )
        logger.exception(error)


if __name__ == "__main__":
    main()
