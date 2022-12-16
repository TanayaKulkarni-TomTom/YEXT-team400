from ftplib import FTP
from pathlib import Path
from threading import Thread
import subprocess
import re
import traceback
import helpers.app_logger as app_logger

logger = app_logger.get_logger(__name__)


def ftp_download(filepath, ftp_dir, dest_dir, host, user, password):
    """
    Download file from ftp using ftplib
    """
    try:
        print(f"Downloading from: sftp://{host}/{filepath}'")
        filename = Path(filepath).name
        with FTP(host, user, password) as ftp_client:
            ftp_client.cwd(ftp_dir)
            dest_file_path = Path(dest_dir).joinpath(filename)
            with open(dest_file_path, "wb") as file:
                ftp_client.retrbinary("RETR {}".format(filepath), file.write)
            print(f"File downloaded: sftp://{host}/{filepath}")
            return dest_file_path
    except Exception as error:
        raise FtpDownloadException(filepath, host, f"{traceback.format_exc()}, {error}")


class GetFilesFromFtp(Thread):
    """
    Class for multithreaded downloading from FTP
    """

    def __init__(self, filename, ftp_dir, dest_dir, host, user, password):
        super().__init__()
        self.filename = filename
        self.ftp_dir = ftp_dir
        self.dest_dir = dest_dir
        self.host = host
        self.user = user
        self.password = password

    def run(self):
        logger.info("Downloading from /{}/{}".format(self.ftp_dir, self.filename))
        ftp_download(
            self.filename,
            self.ftp_dir,
            self.dest_dir,
            self.host,
            self.user,
            self.password,
        )


def wget_ftp_download(filepath, dest_dir, host, user, password):
    """
    Download file from ftp using wget.
    Returning path of downloaded file in destination folder.
    :param filepath: path to file on ftp
    :param dest_dir: path to destination directory
    :param host: ftp host
    :param user: ftp user
    :param password: ftp password
    """
    try:
        cmd = [
            "wget",
            f'--user="{user}"',
            f'--password="{password}"',
            "-P",
            dest_dir,
            f"sftp://{host}/{filepath}",
        ]
        print(f"Downloading from: sftp://{host}/{filepath}'")

        ftp_download_process = subprocess.run(cmd)
        if ftp_download_process.returncode != 0:
            raise FtpDownloadException(
                filepath,
                host,
                str(traceback.format_exc() + "\n" + str(ftp_download_process.stderr)),
            )
        print(f"File downloaded: sftp://{host}/{filepath}")
        return Path(dest_dir).joinpath(filepath)
    except Exception:
        raise


def curl_ftp_download(filepath, dest_dir, host, user, password):
    """
    Download file from ftp using curl.
    Returning path of downloaded file in destination folder.
    :param filepath: path to file on ftp
    :param dest_dir: path to destination directory
    :param host: ftp host
    :param user: ftp user
    :param password: ftp password
    """
    try:
        filename = Path(filepath).name
        dest_path = Path(dest_dir).joinpath(filename)
        dest_path = str(dest_path)
        cmd = [
            "curl",
            "-s",
            f"-u {user}:{password}",
            f"sftp://{host}/{filepath}",
            "-o",
            dest_path,
        ]
        logger.info(f"Command: {''.join(cmd)}")
        cmd = " ".join(cmd)
        print(f"Downloading from: sftp://{host}/{filepath}'")

        ftp_download_process = subprocess.run(cmd, shell=True)

        if ftp_download_process.returncode != 0:
            raise FtpDownloadException(
                filepath=filepath,
                host=host,
                message=str(
                    traceback.format_exc() + "\n" + str(ftp_download_process.stderr)
                ),
            )
        print(f"File downloaded: sftp://{host}/{filepath}")
        return dest_path
    except:
        raise


def get_list_of_files(host, username, password, dir):
    """
    Return list of files from root ftp directory of the host
    """
    logger.info("Lisitng FTP direcory")
    files = []
    with FTP(host, username, password) as ftp_client:
        files = ftp_client.nlst(dir)
        if files:
            logger.info(files)
            return files
        else:
            message = "No files found"
            raise FtpGetListOfFilesException(host=host, message=message)


def filter_yext_files(files):
    """
    Filter yext files from list of files obtained from ftp folder
    """
    print("Filtering Yext filenames")
    pattern = r"^TomTom_.+_.+\.txt\.zip$"
    filtered_files = [
        file for file in files if len(re.findall(pattern, Path(file).name)) != 0
    ]
    return filtered_files


class FtpGetListOfFilesException(Exception):
    """
    Exception raised for errors when listing files in ftp directory
    """

    def __init__(self, host, message="Error when listing files"):
        self.message = message
        self.host = host

    def __str__(self):
        return f"{self.message} -> " + f"ftp://{self.host}/"


class FtpDeliveryFilesNotFoundException(Exception):
    """
    Exception raised when no delivery files found in ftp folder of supplier.
    aka nothing from file list is matching filename pattern
    """

    def __init__(self, message="No delivery files found"):
        self.message = message

    def __str__(self):
        return f"{self.message}"


class FtpDownloadException(Exception):
    """
    Exception raised for errors when downloading file from FTP
    """

    def __init__(self, filepath, host, message="Error when downloading file"):
        self.message = message
        self.filepath = filepath
        self.host = host

    def __str__(self):
        return f"{self.message} -> " + f"ftp://{self.host}/{self.filepath}"