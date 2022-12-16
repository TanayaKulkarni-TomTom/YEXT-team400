import pysftp		 			# To connect to server to download delivery


def sftp_download(filepath, dest_dir, host, username, password, cnopts_known_hosts):
    print(f"Downloading {filepath} from: {host}")

    # This logs into the ftp site
    cnopts = pysftp.CnOpts(knownhosts=cnopts_known_hosts)
    sftp = pysftp.Connection(host, username=username, password=password, cnopts=cnopts)

    # Get file
    sftp.get(str(filepath), dest_dir)

    # destroys the ftp connection
    sftp.close()

    return dest_dir


def list_of_files(folderpath, host, username, password, cnopts_known_hosts):
    print(f"Gets file list from direcotry {folderpath}")

    # This logs into the ftp site
    cnopts = pysftp.CnOpts(knownhosts=cnopts_known_hosts)
    sftp = pysftp.Connection(host, username=username, password=password, cnopts=cnopts)

    # Get the contents of directory
    files = sftp.listdir(folderpath)

    # destroys the ftp connection
    sftp.close()

    return files


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
