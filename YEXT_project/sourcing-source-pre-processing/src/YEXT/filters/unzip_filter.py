from importlib.resources import path
from zipfile import ZipFile
import zipfile
from pathlib import Path


def unzip(files):
    """
    Unzip all files to current directory and return paths of unzipped files
    :param files: iterable of zipped file paths
    """
    for file in files:
        filepath = Path(file)
        if not zipfile.is_zipfile(filepath):
            yield filepath
        with zipfile.ZipFile(filepath) as file:
            zippedfile= filepath.stem
            with file.open(zippedfile) as opened_file:
                yield opened_file
