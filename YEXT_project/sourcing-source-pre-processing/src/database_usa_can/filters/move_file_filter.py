import os
from pathlib import Path
from definitions import PRE_PROCESSED_DIR


def move_file(files):
    for file in files:
        filepath = Path(file)
        if os.path.isfile(filepath):
            new_file_path = Path(PRE_PROCESSED_DIR).joinpath(os.path.basename(filepath))
            new_file_path = filepath.replace(new_file_path)
            yield new_file_path
        else:
            # This is if unzipping delivery lead to folder with actual unzipped delivery inside that folder.
            files_in_unzipped_folder = os.listdir(filepath)
            file = files_in_unzipped_folder[0]
            new_file_path = Path(PRE_PROCESSED_DIR).joinpath(file)
            nested_file_path = Path(filepath).joinpath(file)
            new_file_path = nested_file_path.replace(new_file_path)
            yield new_file_path
