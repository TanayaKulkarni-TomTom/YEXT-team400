import subprocess
import helpers.app_logger as app_logger

logger = app_logger.get_logger(__name__)


def remove_nul_char_from_file(file_paths):
    for file in file_paths:
        cmd = [r'sed -i "s/\x00//g" ' + f"{file}"]
        logger.info(f"Running subprocess with command for delivery: {cmd}")
        process = subprocess.run(cmd, shell=True, capture_output=True)
        if process.returncode == 0:
            yield file
        else:
            raise ValueError(
                f"Cannot remove null byte char from file {file} ------{process.stderr}"
            )
