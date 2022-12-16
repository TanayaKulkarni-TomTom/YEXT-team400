import csv
from definitions import PRE_PROCESSED_DIR
from helper_data.fieldnames import fieldnames
from pathlib import Path
#from helper_data.countries_lookup import countries


def record_to_file_filter(records):
    """
    Get generator with records and save them to a file.
    Name of the file will contain country code.
    """
    for record in records:
        country_code = record["country"]
        filename = f"yext_{country_code}.csv"
        output_path = Path(PRE_PROCESSED_DIR).joinpath(filename)
        file_exists = output_path.exists()
        with open(output_path, "a", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, delimiter=",", fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(record)
        yield output_path
