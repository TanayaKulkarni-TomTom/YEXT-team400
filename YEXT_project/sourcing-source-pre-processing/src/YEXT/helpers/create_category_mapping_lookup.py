import csv
from pathlib import Path
from definitions import ROOT_DIR
import json


def get_recoding_schema():
    """
    Convert csv category mapping file to lookup dict
    """
    recoding_schema_path = Path(ROOT_DIR).joinpath(
        "helper_data", "sic_code_recoding.csv"
    )
    recoding_schema = []
    with open(recoding_schema_path) as file:
        reader = csv.DictReader(file, delimiter=",", quotechar='"')
        for row in reader:
            recoding_schema.append(row)

    create_lookup_category_mapping(recoding_schema)


def create_lookup_category_mapping(recoding):
    """
    re-compose list of dicts to a dict with source_cat_code as a key for faster lookup
    """
    lookup = {}
    for row in recoding:
        changed_row = row.copy()
        del changed_row["Source_Cat_Code"]
        lookup[row["Source_Cat_Code"]] = changed_row

    with open(Path(ROOT_DIR).joinpath("sic_code_mapping_lookup.py"), "w") as f:
        f.write(json.dumps(lookup))


if __name__ == "__main__":
    get_recoding_schema()
