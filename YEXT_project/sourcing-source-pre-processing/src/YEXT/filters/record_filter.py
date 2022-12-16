import csv
import io 


def record_filter(open_files):
    """
    Get generator of file objects and yeild records from files.
    """
    for open_file in open_files:
        dict_reader = csv.DictReader(io.TextIOWrapper(open_file, encoding='utf-8'), delimiter=",", quoting=csv.QUOTE_MINIMAL)
        for row in dict_reader:
            yield row
