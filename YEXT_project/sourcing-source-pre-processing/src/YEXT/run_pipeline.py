import logging

logging.basicConfig(level=logging.DEBUG)

import sys
from time import time
from definitions import TRIPADVISOR_PROPERTIES_URL
from filters_list import filters
from concurrent.futures import ProcessPoolExecutor
from collections import deque
import traceback
from slack.send_slack import send_slack_message


def exhaust(generator):
    deq = deque(generator, maxlen=1)
    return deq.pop()


def create_pipeline(start_generator, filters):
    """
    Create pipeline of generators
    :param start_generator: list of file paths
    :param filters: list of filter functions
    """
    generator = start_generator
    for filter in filters:
        generator = filter(generator)
    return generator


def pipeline_runner(file, filters=filters):
    try:
        ts = time()
        print(f"Started queue task for {file}")
        output = exhaust(create_pipeline([file], filters))
        print(f"Completed queue task for {file} ---- {time() - ts} s")
        return output
    except Exception as error:
        traceback.print_exc()
        print("Error: ", error, traceback.format_exc())
        message = "Error" + str(error) + traceback.format_exc()
        send_slack_message(message)


if __name__ == "__main__":
    ts = time()

    send_slack_message("YEXT pipeline is started")

    urls = [
        r"C:\ubuntu\shared\poi_loader\tripadvisor\12-sept-2022\PropertyList_TomTomDemo.json.gz"
    ]

    results = []
    with ProcessPoolExecutor(max_workers=1) as executor:
        for file, res in zip(urls, executor.map(pipeline_runner, urls)):
            print(f"file: {file}, res: {res}")

    te = time()
    print("Total time {:.4f} s".format(te - ts))
