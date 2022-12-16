from filters_list import filters
from collections import deque
from time import time
import sys
import traceback


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
    """
    Run pipeline for given file
    """
    try:
        ts = time()
        print(f"Started queue task for {file}")
        result = exhaust(create_pipeline([file], filters))
        print(f"Completed queue task for {file} ---- {time() - ts} s")
        return result
    except Exception as error:
        print("Error in pipeline runner: ", error, traceback.format_exc())
        # send slack message
