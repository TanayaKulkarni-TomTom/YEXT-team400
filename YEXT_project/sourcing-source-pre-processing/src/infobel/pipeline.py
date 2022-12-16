from filters_list import filters
from collections import deque
from time import time
import helpers.app_logger as app_logger


logger = app_logger.get_logger(__name__)


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
        logger.info(f"Started queue task for {file}")
        result = exhaust(create_pipeline([file], filters))
        logger.info(f"Completed queue task for {file} ---- {time() - ts} s")
        return result
    except Exception as error:
        logger.exception(f"Error: {error}")
        raise error
