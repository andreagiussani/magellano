import logging
import sys


def setup_logger():
    """
    Setup logger to output logs to the notebook's output cell.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)  # outputs logs to the notebook's output cell
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
