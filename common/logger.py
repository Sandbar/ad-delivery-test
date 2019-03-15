import logging
import os
import common.utils as tool
import sys
import traceback


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler(os.environ["log_file"], encoding="UTF-8")
handler.setLevel(logging.INFO)
logging.Formatter.converter = tool.custom_time
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def log_exp():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    exception_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
    for line in exception_lines:
        logger.info(line.replace('\n', ''))
