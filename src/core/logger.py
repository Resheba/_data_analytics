import logging
from logging import Logger

from config import Settings


logger: Logger = logging.getLogger()
logger.setLevel(logging.INFO)

_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.INFO)

_file_handler = logging.FileHandler(Settings.LOG_PATH)
_file_handler.setLevel(logging.INFO)

logger.addHandler(_console_handler)
logger.addHandler(_file_handler)
